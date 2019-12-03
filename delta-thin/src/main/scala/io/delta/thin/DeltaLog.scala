package io.delta.thin

import java.io.FileNotFoundException
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.CacheBuilder
import io.delta.thin.actions._
import io.delta.thin.exception.DeltaErrors
import io.delta.thin.storage.LogStoreProvider
import io.delta.thin.util.{Clock, SystemClock, ThreadUtils, VerifyChecksum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

class DeltaLog private(
    val logPath: Path,
    val dataPath: Path,
    val clock: Clock)
  extends Checkpoints
  with LogStoreProvider
  with VerifyChecksum {

  import io.delta.thin.util.FileNames._

  private lazy implicit val _clock = clock

  @volatile private[delta] var asyncUpdateTask: Future[Unit] = _
  /** The timestamp when the last successful update action is finished. */
  @volatile private var lastUpdateTimestamp = -1L

  private[delta] val hadoopConf = new Configuration()
  /** Used to read and write physical log files and checkpoints. */
  val store = createLogStore(hadoopConf)
  /** Direct access to the underlying storage system. */
  private[delta] val fs = logPath.getFileSystem(hadoopConf)

  /** Use ReentrantLock to allow us to call `lockInterruptibly` */
  private val deltaLogLock = new ReentrantLock()


  // TODO: There is a race here where files could get dropped when increasing the
  // retention interval...
  protected def metadata = if (snapshot == null) Metadata() else snapshot.metadata

  /** The unique identifier for this table. */
  def tableId: String = metadata.id


  /* ------------------ *
   |  State Management  |
   * ------------------ */

  @volatile private var currentSnapshot: Snapshot = lastCheckpoint.map { c =>
    val checkpointFiles = c.parts
      .map(p => checkpointFileWithParts(logPath, c.version, p))
      .getOrElse(Seq(checkpointFileSingular(logPath, c.version)))
    val deltas = store.listFrom(deltaFile(logPath, c.version + 1))
      .filter(f => isDeltaFile(f.getPath))
      .toArray
    val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
    verifyDeltaVersions(deltaVersions)
    val newVersion = deltaVersions.lastOption.getOrElse(c.version)
    val deltaFiles = ((c.version + 1) to newVersion).map(deltaFile(logPath, _))
    println(s"Loading version $newVersion starting from checkpoint ${c.version}")
    val snapshot = new Snapshot(
      logPath,
      newVersion,
      None,
      checkpointFiles ++ deltaFiles,
      this,
      // we don't want to make an additional RPC here to get commit timestamps when "deltas" is
      // empty. The next "update" call will take care of that if there are delta files.
      deltas.lastOption.map(_.getModificationTime).getOrElse(-1L))

    validateChecksum(snapshot)
    lastUpdateTimestamp = clock.getTimeMillis()
    snapshot
  }.getOrElse {
    new Snapshot(logPath, -1, None, Nil, this, -1L)
  }

  if (currentSnapshot.version == -1) {
    // No checkpoint exists. Call "update" to load delta files.
    update()
  }

  /**
   * Verify the versions are contiguous.
   */
  private def verifyDeltaVersions(versions: Array[Long]): Unit = {
    // Turn this to a vector so that we can compare it with a range.
    val deltaVersions = versions.toVector
    if (deltaVersions.nonEmpty &&
      (deltaVersions.head to deltaVersions.last) != deltaVersions) {
      throw new IllegalStateException(s"versions ($deltaVersions) are not contiguous")
    }
  }

  /** Returns the current snapshot. Note this does not automatically `update()`. */
  def snapshot: Snapshot = currentSnapshot

  /**
   * Run `body` inside `deltaLogLock` lock using `lockInterruptibly` so that the thread can be
   * interrupted when waiting for the lock.
   */
  def lockInterruptibly[T](body: => T): T = {
    deltaLogLock.lockInterruptibly()
    try {
      body
    } finally {
      deltaLogLock.unlock()
    }
  }

  /** Checks if the snapshot of the table has surpassed our allowed staleness. */
  private def isSnapshotStale: Boolean = {
    //TODO: add config
    val stalenessLimit = 0
    stalenessLimit == 0L || lastUpdateTimestamp < 0 ||
      clock.getTimeMillis() - lastUpdateTimestamp >= stalenessLimit
  }

  /**
   * Update ActionLog by applying the new delta files if any.
   *
   * @param stalenessAcceptable Whether we can accept working with a stale version of the table. If
   *                            the table has surpassed our staleness tolerance, we will update to
   *                            the latest state of the table synchronously. If staleness is
   *                            acceptable, and the table hasn't passed the staleness tolerance, we
   *                            will kick off a job in the background to update the table state,
   *                            and can return a stale snapshot in the meantime.
   */
  def update(stalenessAcceptable: Boolean = false): Snapshot = {
    val doAsync = stalenessAcceptable && !isSnapshotStale
    if (!doAsync) {
      lockInterruptibly {
        updateInternal(isAsync = false)
      }
    } else {
      if (asyncUpdateTask == null || asyncUpdateTask.isCompleted) {
        asyncUpdateTask = Future[Unit] {
          tryUpdate(isAsync = true)
        }(DeltaLog.deltaLogAsyncUpdateThreadPool)
      }
      currentSnapshot
    }
  }

  /**
   * Try to update ActionLog. If another thread is updating ActionLog, then this method returns
   * at once and return the current snapshot. The return snapshot may be stale.
   */
  def tryUpdate(isAsync: Boolean = false): Snapshot = {
    if (deltaLogLock.tryLock()) {
      try {
        updateInternal(isAsync)
      } finally {
        deltaLogLock.unlock()
      }
    } else {
      currentSnapshot
    }
  }

  /**
   * Queries the store for new delta files and applies them to the current state.
   * Note: the caller should hold `deltaLogLock` before calling this method.
   */
  private def updateInternal(isAsync: Boolean): Snapshot = {
    try {
      val newFiles = store
        // List from the current version since we want to get the checkpoint file for the current
        // version
        .listFrom(checkpointPrefix(logPath, math.max(currentSnapshot.version, 0L)))
        // Pick up checkpoint files not older than the current version and delta files newer than
        // the current version
        .filter { file =>
        isCheckpointFile(file.getPath) ||
          (isDeltaFile(file.getPath) && deltaVersion(file.getPath) > currentSnapshot.version)
      }.toArray

      val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))
      if (deltas.isEmpty) {
        lastUpdateTimestamp = clock.getTimeMillis()
        return currentSnapshot
      }

      val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
      verifyDeltaVersions(deltaVersions)
      val lastChkpoint = lastCheckpoint.map(CheckpointInstance.apply)
        .getOrElse(CheckpointInstance.MaxValue)
      val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
      val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
      val newSnapshot = if (newCheckpoint.isDefined) {
        // If there is a new checkpoint, start new lineage there.
        val newCheckpointVersion = newCheckpoint.get.version
        assert(
          newCheckpointVersion >= currentSnapshot.version,
          s"Attempting to load a checkpoint($newCheckpointVersion) " +
            s"older than current version (${currentSnapshot.version})")
        val newCheckpointFiles = newCheckpoint.get.getCorrespondingFiles(logPath)

        val newVersion = deltaVersions.last
        val deltaFiles =
          ((newCheckpointVersion + 1) to newVersion).map(deltaFile(logPath, _))

        println(s"Loading version $newVersion starting from checkpoint $newCheckpointVersion")

        new Snapshot(
          logPath,
          newVersion,
          None,
          newCheckpointFiles ++ deltaFiles,
          this,
          deltas.last.getModificationTime)
      } else {
        // If there is no new checkpoint, just apply the deltas to the existing state.
        assert(currentSnapshot.version + 1 == deltaVersions.head,
          s"versions in [${currentSnapshot.version + 1}, ${deltaVersions.head}) are missing")
        new Snapshot(
          logPath,
          deltaVersions.last,
          Some(currentSnapshot.state),
          deltas.map(_.getPath),
          this,
          deltas.last.getModificationTime)
      }
      //TODO:
      validateChecksum(newSnapshot)
      currentSnapshot = newSnapshot
    } catch {
      case f: FileNotFoundException =>
        val message = s"No delta log found for the Delta table at $logPath"
        println(message)
        // When the state is empty, this is expected. The log will be lazily created when needed.
        // When the state is not empty, it's a real issue and we can't continue to execution.
        if (currentSnapshot.version != -1) {
          val e = new FileNotFoundException(message)
          e.setStackTrace(f.getStackTrace())
          throw e
        }
    }
    lastUpdateTimestamp = clock.getTimeMillis()
    currentSnapshot
    }

  /* --------------------- *
   |  Protocol validation  |
   * --------------------- */

  private def oldProtocolMessage(protocol: Protocol): String =
    s"WARNING: The Delta Lake table at $dataPath has version " +
      s"${protocol.simpleString}, but the latest version is " +
      s"${Protocol().simpleString}. To take advantage of the latest features and bug fixes, " +
      "we recommend that you upgrade the table.\n" +
      "First update all clusters that use this table to the latest version of Databricks " +
      "Runtime, and then run the following command in a notebook:\n" +
      "'%scala com.databricks.delta.Delta.upgradeTable(\"" + s"$dataPath" + "\")'\n\n" +
      "For more information about Delta Lake table versions, see " +
      s"${DeltaErrors.baseDocsPath}/delta/versioning.html"

  /**
   * If the given `protocol` is older than that of the client.
   */
  private def isProtocolOld(protocol: Protocol): Boolean = protocol != null &&
    (Action.readerVersion > protocol.minReaderVersion ||
      Action.writerVersion > protocol.minWriterVersion)

  /**
   * Asserts that the client is up to date with the protocol and
   * allowed to read the table that is using the given `protocol`.
   */
  def protocolRead(protocol: Protocol): Unit = {
    if (protocol != null &&
      Action.readerVersion < protocol.minReaderVersion) {
      //TODO: println
      throw new InvalidProtocolVersionException
    }

    if (isProtocolOld(protocol)) {
      //TODO: println
      println(oldProtocolMessage(protocol))
    }
  }

  /* ------------------- *
   |  History Management |
   * ------------------- */

  /** Get the snapshot at `version`. */
  def getSnapshotAt(
      version: Long,
      commitTimestamp: Option[Long] = None,
      lastCheckpointHint: Option[CheckpointInstance] = None): Snapshot = {
    val current = snapshot
    if (current.version == version) {
      return current
    }

    // Do not use the hint if the version we're asking for is smaller than the last checkpoint hint
    val lastCheckpoint = lastCheckpointHint.collect { case ci if ci.version <= version => ci }
      .orElse(findLastCompleteCheckpoint(CheckpointInstance(version, None)))
    val lastCheckpointFiles = lastCheckpoint.map { c =>
      c.getCorrespondingFiles(logPath)
    }.toSeq.flatten
    val checkpointVersion = lastCheckpoint.map(_.version)
    if (checkpointVersion.isEmpty) {
      val versionZeroFile = deltaFile(logPath, 0L)
      val versionZeroFileExists = store.listFrom(versionZeroFile)
        .take(1)
        .exists(_.getPath.getName == versionZeroFile.getName)
      if (!versionZeroFileExists) {
        throw DeltaErrors.logFileNotFoundException(versionZeroFile, 0L, metadata)
      }
    }
    val deltaData =
      ((checkpointVersion.getOrElse(-1L) + 1) to version).map(deltaFile(logPath, _))
    new Snapshot(
      logPath,
      version,
      None,
      lastCheckpointFiles ++ deltaData,
      this,
      commitTimestamp.getOrElse(-1L))
  }

  /* ---------------------------------------- *
   |  Log Directory Management and Retention  |
   * ---------------------------------------- */

  def isValid(): Boolean = {
    val expectedExistingFile = deltaFile(logPath, currentSnapshot.version)
    try {
      store.listFrom(expectedExistingFile)
        .take(1)
        .exists(_.getPath.getName == expectedExistingFile.getName)
    } catch {
      case _: FileNotFoundException =>
        // Parent of expectedExistingFile doesn't exist
        false
    }
  }
}

object DeltaLog {

  protected lazy val deltaLogAsyncUpdateThreadPool = {
    val tpe = ThreadUtils.newDaemonCachedThreadPool("delta-thin-state-update", 8)
    ExecutionContext.fromExecutorService(tpe)
  }
  /**
   * We create only a single [[DeltaLog]] for any given path to avoid wasted work
   * in reconstructing the log.
   */
  private val deltaLogCache = {
    val builder = CacheBuilder.newBuilder()
      .expireAfterAccess(60, TimeUnit.MINUTES)
    sys.props.get("delta.log.cacheSize")
      .flatMap(v => Try(v.toLong).toOption)
      .foreach(builder.maximumSize)
    builder.build[Path, DeltaLog]()
  }

  def apply(rawPath: Path): DeltaLog = {
    val hadoopConf = new Configuration()
    val fs = rawPath.getFileSystem(hadoopConf)
    val path = fs.makeQualified(rawPath)
    // The following cases will still create a new ActionLog even if there is a cached
    // ActionLog using a different format path:
    // - Different `scheme`
    // - Different `authority` (e.g., different user tokens in the path)
    // - Different mount point.
    val cached = try {
      deltaLogCache.get(path, new Callable[DeltaLog] {
        override def call(): DeltaLog = new DeltaLog(path, path.getParent, new SystemClock)
      })
    } catch {
      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
        throw e.getCause
    }

    // Invalidate the cache if the reference is no longer valid as a result of the
    // log being deleted.
    if (cached.snapshot.version == -1 || cached.isValid()) {
      cached
    } else {
      deltaLogCache.invalidate(path)
      apply(path)
    }
  }

  /** Invalidate the cached DeltaLog object for the given `dataPath`. */
  def invalidateCache(dataPath: Path): Unit = {
    try {
      val rawPath = new Path(dataPath, "_delta_log")
      val fs = rawPath.getFileSystem(new Configuration())
      val path = fs.makeQualified(rawPath)
      deltaLogCache.invalidate(path)
    } catch {
      case NonFatal(e) => println(e.getMessage, e)
    }
  }

  def clearCache(): Unit = {
    deltaLogCache.invalidateAll()
  }

  /** Helper for creating a log when it stored at the root of the data. */
  def forTable(dataPath: String): DeltaLog = {
    apply(new Path(dataPath, "_delta_log"))
  }

  /** Find the root of a Delta table from the provided path. */
  def findDeltaTableRoot(path: Path): Option[Path] = {
    val fs = path.getFileSystem(new Configuration())
    var currentPath = path
    while (currentPath != null && currentPath.getName() != "_delta_log" &&
      currentPath.getName() != "_samples") {
      val deltaLogPath = new Path(currentPath, "_delta_log")
      if (Try(fs.exists(deltaLogPath)).getOrElse(false)) {
        return Option(currentPath)
      }
      currentPath = currentPath.getParent()
    }
    None
  }


  def main(args: Array[String]): Unit = {
    val x = DeltaLog.forTable("file:///Users/songjun.sj/Desktop/testdelta")

    val y = x.snapshot.allFiles

    x.update(true)
    println("000000")
  }
}
