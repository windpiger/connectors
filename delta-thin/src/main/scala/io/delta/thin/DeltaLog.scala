package io.delta.thin

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}
import io.delta.thin.storage.LogStoreProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import scala.util.Try

class DeltaLog private(
    val logPath: Path,
    val dataPath: Path) extends Checkpoints with LogStoreProvider {

  import io.delta.thin.util.FileNames._

  /** Used to read and write physical log files and checkpoints. */
  val store = createLogStore(new Configuration())

  /** Use ReentrantLock to allow us to call `lockInterruptibly` */
  private val deltaLogLock = new ReentrantLock()

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
//    try {
      val snapshot = new Snapshot(
        logPath,
        newVersion,
        checkpointFiles ++ deltaFiles,
        this,
        // we don't want to make an additional RPC here to get commit timestamps when "deltas" is
        // empty. The next "update" call will take care of that if there are delta files.
        deltas.lastOption.map(_.getModificationTime).getOrElse(-1L))

//      validateChecksum(snapshot)
//      lastUpdateTimestamp = clock.getTimeMillis()
      snapshot
//    } catch {
//      case e: AnalysisException if Option(e.getMessage).exists(_.contains("Path does not exist")) =>
//        recordDeltaEvent(this, "delta.checkpoint.error.partial")
//        throw DeltaErrors.missingPartFilesException(c, e)
//    }
  }.getOrElse {
    new Snapshot(logPath, -1, Nil, this, -1L)
  }

  if (currentSnapshot.version == -1) {
    // No checkpoint exists. Call "update" to load delta files.
    update()
  }

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


  /**
    * Queries the store for new delta files and applies them to the current state.
    * Note: the caller should hold `deltaLogLock` before calling this method.
    */
  private def updateInternal(isAsync: Boolean): Snapshot = {
//        try {
//          val newFiles = store
//            // List from the current version since we want to get the checkpoint file for the current
//            // version
//            .listFrom(checkpointPrefix(logPath, math.max(currentSnapshot.version, 0L)))
//            // Pick up checkpoint files not older than the current version and delta files newer than
//            // the current version
//            .filter { file =>
//            isCheckpointFile(file.getPath) ||
//              (isDeltaFile(file.getPath) && deltaVersion(file.getPath) > currentSnapshot.version)
//          }.toArray
//
//          val (checkpoints, deltas) = newFiles.partition(f => isCheckpointFile(f.getPath))
//          if (deltas.isEmpty) {
//            return currentSnapshot
//          }
//
//          val deltaVersions = deltas.map(f => deltaVersion(f.getPath))
//          verifyDeltaVersions(deltaVersions)
//          val lastChkpoint = lastCheckpoint.map(CheckpointInstance.apply)
//            .getOrElse(CheckpointInstance.MaxValue)
//          val checkpointFiles = checkpoints.map(f => CheckpointInstance(f.getPath))
//          val newCheckpoint = getLatestCompleteCheckpointFromList(checkpointFiles, lastChkpoint)
//          val newSnapshot = if (newCheckpoint.isDefined) {
//            // If there is a new checkpoint, start new lineage there.
//            val newCheckpointVersion = newCheckpoint.get.version
//            assert(
//              newCheckpointVersion >= currentSnapshot.version,
//              s"Attempting to load a checkpoint($newCheckpointVersion) " +
//                s"older than current version (${currentSnapshot.version})")
//            val newCheckpointFiles = newCheckpoint.get.getCorrespondingFiles(logPath)
//
//            val newVersion = deltaVersions.last
//            val deltaFiles =
//              ((newCheckpointVersion + 1) to newVersion).map(deltaFile(logPath, _))
//
//            println(s"Loading version $newVersion starting from checkpoint $newCheckpointVersion")
//
//            new Snapshot(
//              logPath,
//              newVersion,
//              newCheckpointFiles ++ deltaFiles,
//              this,
//              deltas.last.getModificationTime)
//          } else {
//            // If there is no new checkpoint, just apply the deltas to the existing state.
//            assert(currentSnapshot.version + 1 == deltaVersions.head,
//              s"versions in [${currentSnapshot.version + 1}, ${deltaVersions.head}) are missing")
//            if (currentSnapshot.lineageLength >= maxSnapshotLineageLength) {
//              // Load Snapshot from scratch to avoid StackOverflowError
//              getSnapshotAt(deltaVersions.last, Some(deltas.last.getModificationTime))
//            } else {
//              new Snapshot(
//                logPath,
//                deltaVersions.last,
//                Some(currentSnapshot.state),
//                deltas.map(_.getPath),
//                minFileRetentionTimestamp,
//                this,
//                deltas.last.getModificationTime,
//                lineageLength = currentSnapshot.lineageLength + 1)
//            }
//          }
//          validateChecksum(newSnapshot)
//          currentSnapshot.uncache()
//          currentSnapshot = newSnapshot
//        } catch {
//          case f: FileNotFoundException =>
//            val message = s"No delta log found for the Delta table at $logPath"
//            logInfo(message)
//            // When the state is empty, this is expected. The log will be lazily created when needed.
//            // When the state is not empty, it's a real issue and we can't continue to execution.
//            if (currentSnapshot.version != -1) {
//              val e = new FileNotFoundException(message)
//              e.setStackTrace(f.getStackTrace())
//              throw e
//            }
//        }
//        lastUpdateTimestamp = clock.getTimeMillis()
//        currentSnapshot
      null
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
    lockInterruptibly {
      updateInternal(isAsync = false)
    }
  }
  /** Returns the current snapshot. Note this does not automatically `update()`. */
  def snapshot: Snapshot = currentSnapshot

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
}

object DeltaLog {
//
//  /**
//   * We create only a single [[DeltaLog]] for any given path to avoid wasted work
//   * in reconstructing the log.
//   */
//  private val deltaLogCache = {
//    val builder = CacheBuilder.newBuilder()
//      .expireAfterAccess(60, TimeUnit.MINUTES)
//      .removalListener(new RemovalListener[Path, DeltaLog] {
//        override def onRemoval(removalNotification: RemovalNotification[Path, DeltaLog]) = {
//          val log = removalNotification.getValue
//          try log.snapshot.uncache() catch {
//            case _: java.lang.NullPointerException =>
//            // Various layers will throw null pointer if the RDD is already gone.
//          }
//        }
//      })
//    sys.props.get("delta.log.cacheSize")
//      .flatMap(v => Try(v.toLong).toOption)
//      .foreach(builder.maximumSize)
//    builder.build[Path, DeltaLog]()
//  }
//
//  def apply(rawPath: Path): DeltaLog = {
//    val hadoopConf = new Configuration()
//    val fs = rawPath.getFileSystem(hadoopConf)
//    val path = fs.makeQualified(rawPath)
//    // The following cases will still create a new ActionLog even if there is a cached
//    // ActionLog using a different format path:
//    // - Different `scheme`
//    // - Different `authority` (e.g., different user tokens in the path)
//    // - Different mount point.
//    val cached = try {
//      deltaLogCache.get(path, new Callable[DeltaLog] {
//        override def call(): DeltaLog = new DeltaLog(path, path.getParent)
//      })
//    } catch {
//      case e: com.google.common.util.concurrent.UncheckedExecutionException =>
//        throw e.getCause
//    }
//
//    // Invalidate the cache if the reference is no longer valid as a result of the
//    // log being deleted.
//    if (cached.snapshot.version == -1 || cached.isValid()) {
//      cached
//    } else {
//      deltaLogCache.invalidate(path)
//      apply(spark, path)
//    }
//  }
}
