package io.delta.thin

import java.net.URI

import com.github.mjakubowski84.parquet4s.ParquetReader
import io.delta.thin.actions._
import io.delta.thin.storage.HDFSLogStore
import io.delta.thin.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class Snapshot(
    val path: Path,
    val version: Long,
    previousSnapshot: Option[Set[SingleAction]],
    files: Seq[Path],
    val deltaLog: DeltaLog,
    val timestamp: Long,
    val lineageLength: Int = 1) {

  import Snapshot._

  private val hdpConf = new Configuration()


  /** The current set of actions in this [[Snapshot]]. */
  def state: Set[SingleAction] = stateReconstruction.toSet
  /**
    * Load the transaction logs from paths. The files here may have different file formats and the
    * file format can be extracted from the file extensions.
    *
    * Here we are reading the transaction log, and we need to bypass the ACL checks
    * for SELECT any file permissions.
    */
  private def load(paths: Seq[Path]): Set[SingleAction] = {
    val store = new HDFSLogStore(hdpConf)

    paths.map(_.toString).sortWith(_ < _).flatMap { path =>
      val format = path.split("\\.").last
      if (format == "json") {
        store.read(path).map { line =>
          JsonUtils.mapper.readValue[SingleAction](line)
        }
      } else if (format == "parquet") {
        ParquetReader.read[SingleAction](path).toSeq
      } else Seq.empty[SingleAction]
    }.toSet
  }

  // Reconstruct the state by applying deltas in order to the checkpoint.
  // We partition by path as it is likely the bulk of the data is add/remove.
  // Non-path based actions will be collocated to a single partition.
  val stateReconstruction = {
    val logPath = path.toUri // for serializability
    val state = new InMemoryLogReplay

    // assertLogBelongsToTable
    files.foreach { f =>
      if (f.getParent != new Path(logPath)) {
        throw new AssertionError(s"File ($f) doesn't belong in the " +
          s"transaction log at $logPath. Please contact check it.")
      }
    }

    val allActions = load(files).map(_.unwrap)
      .filterNot(action => action == null || action.isInstanceOf[SetTransaction])
      .map {
        case add: AddFile => add.copy(path = canonicalizePath(add.path, hdpConf)).wrap
        case rm: RemoveFile => rm.copy(path = canonicalizePath(rm.path, hdpConf)).wrap
        case other => other.wrap
    }

    allActions.map(_.unwrap).foreach (action => state.append(0, action))
    println(s"---XXXXXXXXXX-${allActions.size}")
    val x = state.checkpoint
    println(s"----YYYYYY-${x.size}")
    val z = x.map(_.wrap)
    println(s"----ZZZZZZZ-${z.size}")
    z
  }

  // Here we need to bypass the ACL checks for SELECT anonymous function permissions.
  /** All of the files present in this [[Snapshot]]. */
  def allFiles: Set[AddFile] = {
    state.map(_.unwrap).filter(_.isInstanceOf[AddFile]).map(_.asInstanceOf[AddFile])
  }

  lazy val metadata = state.filter(_.unwrap.isInstanceOf[Metadata]).head.asInstanceOf[Metadata]

}

object Snapshot {

  /** Canonicalize the paths for Actions */
  def canonicalizePath(path: String, hadoopConf: Configuration): String = {
    val hadoopPath = new Path(new URI(path))
    if (hadoopPath.isAbsoluteAndSchemeAuthorityNull) {
      val fs = FileSystem.get(hadoopConf)
      fs.makeQualified(hadoopPath).toUri.toString
    } else {
      // return untouched if it is a relative path or is already fully qualified
      hadoopPath.toUri.toString
    }
  }

  def main(args: Array[String]): Unit = {
    val snapShot = new Snapshot(
      new Path("file:///Users/songjun.sj/Desktop/testdelta/_delta_log"),
      0,
      None,
      Seq(new Path("file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000010.json"),
      new Path("file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000010.checkpoint.parquet")),
      null,
      System.currentTimeMillis()
    )

    val x = snapShot.stateReconstruction
    val z = x.map(_.unwrap).filter(_.isInstanceOf[AddFile]).foreach(a => println(s"${(a.asInstanceOf[AddFile].path)}"))
    println("0000")
  }

  private case class State(
      protocol: Protocol,
      metadata: Metadata,
      setTransactions: Seq[SetTransaction],
      sizeInBytes: Long,
      numOfFiles: Long,
      numOfMetadata: Long,
      numOfProtocol: Long,
      numOfRemoves: Long,
      numOfSetTransactions: Long)
}