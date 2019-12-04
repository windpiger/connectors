package io.delta.thin

import java.net.URI

import com.github.mjakubowski84.parquet4s.ParquetReader
import io.delta.thin.Snapshot.State
import io.delta.thin.actions._
import io.delta.thin.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class Snapshot(
    val path: Path,
    val version: Long,
    previousSnapshot: Option[State],
    files: Seq[Path],
    val deltaLog: DeltaLog,
    val timestamp: Long) {

  import Snapshot._

  private val hadoopConf = new Configuration()

  /**
   * Load the transaction logs from paths. The files here may have different file formats and the
   * file format can be extracted from the file extensions.
   *
   * Here we are reading the transaction log, and we need to bypass the ACL checks
   * for SELECT any file permissions.
   */
  private def load(paths: Seq[Path]): Seq[SingleAction] = {
    paths.map(_.toString).sortWith(_ < _).par.flatMap { path =>
      val format = path.split("\\.").last
      if (format == "json") {
        deltaLog.store.read(path).map { line =>
          JsonUtils.mapper.readValue[SingleAction](line)
        }
      } else if (format == "parquet") {
        ParquetReader.read[SingleAction](path).toSeq
      } else Seq.empty[SingleAction]
    }.toList
  }

  // Reconstruct the state by applying deltas in order to the checkpoint.
  // We partition by path as it is likely the bulk of the data is add/remove.
  // Non-path based actions will be collocated to a single partition.
  val state: State = {
    val logPath = path.toUri // for serializability
    val replay = new InMemoryLogReplay(hadoopConf, previousSnapshot)

    // assertLogBelongsToTable
    files.foreach { f =>
      if (f.getParent != new Path(logPath)) {
        throw new AssertionError(s"File ($f) doesn't belong in the " +
          s"transaction log at $logPath. Please contact check it.")
      }
    }

    val deltaData = load(files)

    //TODO: perfermance improvement
    replay.append(0, deltaData.toIterator)
    State(replay.currentProtocolVersion,
      replay.currentMetaData,
      replay.activeFiles.toMap,
      replay.sizeInBytes,
      replay.activeFiles.size,
      replay.numOfMetadata,
      replay.numOfProtocol)
  }

  deltaLog.protocolRead(state.protocol)

  /** Returns the schema of the table. */
  lazy val schemaString: String = state.metadata.schemaString

  /** Returns the data schema of the table, the schema of the columns written out to file. */
  lazy val partitionColumns: Seq[String] = state.metadata.partitionColumns

  lazy val metadata: Metadata = state.metadata

  lazy val protocol: Protocol = state.protocol

  lazy val allFiles: Set[AddFile] = state.activeFiles.values.toSet

  lazy val numOfFiles: Long = state.numOfFiles
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

//    val x = snapShot.stateReconstruction
//    val z = x.map(_.unwrap).filter(_.isInstanceOf[AddFile]).foreach(a => println(s"${(a.asInstanceOf[AddFile].path)}"))
    println("0000")
  }

  case class State(
      protocol: Protocol,
      metadata: Metadata,
      activeFiles: scala.collection.immutable.Map[URI, AddFile],
      sizeInBytes: Long,
      numOfFiles: Long,
      numOfMetadata: Long,
      numOfProtocol: Long)
}