package io.delta.thin.util

import java.io.FileNotFoundException

import io.delta.thin.{DeltaLog, Snapshot}
import io.delta.thin.storage.LogStore
import org.apache.hadoop.fs.Path

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
 * Stats calculated within a snapshot, which we store along individual transactions for
 * verification.
 *
 * @param tableSizeBytes The size of the table in bytes
 * @param numFiles Number of `AddFile` actions in the snapshot
 * @param numMetadata Number of `Metadata` actions in the snapshot
 * @param numProtocol Number of `Protocol` actions in the snapshot
 * @param numTransactions Number of `SetTransaction` actions in the snapshot
 */
case class VersionChecksum(
    tableSizeBytes: Long,
    numFiles: Long,
    numMetadata: Long,
    numProtocol: Long,
    numTransactions: Long)

/**
 * Verify the state of the table using the checksum files.
 */
trait VerifyChecksum {
  self: DeltaLog =>

  val logPath: Path
  private[delta] def store: LogStore


  protected def validateChecksum(snapshot: Snapshot): Unit = {
    val version = snapshot.version
    val checksumFile = FileNames.checksumFile(logPath, version)

    var exception: Option[String] = None
    val content = try Some(store.read(checksumFile)) catch {
      case NonFatal(e) =>
        // We expect FileNotFoundException; if it's another kind of exception, we still catch them
        // here but we log them in the checksum error event below.
        if (!e.isInstanceOf[FileNotFoundException]) {
          exception = Some(Utils.exceptionString(e))
        }
        None
    }

    if (content.isEmpty) {
      // We may not find the checksum file in two cases:
      //  - We just upgraded our Spark version from an old one
      //  - Race conditions where we commit a transaction, and before we can write the checksum
      //    this reader lists the new version, and uses it to create the snapshot.
//      recordDeltaEvent(
//        this,
//        "delta.checksum.error.missing",
//        data = Map("version" -> version) ++ exception.map(("exception" -> _)))

      return
    }
    val checksumData = content.get
    if (checksumData.isEmpty) {
//      recordDeltaEvent(
//        this,
//        "delta.checksum.error.empty",
//        data = Map("version" -> version))
      return
    }
    var mismatchStringOpt: Option[String] = None
    try {
      val checksum = JsonUtils.mapper.readValue[VersionChecksum](checksumData.head)
//      mismatchStringOpt = checkMismatch(checksum, snapshot)
    } catch {
      case NonFatal(e) =>
//        recordDeltaEvent(
//          this,
//          "delta.checksum.error.parsing",
//          data = Map("exception" -> Utils.exceptionString(e)))
    }
    if (mismatchStringOpt.isDefined) {
      // Report the failure to usage logs.
//      recordDeltaEvent(
//        this,
//        "delta.checksum.invalid",
//        data = Map("error" -> mismatchStringOpt.get))
      // We get the active SparkSession, which may be different than the SparkSession of the
      // Snapshot that was created, since we cache `DeltaLog`s.
//      val spark = SparkSession.getActiveSession.getOrElse {
//        throw new IllegalStateException("Active SparkSession not set.")
//      }
//      val conf = DeltaSQLConf.DELTA_STATE_CORRUPTION_IS_FATAL
//      if (spark.sessionState.conf.getConf(conf)) {
//        throw new IllegalStateException(
//          "The transaction log has failed integrity checks. We recommend you contact " +
//            s"Databricks support for assistance. To disable this check, set ${conf.key} to " +
//            s"false. Failed verification of:\n${mismatchStringOpt.get}"
//        )
//      }
    }
  }
//
//  private def checkMismatch(checksum: VersionChecksum, snapshot: Snapshot): Option[String] = {
//    val result = new ArrayBuffer[String]()
//    def compare(expected: Long, found: Long, title: String): Unit = {
//      if (expected != found) {
//        result += s"$title - Expected: $expected Computed: $found"
//      }
//    }
//    compare(checksum.tableSizeBytes, snapshot.sizeInBytes, "Table size (bytes)")
//    compare(checksum.numFiles, snapshot.numOfFiles, "Number of files")
//    compare(checksum.numMetadata, snapshot.numOfMetadata, "Metadata updates")
//    compare(checksum.numProtocol, snapshot.numOfProtocol, "Protocol updates")
//    compare(checksum.numTransactions, snapshot.numOfSetTransactions, "Transactions")
//    if (result.isEmpty) None else Some(result.mkString("\n"))
//  }
}