package io.delta.thin.util

import java.io.FileNotFoundException

import io.delta.thin.{DeltaLog, Snapshot}
import io.delta.thin.storage.LogStore
import org.apache.hadoop.fs.Path

import scala.util.control.NonFatal


/**
  * Verify the state of the table using the checksum files.
  */
trait VerifyChecksum {
  self: DeltaLog =>

  val logPath: Path
  private[delta] def store: LogStore

  protected def validateChecksum(snapshot: Snapshot): Unit = {
    // TODO:
  }
}