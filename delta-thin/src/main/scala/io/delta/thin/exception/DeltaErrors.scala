package io.delta.thin.exception

import java.io.FileNotFoundException

import io.delta.thin.actions.Metadata
import org.apache.hadoop.fs.Path


trait DocsPath {
  /**
   * The URL for the base path of Delta's docs.
   */
  def baseDocsPath: String = "https://docs.delta.io"
}

object DeltaErrors extends DocsPath {

  def analysisException(
      msg: String,
      line: Option[Int] = None,
      startPosition: Option[Int] = None,
      cause: Option[Throwable] = None): AnalysisException = {
    new AnalysisException(msg, line, startPosition, cause)
  }

  def logFileNotFoundException(
      path: Path,
      version: Long,
      metadata: Metadata): Throwable = {
    //TODO: config
    val logRetention = "interval 30 days"
    val checkpointRetention = "interval 2 days"
    new FileNotFoundException(s"$path: Unable to reconstruct state at version $version as the " +
      s"transaction log has been truncated due to manual deletion or the log retention policy ")
    //TODO:
//      s"(${DeltaConfigs.LOG_RETENTION.key}=$logRetention) and checkpoint retention policy " +
//      s"(${DeltaConfigs.CHECKPOINT_RETENTION_DURATION.key}=$checkpointRetention)")
  }
}
