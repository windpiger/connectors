package org.apache.spark.sql.delta.thin

import io.delta.thin.{CheckpointMetaData => CheckpointMetaDataThin}
import io.delta.thin.actions.{AddFile => AddFileThin, Metadata => MetadataThin, Protocol => ProtocolThin}
import org.apache.spark.sql.delta.CheckpointMetaData
import org.apache.spark.sql.delta.actions.{AddFile, Format, Metadata, Protocol}

trait DeltaThinTest {

  def convertAddFile(thin: AddFileThin): AddFile = {
    AddFile(thin.path, thin.partitionValues, thin.size,
      thin.modificationTime, thin.dataChange, thin.stats, thin.tags)
  }

  def convertMetaData(thin: MetadataThin): Metadata = {
    Metadata(thin.id, thin.name, thin.description, Format(thin.format.provider, thin.format.options),
      thin.schemaString, thin.partitionColumns, thin.configuration, thin.createdTime)
  }

  def convertProtocol(thin: ProtocolThin): Protocol = {
    Protocol(thin.minReaderVersion, thin.minWriterVersion)
  }

  def convertCheckpointMetaData(thin: CheckpointMetaDataThin): CheckpointMetaData = {
    CheckpointMetaData(thin.version, thin.size, thin.parts)
  }
}
