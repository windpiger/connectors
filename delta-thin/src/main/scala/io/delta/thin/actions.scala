package io.delta.thin


import java.net.URI
import java.sql.Timestamp

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
import io.delta.thin.util.JsonUtils
import org.codehaus.jackson.annotate.JsonRawValue

/** Thrown when the protocol version of a table is greater than supported by this client. */
class InvalidProtocolVersionException extends RuntimeException(
  "Delta protocol version is too new for this version of the Databricks Runtime. " +
    "Please upgrade to a newer release.")

class ProtocolDowngradeException(oldProtocol: Protocol, newProtocol: Protocol)
  extends RuntimeException(
    s"Protocol version cannot be downgraded from $oldProtocol to $newProtocol")

object Action {
  /** The maximum version of the protocol that this version of Delta understands. */
  val readerVersion = 1
  val writerVersion = 2
  val protocolVersion: Protocol = Protocol(readerVersion, writerVersion)

  def fromJson(json: String): Action = {
    JsonUtils.mapper.readValue[SingleAction](json).unwrap
  }

//  lazy val logSchema = ExpressionEncoder[SingleAction].schema
}

/**
  * Represents a single change to the state of a Delta table. An order sequence
  * of actions can be replayed using [[InMemoryLogReplay]] to derive the state
  * of the table at a given point in time.
  */
sealed trait Action {
  def wrap: SingleAction
  def json: String = JsonUtils.toJson(wrap)
}

/**
  * Used to block older clients from reading or writing the log when backwards
  * incompatible changes are made to the protocol. Readers and writers are
  * responsible for checking that they meet the minimum versions before performing
  * any other operations.
  *
  * Since this action allows us to explicitly block older clients in the case of a
  * breaking change to the protocol, clients should be tolerant of messages and
  * fields that they do not understand.
  */
case class Protocol(
    minReaderVersion: Int = Action.readerVersion,
    minWriterVersion: Int = Action.writerVersion) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)
  @JsonIgnore
  def simpleString: String = s"($minReaderVersion,$minWriterVersion)"
}

/**
  * Sets the committed version for a given application. Used to make operations
  * like streaming append idempotent.
  */
case class SetTransaction(
    appId: String,
    version: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    lastUpdated: Option[Long]) extends Action {
  override def wrap: SingleAction = SingleAction(txn = this)
}

/** Actions pertaining to the addition and removal of files. */
sealed trait FileAction extends Action {
  val path: String
  val dataChange: Boolean
  @JsonIgnore
  lazy val pathAsUri: URI = new URI(path)
}

/**
  * Adds a new file to the table. When multiple [[AddFile]] file actions
  * are seen with the same `path` only the metadata from the last one is
  * kept.
  */
case class AddFile(
    path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    dataChange: Boolean,
    @JsonRawValue
    stats: String = null,
    tags: Map[String, String] = null) extends FileAction {
  require(path.nonEmpty)

  override def wrap: SingleAction = SingleAction(add = this)

  def remove: RemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
                           timestamp: Long = System.currentTimeMillis(),
                           dataChange: Boolean = true): RemoveFile = {
    // scalastyle:off
    RemoveFile(path, Some(timestamp), dataChange)
    // scalastyle:on
  }
}

object AddFile {
  /**
    * Misc file-level metadata.
    *
    * The convention is that clients may safely ignore any/all of these tags and this should never
    * have an impact on correctness.
    *
    * Otherwise, the information should go as a field of the AddFile action itself and the Delta
    * protocol version should be bumped.
    */
  object Tags {
    sealed abstract class KeyType(val name: String)

    /** [[ZCUBE_ID]]: identifier of the OPTIMIZE ZORDER BY job that this file was produced by */
    object ZCUBE_ID extends AddFile.Tags.KeyType("ZCUBE_ID")

    /** [[ZCUBE_ZORDER_BY]]: ZOrdering of the corresponding ZCube */
    object ZCUBE_ZORDER_BY extends AddFile.Tags.KeyType("ZCUBE_ZORDER_BY")
  }

  /** Convert a [[Tags.KeyType]] to a string to be used in the AddMap.tags Map[String, String]. */
  def tag(tagKey: Tags.KeyType): String = tagKey.name
}

/**
  * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
  * deleted permanently.
  */
// scalastyle:off
case class RemoveFile(
    path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    dataChange: Boolean = true) extends FileAction {
  override def wrap: SingleAction = SingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)
}
// scalastyle:on

case class Format(
    provider: String = "parquet",
    options: Map[String, String] = Map.empty)

/**
  * Updates the metadata of the table. Only the last update to the [[Metadata]]
  * of a table is kept. It is the responsibility of the writer to ensure that
  * any data already present in the table is still valid after any change.
  */
case class Metadata(
    id: String = java.util.UUID.randomUUID().toString,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    createdTime: Option[Long] = Some(System.currentTimeMillis())) extends Action {
  override def wrap: SingleAction = SingleAction(metaData = this)
}

/**
  * Interface for objects that represents the information for a commit. Commits can be referred to
  * using a version and timestamp. The timestamp of a commit comes from the remote storage
  * `lastModifiedTime`, and can be adjusted for clock skew. Hence we have the method `withTimestamp`.
  */
trait CommitMarker {
  /** Get the timestamp of the commit as millis after the epoch. */
  def getTimestamp: Long
  /** Return a copy object of this object with the given timestamp. */
  def withTimestamp(timestamp: Long): CommitMarker
  /** Get the version of the commit. */
  def getVersion: Long
}

/**
  * Holds provenance information about changes to the table. This [[Action]]
  * is not stored in the checkpoint and has reduced compatibility guarantees.
  * Information stored in it is best effort (i.e. can be falsified by the writer).
  */
case class CommitInfo(
     // The commit version should be left unfilled during commit(). When reading a delta file, we can
     // infer the commit version from the file name and fill in this field then.
     @JsonDeserialize(contentAs = classOf[java.lang.Long])
     version: Option[Long],
     timestamp: Timestamp,
     userId: Option[String],
     userName: Option[String],
     operation: String,
     @JsonSerialize(using = classOf[JsonMapSerializer])
     operationParameters: Map[String, String],
     job: Option[JobInfo],
     notebook: Option[NotebookInfo],
     clusterId: Option[String],
     @JsonDeserialize(contentAs = classOf[java.lang.Long])
     readVersion: Option[Long],
     isolationLevel: Option[String],
     /** Whether this commit has blindly appended without caring about existing files */
     isBlindAppend: Option[Boolean]) extends Action with CommitMarker {
  override def wrap: SingleAction = SingleAction(commitInfo = this)

  override def withTimestamp(timestamp: Long): CommitInfo = {
    this.copy(timestamp = new Timestamp(timestamp))
  }

  override def getTimestamp: Long = timestamp.getTime
  @JsonIgnore
  override def getVersion: Long = version.get
}

case class JobInfo(
    jobId: String,
    jobName: String,
    runId: String,
    jobOwnerId: String,
    triggerType: String)

object JobInfo {
  def fromContext(context: Map[String, String]): Option[JobInfo] = {
    context.get("jobId").map { jobId =>
      JobInfo(
        jobId,
        context.get("jobName").orNull,
        context.get("runId").orNull,
        context.get("jobOwnerId").orNull,
        context.get("jobTriggerType").orNull)
    }
  }
}

case class NotebookInfo(notebookId: String)

object NotebookInfo {
  def fromContext(context: Map[String, String]): Option[NotebookInfo] = {
    context.get("notebookId").map { nbId => NotebookInfo(nbId) }
  }
}

object CommitInfo {
  def empty(version: Option[Long] = None): CommitInfo = {
    CommitInfo(version, null, None, None, null, null, None, None, None, None, None, None)
  }

  def apply(
      time: Long,
      operation: String,
      operationParameters: Map[String, String],
      commandContext: Map[String, String],
      readVersion: Option[Long],
      isolationLevel: Option[String],
      isBlindAppend: Option[Boolean]): CommitInfo = {
    val getUserName = commandContext.get("user").flatMap {
      case "unknown" => None
      case other => Option(other)
    }

    CommitInfo(
      None,
      new Timestamp(time),
      commandContext.get("userId"),
      getUserName,
      operation,
      operationParameters,
      JobInfo.fromContext(commandContext),
      NotebookInfo.fromContext(commandContext),
      commandContext.get("clusterId"),
      readVersion,
      isolationLevel,
      isBlindAppend
    )
  }
}

/** A serialization helper to create a common action envelope. */
case class SingleAction(
    txn: SetTransaction = null,
    add: AddFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    commitInfo: CommitInfo = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (txn != null) {
      txn
    } else if (protocol != null) {
      protocol
    } else if (commitInfo != null) {
      commitInfo
    } else {
      null
    }
  }
}

/** Serializes Maps containing JSON strings without extra escaping. */
class JsonMapSerializer extends JsonSerializer[Map[String, String]] {
  def serialize(
      parameters: Map[String, String],
      jgen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    jgen.writeStartObject()
    parameters.foreach { case (key, value) =>
      if (value == null) {
        jgen.writeNullField(key)
      } else {
        jgen.writeFieldName(key)
        // Write value as raw data, since it's already JSON text
        jgen.writeRawValue(value)
      }
    }
    jgen.writeEndObject()
  }
}
