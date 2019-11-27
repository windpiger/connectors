package io.delta.thin.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import io.delta.thin.actions.SingleAction
import io.delta.thin.storage.HDFSLogStore
import org.apache.hadoop.conf.Configuration

/** Useful json functions used around the Delta codebase. */
object JsonUtils {
  /** Used to convert between classes and JSON. */
  val mapper = new ObjectMapper with ScalaObjectMapper
  mapper.setSerializationInclusion(Include.NON_ABSENT)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.registerModule(DefaultScalaModule)

  def toJson[T: Manifest](obj: T): String = {
    mapper.writeValueAsString(obj)
  }

  def toPrettyJson[T: Manifest](obj: T): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  def fromJson[T: Manifest](json: String): T = {
    mapper.readValue[T](json)
  }

  def main(args: Array[String]): Unit = {
    val x = new HDFSLogStore(new Configuration())
    val y = x.read("file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000000.json")
      .map{ line =>JsonUtils.mapper.readValue[SingleAction](line) }

//    val z = JsonUtils.mapper.readValue[SingleAction](y.head)
    println(y)
  }
}