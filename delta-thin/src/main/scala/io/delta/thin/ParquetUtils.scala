package io.delta.thin

import com.github.mjakubowski84.parquet4s.ParquetReader
import io.delta.thin.storage.HDFSLogStore
import io.delta.thin.util.JsonUtils
import org.apache.hadoop.conf.Configuration

object ParquetUtils {

  def main(args: Array[String]): Unit = {
    val xx = "file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000000.json"
    val parquetIterable = ParquetReader.read[SingleAction](xx)

    println("00000")

  }
//  val x = new HDFSLogStore(new Configuration())
//  val y = x.read("file:///Users/songjun.sj/Desktop/testdelta/_delta_log/00000000000000000000.json")
//    .map{ line =>JsonUtils.mapper.readValue[SingleAction](line) }

}
