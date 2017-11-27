package de.commerzbank.process

import de.commerzbank.domain.User
import de.commerzbank.sink.DStreamSink
import de.commerzbank.source.DStreamProvider
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


class BXMProcess {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("BXMProcess")
  //    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val ssc = new StreamingContext(sparkConf, Seconds(2))
//      ssc.checkpoint("file://temporary-directory}") //when running on the cluster the Checkpointing dir should be on hdfs

  def start(): Unit = {
    val stream: DStream[User] = DStreamProvider.provide(ssc)
    val sink: DStreamSink[User] = new DStreamSink()

    sink.writeToHBase(ssc, stream)

    ssc.start()
    ssc.awaitTermination()
  }
}


object BXMProcess {
  def main(args: Array[String]): Unit = {
    val job = new BXMProcess()

    job.start()
  }

}