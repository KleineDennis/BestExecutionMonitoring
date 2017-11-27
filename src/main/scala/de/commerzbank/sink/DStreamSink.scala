package de.commerzbank.sink

import java.util.Properties

import de.commerzbank.domain.User
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.kafka.clients.producer._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.codehaus.jackson.map.ObjectMapper


class DStreamSink[T] {

  def writeToHBase(ssc: StreamingContext, dstream: DStream[User]) = {
    val table = "stock"
    val family = "avgprice"
    val qualifier = "price"

    dstream.foreachRDD(rdd => {
      val conf = HBaseConfiguration.create()
      conf.set(TableOutputFormat.OUTPUT_TABLE, table)
      conf.set("hbase.master", "localhost:60000")

      val jobConf = new Configuration(conf)
      jobConf.set("mapreduce.job.outputformat.class", classOf[TableOutputFormat[Text]].getName)

      rdd.map(record => {
        val put = new Put(Bytes.toBytes(record.id))
        put.addColumn(family.getBytes, qualifier.getBytes, Bytes.toBytes(record.name))
        (record.id, put)
      }).saveAsNewAPIHadoopDataset(jobConf)
    })
  }

  def writeToKafka(ssc: StreamingContext, result: DStream[User]) = {
    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer: Producer[String, String] = new KafkaProducer[String, String](props)
    val objectMapper: ObjectMapper = new ObjectMapper()

    val producerVar = ssc.sparkContext.broadcast(kafkaProducer)
    val topicVar = ssc.sparkContext.broadcast("test")
    val objectMapperVar = ssc.sparkContext.broadcast[ObjectMapper](objectMapper)

    result.foreachRDD { rdd =>
      rdd.foreach { record =>
        val topic = topicVar.value
        val producer = producerVar.value

        producer.send(
          new ProducerRecord(
            topic,
            record.name,
            objectMapperVar.value.writeValueAsString(record) //in production ready app consider using avro format
          ),
          new KafkaCallbackHandler()
        )
      }
    }
  }

}


class KafkaCallbackHandler extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    if (exception != null) {
      exception.printStackTrace()
    }
  }
}
