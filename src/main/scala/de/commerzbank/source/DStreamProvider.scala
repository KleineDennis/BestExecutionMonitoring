package de.commerzbank.source

import de.commerzbank.domain.User
import kafka.serializer.DefaultDecoder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.{DatumReader, Decoder, DecoderFactory}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.io.Source
import scala.util.Try


object DStreamProvider {
  private val schemaString = Source.fromURL(getClass.getResource("/schema.avsc")).mkString
  private val schema: Schema = new Schema.Parser().parse(schemaString)

  def provide(ssc: StreamingContext): DStream[User] = {
    val Array(brokers, topics) = Array[String]("localhost:9092", "ptr.omega_bxm")
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

    KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder](
      ssc,
      kafkaParams,
      topicsSet
    ).map(deserialize)
  }

  private def deserialize(message: (Array[Byte], Array[Byte])): User = {
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message._2, null)
    val userData: GenericRecord = reader.read(null, decoder)

    User(userData.get("id").toString.toInt,
      userData.get("name").toString,
      Try(userData.get("email").toString).toOption)
  }
}
