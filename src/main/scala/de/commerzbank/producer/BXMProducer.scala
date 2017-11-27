package de.commerzbank.producer

import java.io.ByteArrayOutputStream
import java.util.{Properties, UUID}

import de.commerzbank.domain.User
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.avro.specific.SpecificDatumWriter

import scala.io.Source


class BXMProducer {

  private val props = new Properties

  props.put("metadata.broker.list", "localhost:9092")
  props.put("message.send.max.retries", "5")
  props.put("request.required.acks", "-1")
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("client.id", UUID.randomUUID().toString())

  private val producer = new Producer[String, Array[Byte]](new ProducerConfig(props))

  private val schemaString = Source.fromURL(getClass.getResource("/schema.avsc")).mkString
  private val schema = new Schema.Parser().parse(schemaString)

  def send(topic: String, users: List[User]): Unit = {
    val genericUser: GenericRecord = new GenericData.Record(schema)

    try {
      val queueMessages = users.map { user =>
        genericUser.put("id", user.id)
        genericUser.put("name", user.name)
        genericUser.put("email", user.email.orNull)

        // Serialize generic record object into byte array
        val writer = new SpecificDatumWriter[GenericRecord](schema)
        val out = new ByteArrayOutputStream()
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(genericUser, encoder)
        encoder.flush()
        out.close()

        val serializedBytes: Array[Byte] = out.toByteArray()

        new KeyedMessage[String, Array[Byte]](topic, serializedBytes)
      }

      producer.send(queueMessages: _*)

    } catch {
      case ex: Exception =>
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
    }
  }

}

object ProducerApp extends App {

  private val topic = "ptr.omega_bxm"

  val producer = new BXMProducer()

  val user1 = User(1, "Sushil Singh", None)
  val user2 = User(2, "Satendra Kumar Yadav", Some("satendra@knoldus.com"))

  producer.send(topic, List(user1, user2))
}