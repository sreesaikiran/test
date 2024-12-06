package com.zaplabs

import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row}
import org.bson.{BsonDateTime, Document}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.LocalDateTime
import java.time.ZoneOffset
import scala.collection.JavaConverters._
import scala.util.Try

object OpenHomesProcessor {

  def processPartition(df: DataFrame, config: Config, now: LocalDateTime, batchSize: Int): Unit = {
    df.foreachPartition { partition: Iterator[Row] =>
      val mongoClient: MongoClient = MongoClients.create(config.getString("mongodb.connectionString"))
      val database = mongoClient.getDatabase(config.getString("mongodb.database"))
      val collection = database.getCollection(config.getString("mongodb.activeCollection"))

      // Kafka configuration
      val kafkaProducerProps = new java.util.Properties()
      kafkaProducerProps.put("bootstrap.servers", config.getString("kafka.bootstrap.servers"))
      kafkaProducerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaProducerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

      val kafkaProducer = new KafkaProducer[String, String](kafkaProducerProps)
      val kafkaTopic = config.getString("kafka.topic")

      // Buffer to hold Kafka messages
      val messagesBuffer = scala.collection.mutable.ArrayBuffer[ProducerRecord[String, String]]()

      try {
        partition.foreach { row =>
          val nowMillis = now.toInstant(ZoneOffset.UTC).toEpochMilli

          // Extract and filter open homes
          val openHomes = Option(row.getAs[Seq[Row]]("open_homes")).getOrElse(Seq.empty).filter { openHome =>
            extractTimestamp(openHome.getAs[Any]("open_house_start_time")).exists(_ >= nowMillis)
          }

          val processingDate = new BsonDateTime(System.currentTimeMillis())
          val filter = Filters.eq("_id", row.getAs[String]("_id"))

          val update = if (openHomes.nonEmpty) {
            val openHomesHash = openHomes
              .map(entry => (
                extractTimestamp(entry.getAs[Any]("open_house_date")).getOrElse(Long.MaxValue),
                extractTimestamp(entry.getAs[Any]("open_house_start_time")).getOrElse(Long.MaxValue),
                extractTimestamp(entry.getAs[Any]("open_house_end_time")).getOrElse(Long.MaxValue),
                entry.getAs[Boolean]("is_canceled")
              ))
              .sortBy(_.hashCode())
              .hashCode()

            val openHomesArray = openHomes.map { openHome =>
              val fields: java.util.Map[String, Any] = openHome.schema.fieldNames.map { fieldName =>
                fieldName -> openHome.getAs[Any](fieldName)
              }.toMap.asJava

              new Document(fields)
            }.asJava

            Updates.combine(
              Updates.set("open_house.open_homes", openHomesArray),
              Updates.set("open_house.hash_code", openHomesHash),
              Updates.set("last_change_date", processingDate),
              Updates.set("property.listing.dates.last_change_date", processingDate)
            )
          } else {
            Updates.combine(
              Updates.set("open_house.open_homes", Seq.empty.asJava),
              Updates.set("open_house.is_open_homes", false),
              Updates.set("open_house.hash_code", 0),
              Updates.set("last_change_date", processingDate),
              Updates.set("property.listing.dates.last_change_date", processingDate)
            )
          }

          // Perform the update
          Try(collection.updateOne(filter, update)).recover {
            case ex: Exception =>
              println(s"Failed to update document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
          }

          // Retrieve the updated document and prepare Kafka message
          Try {
            val updatedDocument = collection.find(filter).first()
            updatedDocument.map { doc =>
              val key = row.getAs[String]("_id")
              val value = doc.toJson
              messagesBuffer.append(new ProducerRecord[String, String](kafkaTopic, key, value))
            }
          }.recover {
            case ex: Exception =>
              println(s"Failed to retrieve updated document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
          }

          // Send batch to Kafka if buffer size exceeds batchSize
          if (messagesBuffer.size >= batchSize) {
            sendBatchToKafka(kafkaProducer, messagesBuffer)
            messagesBuffer.clear()
          }
        }

        // Send remaining messages in the buffer
        if (messagesBuffer.nonEmpty) {
          sendBatchToKafka(kafkaProducer, messagesBuffer)
          messagesBuffer.clear()
        }
      } finally {
        kafkaProducer.close()
        mongoClient.close()
      }
    }
  }

  /**
   * Utility method to send a batch of messages to Kafka.
   */
  private def sendBatchToKafka(producer: KafkaProducer[String, String], messagesBuffer: Seq[ProducerRecord[String, String]]): Unit = {
    messagesBuffer.foreach { message =>
      Try(producer.send(message)).recover {
        case ex: Exception =>
          println(s"Failed to send message: ${message.key()} -> ${message.value()}. Error: ${ex.getMessage}")
      }
    }
    println(s"Sent batch of ${messagesBuffer.size} messages to Kafka.")
  }

  /**
   * Utility method to extract timestamps.
   */
  private def extractTimestamp(value: Any): Option[Long] = {
    Try {
      value match {
        case ts: java.sql.Timestamp => ts.getTime
        case s: String              => java.sql.Timestamp.valueOf(s).getTime
        case l: Long                => l
        case _                      => throw new IllegalArgumentException("Invalid timestamp type")
      }
    }.toOption
  }
}
