package com.zaplabs

import com.typesafe.config.Config
import com.zaplabs.client.kafka.MdpKafkaProducer
import com.zaplabs.helper.listing.KafkaHelper
import com.zaplabs.utils.ConfigUtils
import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, Row}
import org.mongodb.scala.bson.collection.immutable.Document
import org.mongodb.scala.bson.{BsonArray, BsonBoolean, BsonDateTime, BsonDouble, BsonInt32, BsonString}
import org.mongodb.scala.model.{Filters, Updates}
import org.mongodb.scala.{MongoClient, _}

import java.time.{Instant, LocalDateTime, ZoneOffset}
import scala.collection.JavaConverters._
import scala.collection.mutable.WrappedArray
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.util.Try
import scala.util.chaining.scalaUtilChainingOps

object OpenHomesProcessor {

  def processPartition(df: DataFrame, config: Config, now: LocalDateTime, mlsName: String, batchSize: Int): Unit = {
    df.foreachPartition { partition: Iterator[Row] =>
      val mongoClient: MongoClient = MongoClient(config.getString("mongodb.connectionString"))
      val database: MongoDatabase = mongoClient.getDatabase(config.getString("mongodb.database"))
      val collection: MongoCollection[Document] = database.getCollection(config.getString("mongodb.activeCollection"))

      val startTime = Instant.now()
      val kafkaTopic = config.getString("publishJob.kafkaTopic")
      val kafkaClientId = config.getString("kafkaConnectorConfiguration.clientName")
      val kafkaConfig = new java.util.Properties()
          .tap(_.putAll(ConfigUtils.flatten(config.getObject("kafkaConnectorConfiguration")).asJava))
      val kafkaSchemaPath = config.getString("publishJob.schemaPath")
      val clazz = getClass
      val schemaPath = clazz.getResourceAsStream(kafkaSchemaPath)
      val kafkaProducer = new MdpKafkaProducer(
        kafkaTopic,
        kafkaClientId,
        kafkaConfig,
        new Schema.Parser().parse(schemaPath)
      )
      // Buffer to hold Kafka messages & Mongodb Docs
      val messagesBuffer = scala.collection.mutable.ArrayBuffer[Document]()

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

            val openHouseArray = openHomes.map { openHome =>
              val fields = openHome.schema.fieldNames.map { fieldName =>
                val value = openHome.getAs[Any](fieldName) match {
                  case s: String              => BsonString(s)
                  case i: Int                 => BsonInt32(i)
                  case b: Boolean             => BsonBoolean(b)
                  case d: Double              => BsonDouble(d)
                  case ts: java.sql.Timestamp => BsonDateTime(ts.getTime)
                  case arr: WrappedArray[_] =>
                    BsonArray(arr.map {
                      case s: String              => BsonString(s)
                      case i: Int                 => BsonInt32(i)
                      case b: Boolean             => BsonBoolean(b)
                      case d: Double              => BsonDouble(d)
                      case ts: java.sql.Timestamp => BsonDateTime(ts.getTime)
                      case other =>
                        throw new IllegalArgumentException(s"Unsupported type in array: ${other.getClass}")
                    }.toSeq) // Ensure it's a Scala Seq
                  case other =>
                    throw new IllegalArgumentException(s"Unsupported type: ${other.getClass}")
                }
                fieldName -> value
              }.toMap
              Document(fields)
            }

            Updates.combine(
              Updates.set("open_house.open_homes", openHouseArray),
              Updates.set("open_house.hash_code", openHomesHash),
              Updates.set("last_change_date", processingDate),
              Updates.set("property.listing.dates.last_change_date", processingDate)
            )
          } else {
            Updates.combine(
              Updates.set("open_house.open_homes", Seq.empty.asJava),
              Updates.set("open_house.hash_code", 0),
              Updates.set("open_house.is_open_homes", false),
              Updates.set("last_change_date", processingDate),
              Updates.set("property.listing.dates.last_change_date", processingDate)
            )
          }

          // Perform the update synchronously
          try {
            val updateResult = Await.result(collection.updateOne(filter, update).toFuture(), 500.seconds)
            if (updateResult.wasAcknowledged()) {
              val updatedDocument = Await.result(collection.find(filter).first().toFuture(), 500.seconds)
              messagesBuffer += updatedDocument
              if (messagesBuffer.size >= batchSize) {
                sendBatchToKafka(kafkaProducer, messagesBuffer, mlsName, startTime)
                messagesBuffer.clear()
              }
            } else {
              println(s"Failed to update document with _id ${row.getAs[String]("_id")}")
            }
          } catch {
            case ex: Exception =>
              println(s"Error while updating document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
          }
        }

        // Send remaining messages in the buffer
        if (messagesBuffer.nonEmpty) {
          sendBatchToKafka(kafkaProducer, messagesBuffer, mlsName, startTime)
          messagesBuffer.clear()
        }

      } finally {

        kafkaProducer.flush()
        kafkaProducer.close()
        mongoClient.close()
      }
    }

  }

  /**
   * Utility method to send a batch of messages to Kafka.
   */
  private def sendBatchToKafka(kafkaProducer: MdpKafkaProducer, messagesBuffer: Seq[Document], mlsName:String, startTime:Instant): Unit = {
    messagesBuffer.foreach { message =>
      Try{
        val processingTs = Instant.now()
        KafkaHelper.sendMessageToKafkaSecondaryUpdates(
          message,
          mlsName,
          "default_job_id",
          "open_homes",
          processingTs,
          startTime.toEpochMilli,
          kafkaProducer
        )
      }.recover {
        case ex: Exception =>
          println(s"Failed to send document ID: ${message.get("_id")}. Error: ${ex.getMessage}")
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
