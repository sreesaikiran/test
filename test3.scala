import org.mongodb.scala.model.WriteModel
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.{UpdateOneModel, BulkWriteOptions}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Try

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

      // Buffers for batching
      val updatesBuffer = scala.collection.mutable.ArrayBuffer[WriteModel[Document]]()
      val messagesBuffer = scala.collection.mutable.ArrayBuffer[Document]()

      try {
        partition.foreach { row =>
          val nowMillis = now.toInstant(ZoneOffset.UTC).toEpochMilli

          // Extract and filter open homes
          val openHomes = Option(row.getAs[Seq[Row]]("open_homes")).getOrElse(Seq.empty).filter { openHome =>
            extractTimestamp(openHome.getAs[Any]("open_house_start_time")).exists(_ >= nowMillis)
          }

          val processingDate = new BsonDateTime(System.currentTimeMillis())
          val filter = eq("_id", row.getAs[String]("_id"))

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
                    }.toSeq)
                  case other =>
                    throw new IllegalArgumentException(s"Unsupported type: ${other.getClass}")
                }
                fieldName -> value
              }.toMap
              Document(fields)
            }

            combine(
              set("open_house.open_homes", openHouseArray),
              set("open_house.hash_code", openHomesHash),
              set("last_change_date", processingDate),
              set("property.listing.dates.last_change_date", processingDate)
            )
          } else {
            combine(
              set("open_house.open_homes", Seq.empty.asJava),
              set("open_house.hash_code", 0),
              set("open_house.is_open_homes", false),
              set("last_change_date", processingDate),
              set("property.listing.dates.last_change_date", processingDate)
            )
          }

          // Add the update operation to the buffer
          updatesBuffer += UpdateOneModel(filter, update)

          // Perform bulk write when buffer reaches batchSize
          if (updatesBuffer.size >= batchSize) {
            val bulkWriteResult = Await.result(collection.bulkWrite(updatesBuffer.asJava, BulkWriteOptions().ordered(false)).toFuture(), 500.seconds)
            println(s"Bulk write completed. Matched: ${bulkWriteResult.getMatchedCount}, Modified: ${bulkWriteResult.getModifiedCount}")
            updatesBuffer.clear()
          }

          // Optionally fetch the updated document for Kafka
          val updatedDocument = Document("_id" -> row.getAs[String]("_id")) // Add relevant fields
          messagesBuffer += updatedDocument
          if (messagesBuffer.size >= batchSize) {
            sendBatchToKafka(kafkaProducer, messagesBuffer, mlsName, startTime)
            messagesBuffer.clear()
          }
        }

        // Write remaining updates in the buffer
        if (updatesBuffer.nonEmpty) {
          val bulkWriteResult = Await.result(collection.bulkWrite(updatesBuffer.asJava, BulkWriteOptions().ordered(false)).toFuture(), 500.seconds)
          println(s"Final bulk write completed. Matched: ${bulkWriteResult.getMatchedCount}, Modified: ${bulkWriteResult.getModifiedCount}")
          updatesBuffer.clear()
        }

        // Send remaining Kafka messages
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

  private def sendBatchToKafka(kafkaProducer: MdpKafkaProducer, messagesBuffer: Seq[Document], mlsName: String, startTime: Instant): Unit = {
    messagesBuffer.foreach { message =>
      Try {
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
