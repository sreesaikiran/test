import org.mongodb.scala._
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Updates
import org.mongodb.scala.bson.BsonDateTime
import org.apache.spark.sql.{DataFrame, Row}
import com.typesafe.config.Config
import java.time.{LocalDateTime, ZoneOffset, Instant}
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

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
      kafkaConfig.putAll(ConfigUtils.flatten(config.getObject("kafkaConnectorConfiguration")).asJava)
      val kafkaSchemaPath = config.getString("publishJob.schemaPath")
      val clazz = getClass
      val schemaPath = clazz.getResourceAsStream(kafkaSchemaPath)
      val kafkaProducer = new MdpKafkaProducer(
        kafkaTopic,
        kafkaClientId,
        kafkaConfig,
        new Schema.Parser().parse(schemaPath)
      )

      // Buffer to hold Kafka messages
      val messagesBuffer = ArrayBuffer[Document]()

      try {
        partition.foreach { row =>
          val nowMillis = now.toInstant(ZoneOffset.UTC).toEpochMilli

          // Extract and filter open homes
          val openHomes = Option(row.getAs[Seq[Row]]("open_homes")).getOrElse(Seq.empty).filter { openHome =>
            extractTimestamp(openHome.getAs[Any]("open_house_start_time")).exists(_ >= nowMillis)
          }

          val processingDate = BsonDateTime(System.currentTimeMillis())
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
              Document(openHome.schema.fieldNames.map { fieldName =>
                fieldName -> openHome.getAs[Any](fieldName)
              }: _*)
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

          // Perform the update asynchronously
          val updateFuture = collection.updateOne(filter, update).toFuture()
          updateFuture.onComplete {
            case Success(_) =>
              // Retrieve the updated document & add to buffer
              val findFuture = collection.find(filter).first().toFuture()
              findFuture.onComplete {
                case Success(updatedDocument) =>
                  messagesBuffer += updatedDocument
                  if (messagesBuffer.size >= batchSize) {
                    sendBatchToKafka(kafkaProducer, messagesBuffer, mlsName, startTime)
                    messagesBuffer.clear()
                  }
                case Failure(ex) =>
                  println(s"Failed to retrieve updated document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
              }
            case Failure(ex) =>
              println(s"Failed to update document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
          }
        }

        // Send remaining messages in the buffer
        if (messagesBuffer.nonEmpty) {
          sendBatchToKafka(kafkaProducer, messagesBuffer, mlsName, startTime)
          messagesBuffer.clear()
        }
      } finally {
        mongoClient.close()
        kafkaProducer.flush()
        kafkaProducer.close()
      }
    }
  }

  private def sendBatchToKafka(
      kafkaProducer: MdpKafkaProducer,
      messagesBuffer: ArrayBuffer[Document],
      mlsName: String,
      startTime: Instant
  ): Unit = {
    messagesBuffer.foreach { document =>
      kafkaProducer.send(document.toJson) // Replace with your Kafka producer send logic
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
