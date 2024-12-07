import org.apache.spark.sql.{DataFrame, Row}
import org.mongodb.scala._
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Updates
import org.mongodb.scala.bson.BsonDateTime
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success}

object OpenHomesProcessor {

  def processPartition(df: DataFrame, config: Config, now: LocalDateTime, mlsName: String, batchSize: Int): Unit = {
    df.foreachPartition { partition =>
      // Create MongoClient inside foreachPartition
      val mongoClient: MongoClient = MongoClient(config.getString("mongodb.connectionString"))
      val database: MongoDatabase = mongoClient.getDatabase(config.getString("mongodb.database"))
      val collection: MongoCollection[Document] = database.getCollection(config.getString("mongodb.activeCollection"))

      val messagesBuffer = ArrayBuffer[Document]()

      try {
        partition.foreach { row =>
          val id = row.getAs[String]("_id")
          if (id == null || id.isEmpty) {
            println(s"Skipping row with missing or invalid _id: $row")
          } else {
            val nowMillis = now.toInstant(ZoneOffset.UTC).toEpochMilli

            // Example processing logic for openHomes
            val openHomes = Option(row.getAs[Seq[Row]]("open_homes")).getOrElse(Seq.empty).filter { openHome =>
              extractTimestamp(openHome.getAs[Any]("open_house_start_time")).exists(_ >= nowMillis)
            }

            val processingDate = BsonDateTime(System.currentTimeMillis())
            val filter = Filters.eq("_id", id)

            val update = if (openHomes.nonEmpty) {
              val openHomesHash = openHomes.map(_.hashCode()).sorted.hashCode()
              Updates.combine(
                Updates.set("open_house.open_homes", openHomes.map(_.toString)),
                Updates.set("open_house.hash_code", openHomesHash),
                Updates.set("last_change_date", processingDate)
              )
            } else {
              Updates.combine(
                Updates.set("open_house.open_homes", Seq.empty[String]),
                Updates.set("open_house.hash_code", 0),
                Updates.set("last_change_date", processingDate)
              )
            }

            // Perform the update operation
            val updateFuture = collection.updateOne(filter, update).toFuture()
            updateFuture.onComplete {
              case Success(_) =>
                // Retrieve and add the updated document to buffer
                val findFuture = collection.find(filter).first().toFuture()
                findFuture.onComplete {
                  case Success(updatedDocument) =>
                    messagesBuffer += updatedDocument
                    if (messagesBuffer.size >= batchSize) {
                      sendBatchToKafka(messagesBuffer, mlsName)
                      messagesBuffer.clear()
                    }
                  case Failure(ex) =>
                    println(s"Failed to retrieve updated document with _id $id: ${ex.getMessage}")
                }
              case Failure(ex) =>
                println(s"Failed to update document with _id $id: ${ex.getMessage}")
            }
          }
        }

        // Send remaining messages in the buffer
        if (messagesBuffer.nonEmpty) {
          sendBatchToKafka(messagesBuffer, mlsName)
          messagesBuffer.clear()
        }
      } finally {
        mongoClient.close()
      }
    }
  }

  private def sendBatchToKafka(messagesBuffer: ArrayBuffer[Document], mlsName: String): Unit = {
    println(s"Sending ${messagesBuffer.size} messages to Kafka for MLS: $mlsName.")
    messagesBuffer.foreach { doc =>
      println(doc.toJson) // Replace with Kafka producer logic
    }
  }

  private def extractTimestamp(value: Any): Option[Long] = {
    try {
      value match {
        case ts: java.sql.Timestamp => Some(ts.getTime)
        case s: String              => Some(java.sql.Timestamp.valueOf(s).getTime)
        case l: Long                => Some(l)
        case _                      => None
      }
    } catch {
      case _: Throwable => None
    }
  }
}
