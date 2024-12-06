package com.zaplabs

import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.client.{MongoClient, MongoClients}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row}
import org.bson.{BsonDateTime, Document}

import java.time.LocalDateTime
import java.time.ZoneOffset
import scala.collection.JavaConverters._
import scala.util.Try

object OpenHomesProcessor {

  def processPartition(df: DataFrame, config: Config, now: LocalDateTime): Unit = {
    df.foreachPartition { partition: Iterator[Row] =>
      val mongoClient: MongoClient = MongoClients.create(config.getString("mongodb.connectionString"))
      val database = mongoClient.getDatabase(config.getString("mongodb.database"))
      val collection = database.getCollection(config.getString("mongodb.activeCollection"))

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

            val openHouseDocument = new Document("open_homes", openHomes.map { openHome =>
              openHome.schema.fieldNames.map { fieldName =>
                fieldName -> openHome.getAs[Any](fieldName)
              }.toMap.asJava
            }.asJava)

            Updates.combine(
              Updates.set("open_house.open_homes", openHouseDocument),
              Updates.set("open_house.hash_code", openHomesHash),
              Updates.set("last_change_date", processingDate),
              Updates.set("property.listing.dates.last_change_date", processingDate)
            )
          } else {
            val openHouseDocument = new Document()
              .append("open_homes", Seq.empty.asJava)
              .append("is_open_homes", false)

            Updates.combine(
              Updates.set("open_house.open_homes", openHouseDocument),
              Updates.set("open_house.hash_code", 0),
              Updates.set("last_change_date", processingDate),
              Updates.set("property.listing.dates.last_change_date", processingDate)
            )
          }

          Try(collection.updateOne(filter, update)).recover {
            case ex: Exception =>
              println(s"Failed to update document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
          }
        }
      } finally {
        mongoClient.close()
      }
    }
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
