package com.zaplabs

import com.mongodb.client.model.{Filters, Updates}
import com.mongodb.client.{MongoClient, MongoClients}
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, Row}
import org.bson.{BsonDateTime, Document}

import java.time.LocalDateTime
import scala.collection.JavaConverters.asJavaIterableConverter
import scala.util.Try

object OpenHomesProcessor {


  def processPartition(df: DataFrame, config: Config, now: LocalDateTime): Unit = {
    df.foreachPartition { partition: Iterator[Row] =>
      val mongoClient: MongoClient = MongoClients.create(config.getString("mongodb.connectionString"))
      val database = mongoClient.getDatabase(config.getString("mongodb.database"))
      val collection = database.getCollection(config.getString("mongodb.activeCollection"))

      try {
        partition.foreach { row =>
          val nowMillis = now.toInstant(java.time.ZoneOffset.UTC).toEpochMilli
          val openHomes = Option(row.getAs[Seq[Row]]("open_homes")).getOrElse(Seq.empty).filter { openHome =>
            val startTime = Try {
              openHome.getAs[Any]("open_house_start_time") match {
                case ts: java.sql.Timestamp => ts.getTime
                case s: String => java.sql.Timestamp.valueOf(s).getTime
                case _ => Long.MaxValue
              }
            }.getOrElse(Long.MaxValue)
            startTime >= nowMillis
          }

          val processingDate = new BsonDateTime(System.currentTimeMillis())
          val filter = Filters.eq("_id", row.getAs[String]("_id"))
          val update = if (openHomes.nonEmpty) {
            val openHomesHash = openHomes
              .map { entry =>
                (
                  entry.getAs[Any]("open_house_date") match {
                    case ts: java.sql.Timestamp => ts.getTime
                    case s: String => java.sql.Timestamp.valueOf(s).getTime
                    case l: Long => l
                    case _ => Long.MaxValue
                  },
                  entry.getAs[Any]("open_house_start_time") match {
                    case ts: java.sql.Timestamp => ts.getTime
                    case s: String => java.sql.Timestamp.valueOf(s).getTime
                    case l: Long => l
                    case _ => Long.MaxValue
                  },
                  entry.getAs[Any]("open_house_end_time") match {
                    case ts: java.sql.Timestamp => ts.getTime
                    case s: String => java.sql.Timestamp.valueOf(s).getTime
                    case l: Long => l
                    case _ => Long.MaxValue
                  },
                  entry.getAs[Boolean]("is_canceled")
                )
              }
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
            val openHouseDocument = new Document("open_homes", Seq.empty.asJava)
            openHouseDocument.append("is_open_homes", false)
            Updates.combine(
              Updates.set("open_house.open_homes", openHouseDocument),
              Updates.set("open_house.hash_code", 0),
              Updates.set("last_change_date", processingDate),
              Updates.set("property.listing.dates.last_change_date", processingDate)
            )
          }

          collection.updateOne(filter, update)
        }
      } finally {
        mongoClient.close()
      }
    }
  }


}
