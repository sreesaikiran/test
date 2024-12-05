import com.mongodb.client.model.Filters
import com.mongodb.client.model.Updates._
import org.apache.spark.sql.Row
import org.mongodb.scala.MongoClient
import scala.util.Try

// Assuming `filterDf` is a DataFrame and `now` is the current timestamp in milliseconds
filterDf.foreachPartition { partition: Iterator[Row] =>
  val mongoClient = MongoClient(config.getString("mongodb.connectionString"))
  val database = mongoClient.getDatabase(config.getString("mongodb.database"))
  val collection = database.getCollection(config.getString("mongodb.activeCollection"))

  try {
    partition.foreach { row =>
      // Handle the "open_homes" field safely
      val openHomes = Option(row.getAs[Seq[Row]]("open_homes")).getOrElse(Seq.empty).filter { openHome =>
        val startTime = Try {
          openHome.getAs[Any]("open_house_start_time") match {
            case ts: java.sql.Timestamp => ts.getTime
            case s: String              => java.sql.Timestamp.valueOf(s).getTime
            case _                      => Long.MaxValue
          }
        }.getOrElse(Long.MaxValue) // Default to Long.MaxValue if parsing fails
        startTime >= now // Filter by the current timestamp
      }

      // Only update documents with non-empty "open_homes"
      if (openHomes.nonEmpty) {
        val openHomesHash = openHomes.map(_.hashCode()).sorted.hashCode() // Generate a consistent hash
        val update = combine(
          set("open_house.open_homes", openHomes.map(openHome => Map(
            "field1" -> openHome.getAs[Any]("field1"), // Replace "field1" with actual fields
            "field2" -> openHome.getAs[Any]("field2")  // Replace "field2" with actual fields
          ).asJava).asJava),
          set("open_house.hash_code", openHomesHash)
        )

        // Perform the update operation
        try {
          collection.updateOne(
            Filters.eq("_id", row.getAs[String]("_id")),
            update
          ).toFuture().recover {
            case e: Exception =>
              println(s"Failed to update document with _id ${row.getAs[String]("_id")}: ${e.getMessage}")
          }
        } catch {
          case e: Exception =>
            println(s"Error processing row with _id ${row.getAs[String]("_id")}: ${e.getMessage}")
        }
      }
    }
  } finally {
    // Ensure the MongoDB client is closed to release resources
    mongoClient.close()
  }
}
