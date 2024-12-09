import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.model.{BatchWriteItemRequest, PutRequest, WriteRequest}
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import org.apache.spark.sql.{DataFrame, Row}

import java.time.{LocalDateTime, ZoneOffset}
import scala.collection.JavaConverters._

object ExpiredOpenHomesProcessor {

  def processPartition(df: DataFrame, dynamoDBTableName: String, now: LocalDateTime): Unit = {
    df.foreachPartition { partition: Iterator[Row] =>
      // Create a DynamoDB client for the partition
      val dynamoDBClient: AmazonDynamoDB = AmazonDynamoDBClientBuilder.defaultClient()
      val dynamoDB = new DynamoDB(dynamoDBClient)

      try {
        // Buffer for batch writes
        val writeRequestsBuffer = scala.collection.mutable.ArrayBuffer[WriteRequest]()

        partition.foreach { row =>
          val openHomes = Option(row.getAs[Seq[Row]]("open_homes")).getOrElse(Seq.empty)

          // Filter expired open homes
          val expiredOpenHomes = openHomes.filter { openHome =>
            val endTime = extractTimestamp(openHome.getAs[Any]("open_house_end_time"))
            endTime.exists(_ < now.toInstant(ZoneOffset.UTC).toEpochMilli)
          }

          expiredOpenHomes.foreach { expiredOpenHome =>
            // Convert expired open home to DynamoDB item
            val itemMap = expiredOpenHome.schema.fieldNames.map { fieldName =>
              fieldName -> convertToDynamoDBValue(expiredOpenHome.getAs[Any](fieldName))
            }.toMap.asJava

            val putRequest = new PutRequest().withItem(itemMap)
            writeRequestsBuffer += new WriteRequest().withPutRequest(putRequest)

            // Write in batches when buffer reaches 25 (DynamoDB batch size limit)
            if (writeRequestsBuffer.size >= 25) {
              batchWriteToDynamoDB(dynamoDBClient, dynamoDBTableName, writeRequestsBuffer)
              writeRequestsBuffer.clear()
            }
          }
        }

        // Write remaining items in the buffer
        if (writeRequestsBuffer.nonEmpty) {
          batchWriteToDynamoDB(dynamoDBClient, dynamoDBTableName, writeRequestsBuffer)
          writeRequestsBuffer.clear()
        }

      } finally {
        dynamoDBClient.shutdown()
      }
    }
  }

  // Utility function to perform batch write
  private def batchWriteToDynamoDB(dynamoDBClient: AmazonDynamoDB, tableName: String, writeRequests: Seq[WriteRequest]): Unit = {
    val batchWriteRequest = new BatchWriteItemRequest()
      .withRequestItems(Map(tableName -> writeRequests.asJava).asJava)

    try {
      val result = dynamoDBClient.batchWriteItem(batchWriteRequest)
      if (!result.getUnprocessedItems.isEmpty) {
        println(s"Unprocessed items: ${result.getUnprocessedItems}")
        // Retry logic can be added here if necessary
      }
    } catch {
      case e: ResourceNotFoundException =>
        println(s"Table $tableName not found: ${e.getMessage}")
      case e: Exception =>
        println(s"Failed to write batch to DynamoDB: ${e.getMessage}")
    }
  }

  // Utility to extract timestamp
  private def extractTimestamp(value: Any): Option[Long] = {
    value match {
      case ts: java.sql.Timestamp => Some(ts.getTime)
      case s: String              => Some(java.sql.Timestamp.valueOf(s).getTime)
      case l: Long                => Some(l)
      case _                      => None
    }
  }

  // Utility to convert Spark types to DynamoDB attribute values
  private def convertToDynamoDBValue(value: Any): AnyRef = {
    value match {
      case null => null
      case s: String => s
      case i: Int => i.asInstanceOf[AnyRef]
      case l: Long => l.asInstanceOf[AnyRef]
      case d: Double => d.asInstanceOf[AnyRef]
      case b: Boolean => b.asInstanceOf[AnyRef]
      case ts: java.sql.Timestamp => ts.toString
      case seq: Seq[_] => seq.map(convertToDynamoDBValue).asJava
      case _ => throw new IllegalArgumentException(s"Unsupported type: ${value.getClass}")
    }
  }
}
