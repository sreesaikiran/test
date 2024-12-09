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
