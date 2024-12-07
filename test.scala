def processPartition(df: DataFrame, config: Config, now: LocalDateTime, mlsName: String, batchSize: Int): Unit = {
    df.foreachPartition { partition: Iterator[Row] =>
      val mongoClient: MongoClient = MongoClients.create(config.getString("mongodb.connectionString"))
      val database = mongoClient.getDatabase(config.getString("mongodb.database"))
      val collection = database.getCollection(config.getString("mongodb.activeCollection"))
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
      // Buffer to hold Kafka messages
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
              val fields: java.util.Map[String, Object] = openHome.schema.fieldNames.map { fieldName =>
                fieldName -> openHome.getAs[Any](fieldName).asInstanceOf[Object]
              }.toMap.asJava
              new Document(fields)
            }.asJava

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

          Try(collection.updateOne(filter, update)).recover {
            case ex: Exception =>
              println(s"Failed to update document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
          }

          // Retrieve updated document & send to kafka
          Try {
            val updatedDocument = collection.find(filter).first()
            messagesBuffer += updatedDocument
          }.recover{
            case ex: Exception =>
              println(s"Failed to retrieve updated document with _id ${row.getAs[String]("_id")}: ${ex.getMessage}")
          }

          // Send batch to Kafka if buffer size exceeds batchSize
          if (messagesBuffer.size >= batchSize) {
            sendBatchToKafka(kafkaProducer, messagesBuffer, mlsName, startTime)
            messagesBuffer.clear()
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
