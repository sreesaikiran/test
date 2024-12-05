filterDf.foreachPartition { partition: Iterator[Row] =>
      val mongoClient = MongoClient(config.getString("mongodb.connectionString"))
      val database = mongoClient.getDatabase(config.getString("mongodb.database"))
      val collection = database.getCollection(config.getString("mongodb.activeCollection"))

      partition.foreach { row =>
        val openHomes = row.getAs[Seq[Row]]("open_homes") match {
          case null => Seq.empty[Row]
          case homes => homes.filter { openHome =>
            val startTime = openHome.getAs[Any]("open_house_start_time") match {
              case ts: java.sql.Timestamp => ts.getTime
              case s: String => Timestamp.valueOf(s).getTime
              case _ => Long.MaxValue
            }
            startTime >= now
          }
        }

        if (openHomes.nonEmpty) {
          val openHomesHash = openHomes.map(_.hashCode()).sorted.hashCode()
          val update = combine(
            set("open_house.open_homes", openHomes),
            set("open_house.hash_code", openHomesHash)
          )
          collection.updateOne(Filters.eq("_id", row.getAs[String]("_id")), update).toFuture().recover {
            case e: Exception => println(s"Failed to update document: ${e.getMessage}")
          }
        }
      }

      mongoClient.close()
    }
