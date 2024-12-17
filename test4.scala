package com.zaplabs
package webapi.downloader.startegy

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.typesafe.scalalogging.StrictLogging
import com.zaplabs.AwsBatchMain.config
import com.zaplabs.dao.{DaoLoader, PostgresDownloaderDao}
import com.zaplabs.utils.DateUtils.convertToInstant
import com.zaplabs.utils.{DataDogLogging, PhotoUtils}
import com.zaplabs.webapi.WebApiUtils
import com.zaplabs.webapi.WebApiUtils.DownloadPageFailure
import com.zaplabs.webapi.dto.{ImagesMetaDataTrestleSpecific, MediaInfoTrestleSpecific}
import org.asynchttpclient.AsyncHttpClient
import play.api.libs.json.Json

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutorService, Future}

class TrestleStrategy(
    downloadAssistant: PhotoUtils.DownloadProcessAssistant,
    threadPool: ExecutionContextExecutorService,
    source: String,
    processId: String,
    waitTimeInMilliSecBetweenWebAPICalls: Int,
    outputSubPath: String,
    outputLogPath: String
) extends StrictLogging
    with DownloadStrategy {

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)

  val postgresDao = DaoLoader.loadDao(config, "postgres").asInstanceOf[PostgresDownloaderDao]
  var useCustomOrder = postgresDao.getMdpProperty("use.trestle.photo.customorder.field", source).toBoolean
  var includeFloorPlan = postgresDao.getMdpProperty("include.floor.plan.photo.type", source).toBoolean

  override def processImages(
      imageUrls: WebApiPhotoUrl,
      initialRequestToken: String,
      useHeader: Boolean,
      headerKeyValue: Map[String, String],
      ahc: AsyncHttpClient
  ): Future[Unit] = {
    val listingNum = imageUrls.listingId
    val startTime = convertToInstant(outputSubPath)
    logger.info(s"Downloading photos for ${listingNum}, url ${imageUrls.url}")
    Future {
      var apiUrl: Option[String] = Option(imageUrls.url)
      var imagesList: Seq[MediaInfoTrestleSpecific] = Seq.empty[MediaInfoTrestleSpecific]
      try {
        while (apiUrl.isDefined) {
          DataDogLogging(
            mdpSourceName = source,
            dataDestination = "downloader component",
            action = s"Hit Download API request",
            reason = s"Hit API for photo download",
            rdmSourceSystemKey = source,
            mdpJobId = processId,
            downloadType = "incrphoto",
            eventTimestamp = startTime,
            timestamp = startTime.toEpochMilli
          )
          val mediaPage =
            Await.result(
              WebApiUtils.downloadPage(ahc, apiUrl.get, initialRequestToken, logger, useHeader, headerKeyValue),
              Duration.Inf
            )
          val images = mapper.readValue(mediaPage.toString, classOf[ImagesMetaDataTrestleSpecific])
          imagesList = imagesList ++ images.value
          apiUrl = (mediaPage \ "@odata.nextLink").asOpt[String]
        }
        if (!includeFloorPlan) {
          imagesList = imagesList
            .filter(media => Option(media.MediaCategory).isEmpty || media.MediaCategory.equals("Photo"))
            .sortBy(_.Order)
        } else {
          imagesList = imagesList
            .filter(media =>
              Option(media.MediaCategory).isEmpty || media.MediaCategory.equals("Photo") || media.MediaCategory
                .equals("FloorPlan")
            )
            .sortBy(_.Order)
        }
        val adjustOrder = imagesList.head.Order == 1
        if (imagesList.nonEmpty) {
          var failureCount: Int = 0
          var customOrder = 0
          imagesList.foreach { imageUrl =>
            val mediaUrlToUse = DownloadStrategyFactory.checkMissingProtocolInImageURL(imageUrl.MediaURL)
            try {
              val (imageBytes: Array[Byte], contentType: String) = {
                DataDogLogging(
                  mdpSourceName = source,
                  dataDestination = "downloader component",
                  action = s"Download API request",
                  reason = s"Hit API for photo download",
                  rdmSourceSystemKey = source,
                  mdpJobId = processId,
                  downloadType = "incrphoto",
                  eventTimestamp = startTime,
                  timestamp = startTime.toEpochMilli
                )
                val imageAndContentType = PhotoUtils.getPhotoWithRedirect(mediaUrlToUse, ahc, 1)
                (imageAndContentType._2, imageAndContentType._1)
              }
              // don't hit photo urls too fast, will cause exceeding quota issues.
              Thread.sleep(waitTimeInMilliSecBetweenWebAPICalls)
              try {
                downloadAssistant.submitPhoto(
                  listingNum,
                  PhotoUtils.fileExtesionFromMime(contentType),
                  imageBytes,
                  if (!useCustomOrder) {
                    if (adjustOrder) imageUrl.Order - 1 - failureCount else imageUrl.Order - failureCount
                  } else { customOrder },
                  null,
                  if (imageUrl.LongDescription != null && imageUrl.LongDescription.equals("Virtual renderings"))
                    imageUrl.LongDescription
                  else ""
                )
                customOrder += 1
              } catch {
                case _: IllegalStateException =>
                  logger.error(
                    s"Failed, photo processing for listingNum $listingNum, image# ${imageUrl.Order}, ${mediaUrlToUse}  skipped due to unsupported format."
                  )
                  logger.error(s"Resp as json -> ${Json.parse(imageBytes)}")
              }
            } catch {
              case ex: Exception =>
                failureCount += 1
                logger.error(
                  s"Failed, photo processing for listingNum $listingNum, image# ${imageUrl.Order}, ${mediaUrlToUse} skipped due to error $ex"
                )
            }
          }
          logger.info(s"Photo download complete for $listingNum, failures : " + failureCount)
          downloadAssistant.reportProcessingListingDone(listingNum, true)
        } else {
          logger.warn("No images to process for listing : " + listingNum)
          downloadAssistant.reportProcessingListingDone(listingNum, true)
        }
      } catch {
        case e: DownloadPageFailure =>
          e.printStackTrace()
          logger.error(
            "Error downloading image metadata page(s) for : " + listingNum + ", marking as a failure. Message : " + e.getMessage
          )
          downloadAssistant.reportProcessingListingDone(listingNum, false)
        case e: Exception =>
          e.printStackTrace()
          logger.error(
            "An unknown exception occurred while downloading image metadata for : " + listingNum + ", marking as a failure. Message : " + e.getMessage
          )
          downloadAssistant.reportProcessingListingDone(listingNum, false)
      }
    }(threadPool)
  }
}
