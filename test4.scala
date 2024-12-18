TrestleStrategyTest.scala:93:8
value thenReturn is not a member of Nothing
possible cause: maybe a semicolon is missing before `value thenReturn'?
      .thenReturn(Future.successful(testPageResponse.as[JsObject]))

package com.zaplabs.webapi.downloader.strategy

import com.typesafe.scalalogging.Logger
import com.zaplabs.WebApiPhotoUrl
import com.zaplabs.utils.PhotoUtils
import com.zaplabs.webapi.WebApiUtils
import com.zaplabs.webapi.downloader.startegy.TrestleStrategy
import com.zaplabs.webapi.dto.{ImagesMetaDataTrestleSpecific, MediaInfoTrestleSpecific}
import org.asynchttpclient.AsyncHttpClient
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.json.{JsObject, Json}

import java.sql.Timestamp
import java.util.concurrent.Executors
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}

class TrestleStrategyTest extends AsyncFlatSpec with Matchers with MockitoSugar with ScalaFutures {

  // Mock dependencies
  val mockDownloadAssistant = mock[PhotoUtils.DownloadProcessAssistant]
  val mockAsyncHttpClient = mock[AsyncHttpClient]
  val mockExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

  // Sample data
  val testListingId = "12345"
  val testUrl = "https://mockapi.com/images"
  val testPhotoUrl = WebApiPhotoUrl(testListingId, testUrl)

  val testImagesMetadata = ImagesMetaDataTrestleSpecific(
    List(
      MediaInfoTrestleSpecific(MediaURL = "https://mockimage1.com", MediaCategory = "Photo", Order = 1, LongDescription = "Sample Image 1", MediaModificationTimestamp = new Timestamp(System.currentTimeMillis()),ResourceRecordID = "resource_1"),
      MediaInfoTrestleSpecific(MediaURL = "https://mockimage2.com", MediaCategory = "Photo", Order = 2, LongDescription = "Sample Image 2", MediaModificationTimestamp = new Timestamp(System.currentTimeMillis()),ResourceRecordID = "resource_1")
    )
  )

  // Mock WebApiUtils behavior
  val testPageResponse = Json.parse(
    """
      |{
      |  "value": [
      |    {"MediaURL": "https://mockimage1.com", "MediaCategory": "Photo", "Order": 1, "LongDescription": "Sample Image 1"},
      |    {"MediaURL": "https://mockimage2.com", "MediaCategory": "Photo", "Order": 2, "LongDescription": "Sample Image 2"}
      |  ],
      |  "@odata.nextLink": null
      |}
      |""".stripMargin
  )

  behavior of "TrestleStrategy"

  it should "process images successfully and submit them to downloadAssistant" in {
    // Arrange: mock WebApiUtils to return the test JSON response
//    when(WebApiUtils.downloadPage(any[AsyncHttpClient], any[String], any[String], any, any[Boolean], any[Map[String, String]]))
//      .thenReturn(Future.successful(testPageResponse))

    // Mock PhotoUtils to simulate successful photo retrieval


    // Mock PhotoUtils to simulate successful photo retrieval
//    when(PhotoUtils.getPhotoWithRedirect(any[String], any[AsyncHttpClient], any[Int]))
//      .thenReturn(("image/jpeg", Array[Byte](1, 2, 3, 4)))


    // Mock WebApiUtils behavior
    // Mock WebApiUtils behavior
    val testPageResponse = Json.parse(
      """|{
         |  "@odata.context" : "http://retsapi.raprets.com/LUBB/RESO/OData/$metadata#Media",
         |  "value" : [ {
         |    "MediaKey" : "04774438-58c3-40b5-bf5a-d22a57b38bcc.jpg",
         |    "ResourceRecordKeyNumeric" : 199720,
         |    "MediaModificationTimestamp" : "2024-10-24T10:27:55-05:00",
         |    "ResourceRecordID" : "202414905",
         |    "LongDescription" : "",
         |    "MediaType" : "jpeg",
         |    "ClassName" : "Residential",
         |    "Order" : 21,
         |    "MediaURL" : "https://mockimage1.com/8bcc.jpg",
         |    "Permission" : "Public",
         |    "ResourceRecordKey" : "199720"
         |  }]}""".stripMargin)

    when(WebApiUtils.downloadPage(ArgumentMatchers.any[AsyncHttpClient],
      ArgumentMatchers.any[String], ArgumentMatchers.any[String],
      ArgumentMatchers.any[Logger], ArgumentMatchers.any[Boolean],
      ArgumentMatchers.any[Map[String, String]], ArgumentMatchers.any[FiniteDuration]),
      ArgumentMatchers.any[ExecutionContext])
      .thenReturn(Future.successful(testPageResponse.as[JsObject]))

    // Create the instance of TrestleStrategy
    val trestleStrategy = new TrestleStrategy(
      downloadAssistant = mockDownloadAssistant,
      threadPool = mockExecutionContext,
      source = "testSource",
      processId = "testProcessId",
      waitTimeInMilliSecBetweenWebAPICalls = 10,
      outputSubPath = "20240617_053806",
      outputLogPath = "/logs"
    )

    // Act
    val result = trestleStrategy.processImages(
      imageUrls = testPhotoUrl,
      initialRequestToken = "testToken",
      useHeader = false,
      headerKeyValue = Map.empty,
      ahc = mockAsyncHttpClient
    )

    // Assert
    result.map { _ =>
      succeed
    }
  }


}
