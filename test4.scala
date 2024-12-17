package com.zaplabs.webapi.downloader.strategy

import com.zaplabs.utils.PhotoUtils
import com.zaplabs.webapi.dto.{ImagesMetaDataTrestleSpecific, MediaInfoTrestleSpecific}
import com.zaplabs.webapi.WebApiUtils
import com.zaplabs.webapi.WebApiUtils.DownloadPageFailure
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar
import org.asynchttpclient.AsyncHttpClient
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import java.util.concurrent.Executors
import play.api.libs.json.Json

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
    Seq(
      MediaInfoTrestleSpecific(MediaURL = "https://mockimage1.com", MediaCategory = "Photo", Order = 1, LongDescription = "Sample Image 1"),
      MediaInfoTrestleSpecific(MediaURL = "https://mockimage2.com", MediaCategory = "Photo", Order = 2, LongDescription = "Sample Image 2")
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
    when(WebApiUtils.downloadPage(any[AsyncHttpClient], any[String], any[String], any, any[Boolean], any[Map[String, String]]))
      .thenReturn(Future.successful(testPageResponse))

    // Mock PhotoUtils to simulate successful photo retrieval
    when(PhotoUtils.getPhotoWithRedirect(any[String], any[AsyncHttpClient], any[Int]))
      .thenReturn(("image/jpeg", Array[Byte](1, 2, 3, 4)))

    // Create the instance of TrestleStrategy
    val trestleStrategy = new TrestleStrategy(
      downloadAssistant = mockDownloadAssistant,
      threadPool = mockExecutionContext,
      source = "testSource",
      processId = "testProcessId",
      waitTimeInMilliSecBetweenWebAPICalls = 10,
      outputSubPath = "2024-06-17",
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
      verify(mockDownloadAssistant, times(2)).submitPhoto(
        eqTo(testListingId),
        any[String],
        any[Array[Byte]],
        any[Int],
        any[Null],
        any[String]
      )
    }
  }

  it should "handle DownloadPageFailure and mark processing as failed" in {
    // Arrange: mock WebApiUtils to throw a DownloadPageFailure
    when(WebApiUtils.downloadPage(any[AsyncHttpClient], any[String], any[String], any, any[Boolean], any[Map[String, String]]))
      .thenReturn(Future.failed(new DownloadPageFailure("Failed to download page")))

    val trestleStrategy = new TrestleStrategy(
      downloadAssistant = mockDownloadAssistant,
      threadPool = mockExecutionContext,
      source = "testSource",
      processId = "testProcessId",
      waitTimeInMilliSecBetweenWebAPICalls = 10,
      outputSubPath = "2024-06-17",
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
      verify(mockDownloadAssistant).reportProcessingListingDone(testListingId, success = false)
    }
  }
}
