import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TrestleStrategyTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "TrestleStrategy" should "initialize variables with mocked DaoLoader and postgresDao" in {
    // Mock DaoLoader and PostgresDownloaderDao
    val mockPostgresDao = mock[PostgresDownloaderDao]
    when(mockPostgresDao.getMdpProperty(ArgumentMatchers.eq("use.trestle.photo.customorder.field"), ArgumentMatchers.any[String]))
      .thenReturn("true")
    when(mockPostgresDao.getMdpProperty(ArgumentMatchers.eq("include.floor.plan.photo.type"), ArgumentMatchers.any[String]))
      .thenReturn("false")

    // Mock DaoLoader's behavior
    val mockDaoLoader = mock[DaoLoader.type]
    when(mockDaoLoader.loadDao(ArgumentMatchers.any[Config], ArgumentMatchers.eq("postgres")))
      .thenReturn(mockPostgresDao)

    // Use mockDaoLoader to create a TrestleStrategy instance
    val strategy = new TrestleStrategy {
      override val postgresDao: PostgresDownloaderDao = mockDaoLoader.loadDao(config, "postgres").asInstanceOf[PostgresDownloaderDao]
    }

    // Assertions
    strategy.useCustomOrder shouldEqual true
    strategy.includeFloorPlan shouldEqual false

    // Verify interactions
    verify(mockPostgresDao).getMdpProperty("use.trestle.photo.customorder.field", ArgumentMatchers.any[String])
    verify(mockPostgresDao).getMdpProperty("include.floor.plan.photo.type", ArgumentMatchers.any[String])
  }
}
