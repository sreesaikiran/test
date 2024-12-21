
class TrestleStrategy{
val postgresDao = DaoLoader.loadDao(config, "postgres").asInstanceOf[PostgresDownloaderDao]
  var useCustomOrder = postgresDao.getMdpProperty("use.trestle.photo.customorder.field", source).toBoolean
  var includeFloorPlan = postgresDao.getMdpProperty("include.floor.plan.photo.type", source).toBoolean
}

object DaoLoader {

  def loadDao(conf: Config, daoType: String): DownloaderDao = {
    daoType match {
      case "mongo" => new MongoDownloaderDaoImpl(conf)
      case "oracle" => new OracleDownloaderDaoImpl(conf)
      case "postgres" => new PostgresDownloaderDaoImpl(conf)
      case dao => throw new IllegalStateException(s"DAO type not recognized -> $dao. Exiting.")
    }
  }

}
