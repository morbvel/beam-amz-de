import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.amz.de.functional.arap.FunctionalArapService
import com.beamsuntory.amz.de.harmonization.arap.HarmonizationArapService
import com.beamsuntory.amz.de.reporting.arap.ReportingArapService
import com.beamsuntory.amz.de.reporting.ams.ReportingAmsService
import com.beamsuntory.amz.de.functional.ams.FunctionalAmsService
import com.beamsuntory.amz.de.harmonization.ams.HarmonizationAmsService
import com.beamsuntory.amz.de.harmonization.reviews.HarmonizationReviewsService
import com.beamsuntory.amz.de.functional.reviews.FunctionalReviewsService
import com.beamsuntory.amz.de.reporting.reviews.ReportingReviewsService
import com.beamsuntory.amz.de.harmonization.keepa.HarmonizationKeepaService
import com.beamsuntory.amz.de.functional.keepa.FunctionalKeepaService
import com.beamsuntory.amz.de.reporting.keepa.ReportingKeepaService

import org.apache.log4j.{Level, Logger}

object AmzDEmain  {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    //Suppress Spark output
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val utils = new UtilsAmzDE

    args match {
      case Array("-h", "p_rev") =>
        new HarmonizationReviewsService(utils).parallelExecution()
      case Array("-h", "s_rev") =>
        new HarmonizationReviewsService(utils).linealExecuction()
      case Array("-h", "p") =>
        new HarmonizationArapService(utils).parallelExecution()
      case Array("-h", "s") =>
        new HarmonizationArapService(utils).linealExecuction()
      case Array("-h", "p_ams") =>
        new HarmonizationAmsService(utils).parallelExecution()
      case Array("-h", "s_ams") =>
        new HarmonizationAmsService(utils).linealExecuction()
      case Array("-h", "p_keepa") =>
        new HarmonizationKeepaService(utils).parallelExecution()
      case Array("-f" , "s") =>
        new FunctionalArapService(utils).linealExecuction()
      case Array("-f" , "p") =>
        new FunctionalArapService(utils).parallelExecution()
      case Array("-f" , "s_ams") =>
        new FunctionalAmsService(utils).linealExecuction()
      case Array("-f" , "p_keepa") =>
        new FunctionalKeepaService(utils).linealExecuction()
      case Array("-f" , "p_ams") =>
        new FunctionalAmsService(utils).parallelExecution()
      case Array("-f", "p_rev") =>
        new FunctionalReviewsService(utils).parallelExecution()
      case Array("-f", "s_rev") =>
        new FunctionalReviewsService(utils).linealExecuction()
      case Array("-r", "s") =>
        new ReportingArapService(utils).linealExecuction()
      case Array("-r", "p") =>
        new ReportingArapService(utils).parallelExecution()
      case Array("-r", "s_ams") =>
        new ReportingAmsService(utils).linealExecuction()
      case Array("-r", "p_ams") =>
        new ReportingAmsService(utils).parallelExecution()
      case Array("-r", "p_rev") =>
        new ReportingReviewsService(utils).parallelExecution()
      case Array("-r", "s_rev") =>
        new ReportingReviewsService(utils).linealExecuction()
      case Array("-r", "s_keepa") =>
        new ReportingKeepaService(utils).parallelExecution()
      case _ =>
        logger.error("ERROR: Invalid option.")
        logger.error("AmzDEMain$ Spark -h <p|s> -f <p|s> -r <p|s> (-h harmonization -f functional -r reporting p: Paralelize  s: Serial)")
        logger.error("AmzDEMain$ Spark p|s Arap p_ams|s_ams AMS p_keepa|s_keepa")
        System.exit(1)
    }

  }

}
