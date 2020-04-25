package com.beamsuntory.amz.de.test

import com.beamsuntory.amz.de.commons.UtilsTest
import org.scalatest._

class FeatureBasicTest extends FunSuite with GivenWhenThen with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  var utils: Option[UtilsTest] = None: Option[UtilsTest]
  val NULL_VALUE:Null = null


  override def beforeEach() : Unit = {
    super.beforeEach()
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    sys.props("testing") = "true"
    sys.props("nousehive") = "true"
    System.setProperty("spark.master", "local")
    utils = Option(new UtilsTest)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    utils.get.spark.sparkContext.stop()
  }

}
