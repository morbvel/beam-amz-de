package com.beamsuntory.amz.de.harmonization.arap

import com.beamsuntory.amz.de.commons.UtilsAmzDE

class HarmonizationArapService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T >: Null <: AnyRef](klass: T): Caller[T] = Caller(klass)

  case class Caller[T >: Null <: AnyRef](klass: T) {
    def call(methodName: String, args: AnyRef*): AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass, args: _*)
    }
  }

  def parallelExecution(): Unit = {

    val functions = Seq("harmonizationCleanSalesOrdered", "harmonizationCleanSalesShipped", "harmonizationCleanAlternatePurchase",
      "harmonizationCleanAmzTerms", "harmonizationCleanCustomerReviews",
      "harmonizationCleanForecastInventoryShipped", "harmonizationCleanForecastInventoryOrdered",
      "harmonizationCleanGeographicSales", "harmonizationCleanInventoryHealth", "harmonizationCleanItemComparison",
      "harmonizationCleanMarketBastketAnalysis", "harmonizationCleanSalesShippedSourcing")
    functions.par.foreach(function =>
      this call function
    )
  }

  def linealExecuction(): Unit = {
    harmonizationSalesDiagnosticUnion()
    //harmonizationEdw()
  }

  def harmonizationArap(): Unit = new HarmonizationArap(utils).mainExecution

  def harmonizationEdw(): Unit = new HarmonizationEdw(utils).mainExecution

  def harmonizationSalesDiagnosticUnion(): Unit = new HarmonizationArap(utils).harmonizationSalesDiagnosticUnion()

  def harmonizationCleanSalesOrdered(): Unit = new HarmonizationArap(utils).harmonizationCleanSalesOrdered()

  def harmonizationCleanSalesShipped(): Unit = new HarmonizationArap(utils).harmonizationCleanSalesShipped()

  def harmonizationCleanAlternatePurchase(): Unit = new HarmonizationArap(utils).harmonizationCleanAlternatePurchase()

  def harmonizationCleanAmzTerms(): Unit = new HarmonizationArap(utils).harmonizationCleanAmzTerms()

  def harmonizationCleanCustomerReviews(): Unit = new HarmonizationArap(utils).harmonizationCleanCustomerReviews()

  def harmonizationCleanForecastInventoryShipped(): Unit = new HarmonizationArap(utils).harmonizationCleanForecastInventoryShipped()

  def harmonizationCleanForecastInventoryOrdered(): Unit = new HarmonizationArap(utils).harmonizationCleanForecastInventoryOrdered()

  def harmonizationCleanGeographicSales(): Unit = new HarmonizationArap(utils).harmonizationCleanGeographicSales()

  def harmonizationCleanInventoryHealth(): Unit = new HarmonizationArap(utils).harmonizationCleanInventoryHealth()

  def harmonizationCleanItemComparison(): Unit = new HarmonizationArap(utils).harmonizationCleanItemComparison()

  def harmonizationCleanSalesShippedSourcing(): Unit = new HarmonizationArap(utils).harmonizationCleanSalesShippedSourcing()

  def harmonizationCleanMarketBastketAnalysis(): Unit = new HarmonizationArap(utils).harmonizationCleanMarketBastketAnalysis()


}
