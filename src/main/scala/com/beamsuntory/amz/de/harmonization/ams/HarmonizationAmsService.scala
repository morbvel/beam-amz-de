package com.beamsuntory.amz.de.harmonization.ams

import com.beamsuntory.amz.de.commons.UtilsAmzDE

class HarmonizationAmsService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  case class Caller[T >: Null <: AnyRef](klass: T) {
    def call(methodName: String, args: AnyRef*): AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass, args: _*)
    }
  }

  def execution(option: String): Unit = {

    //new FunctionalSalesforce(utils)
    //new FunctionalZylem(utils)
    //new FunctionalEdw(utils)

    option match {
      case "p" => parallelExecution()
      case _ => linealExecuction()
    }

  }

  def linealExecuction(): Unit = {
    harmonizationAms()

  }

  def parallelExecution(): Unit = {

    val functions = Seq("shipmentsExecution", "productExecution")
    functions.par.foreach(function =>
      this call function
    )

  }

  def harmonizationAms(): Unit = new HarmonizationAms(utils).mainExecution()

}
