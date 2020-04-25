package com.beamsuntory.amz.de.reporting.ams

import com.beamsuntory.amz.de.commons.UtilsAmzDE

class ReportingAmsService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  case class Caller[T>:Null<:AnyRef](klass:T) {
    def call(methodName:String,args:AnyRef*):AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass,args: _*)
    }
  }

  def parallelExecution(): Unit = {

    val functions = Seq("reportingFinalAms")
    functions.par.foreach(function =>
      this call function
    )

  }

  def execution(option: String): Unit = {

    option match {
      case "p" => parallelExecution()
      case _ => linealExecuction()
    }

  }

  def linealExecuction(): Unit = {
    reportingFinalAms()

  }
  def reportingFinalAms(): Unit = new ReportingAms(utils).mainExecution()

}
