package com.beamsuntory.amz.de.reporting.keepa

import com.beamsuntory.amz.de.commons.UtilsAmzDE

class ReportingKeepaService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  case class Caller[T>:Null<:AnyRef](klass:T) {
    def call(methodName:String,args:AnyRef*):AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass,args: _*)
    }
  }

  def parallelExecution(): Unit = {

    val functions = Seq("reportingKeepa")
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
    reportingKeepa()

  }
  def reportingKeepa(): Unit = new ReportingKeepa(utils).mainExecution()

}
