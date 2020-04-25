package com.beamsuntory.amz.de.functional.keepa

import com.beamsuntory.amz.de.commons.UtilsAmzDE
import com.beamsuntory.amz.de.harmonization.keepa.HarmonizationHierarchy

class FunctionalKeepaService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  case class Caller[T>:Null<:AnyRef](klass:T) {
    def call(methodName:String,args:AnyRef*):AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass,args: _*)
    }
  }

  def parallelExecution(): Unit = {

    val functions = Seq("funtionalKeepaEdw", "functionalGeographicKeepa")
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
    funtionalKeepaEdw()
    functionalGeographicKeepa()
  }
  
  def funtionalKeepaEdw(): Unit = new FunctionalKeepaEdw(utils).mainExecution()
  def functionalGeographicKeepa(): Unit = new FunctionalGeographicKeepa(utils).mainExecution()


}
