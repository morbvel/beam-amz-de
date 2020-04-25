package com.beamsuntory.amz.de.functional.ams

import com.beamsuntory.amz.de.commons.UtilsAmzDE

class FunctionalAmsService(val utils: UtilsAmzDE) {

  implicit def anyRef2callable[T>:Null<:AnyRef](klass:T):Caller[T] = Caller(klass)

  case class Caller[T>:Null<:AnyRef](klass:T) {
    def call(methodName:String,args:AnyRef*):AnyRef = {
      def argTypes = args.map(_.getClass)
      def method = klass.getClass.getMethod(methodName, argTypes: _*)
      method.invoke(klass,args: _*)
    }
  }

  def parallelExecution(): Unit = {

    val functions = Seq("funtionalEdwAms", "functionalRadar", "functionalCreateScenarios")
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
    funtionalEdwAms()
    functionalRadar()
    functionalCreateScenarios()
  }
  
  def funtionalEdwAms(): Unit = new FunctionalAmsEdw(utils).mainExecution()
  def functionalRadar(): Unit =  new FunctionalRadarAms(utils).mainExecution()
  def functionalCreateScenarios(): Unit = new FunctionalScenariosAms(utils).mainExecution()

}
