
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

trait Print{
  def print1()
}

abstract class  PrintA4{
  val a4="a4"
  def printA4()
}

class  A6 extends PrintA4
  {
    val logger = Logger.getLogger(classOf[A6])
    //override val a4="newa4"
  def print1(): Unit ={
    println("trait print method")
    logger.info("Testing log4j -debug messages")
    println(a4)
  }

  def printA4(): Unit = {
    println("abstract class print ")
    println(a4)
  }
}
object MainObject  {

  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
   def main(args: Array[String]): Unit = {

    var a = new A6() with Print
    var count=9
    var x=count
    a.print1();
    a.printA4()

    /*val df =spark.
            read.
            option("header","true").
            option("inferSchema","true").
            csv("file:///C://Users//Himakshi.Bhagtani.ADASTRACORPNET//Documents//CoBA BDAA//Temp/sample.txt")*/
    //df.show(false)
  }


}
