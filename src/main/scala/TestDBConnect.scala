import java.io.FileInputStream
import java.io.File
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.tools.fusesource_embedded.jansi.AnsiConsole

object TestDBConnect {



  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
  val sc = spark.sparkContext

  def main(args: Array[String]):Unit={

   //Class.forName("oracle.jdbc.driver.OracleDriver")
    /*val user="SYSTEM"
    val pwd ="admin"*/

    val connectProp = new Properties()
   /* connectProp.put("user","SYSTEM")
    connectProp.put("password","admin")
    connectProp.put("driver","oracle.jdbc.driver.OracleDriver")
*/

    //driver=oracle.jdbc.driver.OracleDriver
    val inputStream = new FileInputStream(new File("C:\\tmp\\src.conf"))
    connectProp.load(inputStream)

    val dburl="jdbc:oracle:thin:@localhost:1521/xe"
    val uri=connectProp.getProperty("url")
    println("url" +uri)
    val pushdown_query = "(select * from newemployee) emp"
    val df= spark.read.jdbc(dburl,pushdown_query ,connectProp)

    df.show(false)

  }

}
