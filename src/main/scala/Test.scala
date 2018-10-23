package main.scala

import java.io.File
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import scala.language.implicitConversions


object Test {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("Task")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkSession")
      .getOrCreate()

    val schema = StructType(Array(
      StructField("EmployeeID", StringType, true),
      StructField("City", StringType, true),
      StructField("Country", StringType, true),
      StructField("PrimarySkill", StringType, true)
    ))

    def getListOfFiles(dir: String): List[String] = {
      val d = new File(dir)
      d.listFiles.filter(_.isFile).sortBy(_.lastModified()).map(_.getName).filter(x => (x.contains("UPD")) || (x.contains("DEL"))).toList
    }

    def Update(f: (String) => List[String], x: String): Unit = {

      val sourceFile = sc.textFile("C:\\files\\10001_20180101_SOURCE_FILE1.txt")
      val rowsRDD = sourceFile.map(x => {
        val m = x.split(";");
        if (m.length == 4) {
          Row(m(0), m(1), m(2), m(3))
        }
        else Row(" ", " ", " ", " ")
      })
      val dfSource = spark.createDataFrame(rowsRDD, schema)

      val result = f(x)
      for (i <- result) {
        val UPDFile = sc.textFile("C:\\files" + i)
        //UPDFile.foreach(println)
        val rowsUPD = UPDFile.map(x => {
          val m = x.split(";");
          if (m.length == 4) {
            Row(m(0), m(1), m(2), m(3))
          }
          else Row(" ", " ", " ", " ")
        })
        val df = spark.createDataFrame(rowsUPD, schema)
        //df.show()

        if (i.contains("UPD")) {
          println("-------------------------UPDATE records-------------------------")
          dfSource.show()
          val merge = df.union(dfSource.join(df, Seq("EmployeeID"), joinType = "left_anti").orderBy("EmployeeID"))
          //merge.write.mode("append").insertInto("dfSource")
         // dfSource.write.mode(SaveMode.Append)
          merge.show()
        }
        else {

          println("---------------------DELETE records---------------------")
          val DEL = dfSource.join(df, Seq("EmployeeID"), joinType = "left_anti")
          DEL.show()
        }
        dfSource.show()
      }


    }
    //getListOfFiles("C:/Users/galena.teneva/Desktop/Tasks/Tasks2")
    Update(getListOfFiles, "C:\\files")

  }
}