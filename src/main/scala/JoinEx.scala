import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object JoinEx {
  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
  val sc = spark.sparkContext
  def main(args: Array[String]): Unit = {


    val dfTime = spark.read.option("header", true)
      .csv("file:///C://tmp//times.txt.txt")

    dfTime.show()

    val pcrit = dfTime.selectExpr("from_unixtime(times/1000, 'yyyy-MM-dd') as dt").distinct()

    val xxx = dfTime.withColumn("dt", from_unixtime(col("times") / 1000, "yyyy-MM-dd"))
    xxx.show()

    pcrit.show()

    var l = pcrit.select("dt").collect()

    l.foreach(x => {
      xxx.filter(xxx("dt") === x.get(0)).show
    })


    import scala.collection.JavaConverters._

  }
}
