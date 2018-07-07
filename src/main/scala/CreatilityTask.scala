import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object CreatilityTask {

  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
  val sc = spark.sparkContext
  def main(args: Array[String]){

    val custschema= new StructType(
      Array(
        (new StructField("Time",DataTypes.LongType, true)),
        (new StructField("Value",DataTypes.DoubleType,true))
      )
    )

    var df =spark.read.option("sep","\t").
      schema(custschema).
      csv("C:\\tmp\\data.txt")
    df.show()
    df= df.withColumn("T1",to_timestamp(from_unixtime(col("Time"))))

    df.printSchema()
    df.show(5)


 df.groupBy(col("T1"),window(col("T1")isNotNull,"1 minute")).sum("Value").show()



  }
}
