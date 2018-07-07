import RddDfEx.spark
import org.apache.spark.sql.SparkSession
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DecimalType}

object avroCheck {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
    val sc = spark.sparkContext

    var newFile= spark.read.option("delimiter",";").
      option("header","true").csv("C:\\tmp\\dec.txt")
    newFile.show(false)
    newFile.printSchema()

    for(col <- newFile.columns)
      {
        if(col=="Value")
          {
            println("transforming decimal value")
            //newFile=newFile.withColumn(col, expr("cast(regexp_replace(newFile.col(col), \",\",\".\"))as DECIMAL(12,3)"))
            newFile=newFile.withColumn(col, regexp_replace(newFile.col(col), ",",".").cast(DecimalType(10,2)))
          }
      }
    newFile.printSchema()
    newFile.show(false)
   // newFile.write.format("com.databricks.spark.avro").save("C://tmp//avro1")

    /*import java.util.Base64
    val bytes = "Hello, /World\\another\\\\this is new".getBytes("UTF-8")
    val encoded = org.apache.commons.codec.binary.
      Base64.encodeBase64URLSafeString(("http://localhost:8080/db\\dclhst").getBytes())

    val dec = new String(org.apache.commons.codec.binary.
      Base64.decodeBase64(encoded))
    val decoded = Base64.getDecoder.decode(encoded)
    val value = new String(decoded)

    println("encoded "+ encoded)
    println("decoded "+ value)
    println("decoded value "+ dec)*/

  }



}

