import org.apache.spark.sql.SparkSession
import java.io.File

object RemoteHiveConnTest {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath




    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
     .config("hive.metastore.uris", "thrift://192.168.59.32:9083")
    //  .config("fs.defaultFS", "hdfs://192.168.59.32:8022")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .master("local").appName("HiveConnTest").enableHiveSupport().getOrCreate()

    import org.apache.spark.SparkContext
    val context = spark.sparkContext.hadoopConfiguration


    spark.sql("show databases").show()

    //spark.sql("use demo_az")

    spark.sql("show tables").show()

    spark.sql("select * from poc_vw.employee").show()

  }
}
