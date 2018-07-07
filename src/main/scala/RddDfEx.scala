import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.types._

object RddDfEx {

  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
  val sc = spark.sparkContext
  def main(args: Array[String]){

    val oneSpace="   ";
    val noSpace="";

    println(StringUtils.isBlank(oneSpace))
    println(StringUtils.isBlank(noSpace))
   val custschema = new StructType(Array( StructField("a1",StringType),
      StructField("a2",StringType,true),
      StructField("a3",StringType,true),
      StructField("a4",StringType,true),
      StructField("a5",StringType,true),
      StructField("a6",StringType,true),
      StructField("a7",IntegerType,true),
     StructField("corruptRecord", StringType,true)))

   /* val df = spark.read
      .format("com.databricks.spark.csv")
        .schema(custschema)
      .option("delimiter",";")
      .option("mode","PERMISSIVE")
        //.option("badRecordsPath","C://tmp//badRec")
        .option("columnNameOfCorruptRecord", "corruptRecord")
      .option("header", "true")
      .option("escape", "|")
      .load("file:///C://tmp//file2.txt");
*/
   val df = spark.read
     .schema(custschema)
     .option("delimiter",";")
     .option("mode","PERMISSIVE")
     //.option("badRecordsPath","C://tmp//badRec")
     .option("columnNameOfCorruptRecord", "corruptRecord")
     .option("header", "true")
     .option("escape", "|")
       .option("ignoreLeadingWhiteSpace", true)
       .option("ignoreTrailingWhiteSpace", true)
     .csv("file:///C://tmp//file2.txt");

    df.show()

    val acc= sc.longAccumulator("bad lines")
    acc.add(2)
    acc.add(1)
    acc.add(3)
    println("acc id "+ acc.id)
    println("acc " + acc);

    println("acc value " + acc.count);

   /* val finalDF = df.filter(df.col("corruptRecord").isNull).drop("corruptRecord")
    finalDF.show

    val malformedRecords=df.filter(df.col("corruptRecord").isNotNull)

    malformedRecords.show()
    import spark.sqlContext.implicits

    //Seq("""{"a": 1, "b": 2}""", """{bad-record""").toDF().write.text("/tmp/input/jsonFile")
    val schema1 = new StructType(Array( StructField("a",IntegerType,true),
      StructField("b",IntegerType,true)))
    val df1 = spark.read
      .option("badRecordsPath", "C://tmp//badRec")
      .schema(schema1)
      .json("file:///C://tmp//myJson.txt")

    //df1.show()
   /* val newFile= spark.read.option("delimiter",";").option("escape", "%").option("schema", "schema").csv("file:///C://tmp//file2.txt")
    newFile.show(false)*/

  /*  val newFile= spark.read.csv("file:///C://tmp//test6//part-00000-75c198f2-cb63-4789-a45c-15c2a5b52912-c000.csv")
    newFile.show(false)
    val myxml =spark.read.text("file:///C:/tmp/h.xml")

    println("myxml type : " + myxml.getClass)
    println("my xml dtata " + myxml.dtypes)
    myxml.show(false)
    println(myxml.count())
    val x1= myxml.collect()

    println("printing x "+ x1)*/
    /*var data = sc.textFile("file:///C://tmp//file1.txt")
    val header = data.first() //extract header
      data = data.filter(row => row != header)   //filter out header
    data.foreach(println)*/

   /* var rdd = sc.textFile("file:///C://tmp//file1.txt")
    println("Number of partition: " +rdd.getNumPartitions)
    val i=2;
    rdd=rdd.mapPartitionsWithIndex { (idx, iter) => if (idx ==0) iter.drop(i) else iter }
    println("New way")
    rdd.foreach(println)


  val text= spark.read.format("csv").option("parserlib","univocity").option("delimiter",";").option("header","true").
    option("inferSchema", "true").load("file:///C://tmp//file2.txt")*/
  val text= spark.read.format("csv").option("delimiter", ";").load("file:///C://tmp//dec.txt")*/
   // text.write.format("text").save("file:///C://tmp//test")

    //text.show(false)
    //text.printSchema()

   //text.write.format("csv").option("delimiter",";").mode("append").save("C://tmp//test1")

   /* val df= text.selectExpr("_c0", "to_date(from_unixtime(unix_timestamp(_c1, 'MM.dd.yyyy'))) as dt", "_c4")
    df.printSchema()
    df.show(false)

    val df1 = df.select(df("_c0").cast(StringType),df("dt").cast(StringType),df("_c4"))
    df1.printSchema()

    val fillnull= df1.na.fill(" ")
    fillnull.show()
    fillnull.printSchema()
*/
   //text.write.format("com.databricks.spark.csv").option("quoteALL","true").save("C://tmp/test3")
    //fillnull.write.option("quoteMode","ALL").option("quote", "\"").csv("C://tmp/test2")

    // fillnull.map(_ => _.toString()).printSchema()
/*
   val text1=fillnull.
      selectExpr("rpad(_c0, 6, ' ')","rpad(_c1, 6, ' ')","rpad(_c2, 6, ' ')","rpad(_c3, 6, ' ')")

    text1.rdd.saveAsTextFile("file:///C://tmp//test")
    text1.show(false)
*/

      /*text.rdd.map(_.toString().replace("[","").replace("]", "").replace("," , "").replace("null" , " ")).
      saveAsTextFile("file:///C://tmp//test1")
*/
   // text.show()
 //val xmlFile= spark.read.format("com.databricks.spark.xml").option("rowTag","fremdeReferenz").load("file:///C:/tmp/trade_10_100.xml")

   /* val xmlFile= spark.read.format("com.databricks.spark.xml").option("rowTag","users").load("file:///C:/tmp/h.xml")

    xmlFile.show(false)
    println(xmlFile.describe())
    xmlFile.printSchema()*/
   /* val rdd = sc.parallelize(
      Seq(
        ("first", Array(2.0, 1.0, 2.1, 5.4)),
        ("test", Array(1.5, 0.5, 0.9, 3.7)),
        ("choose", Array(8.0, 2.9, 9.1, 2.5))
      )
    )

    val sqlContext = spark.sqlContext
import sqlContext.implicits._

    println(rdd.first())
    println(rdd.collect())
    val newrdd=rdd.+("NewStr")

    println(newrdd)
    println(rdd.count())
    val msg="New String"
    val rawData = List(msg)
    rawData.toDF()

    val parallelrdd = sc.parallelize(Array(0, 1, 2, 3, 4, 5, 6, 7, 8))


    println(parallelrdd.getStorageLevel)
    println(parallelrdd.min())

    println(parallelrdd.max())

    println(parallelrdd.take(5))

    println(parallelrdd.collect()(5))
    val rddop1=parallelrdd.filter(x=> x%2==0)

    println(rddop1.collect()(1))*/



  }
}
