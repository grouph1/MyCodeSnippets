import java.io.{File, FileReader}
import javax.mail.Provider.Type
import javax.xml.parsers.SAXParserFactory
import javax.xml.transform.stream.StreamSource
import javax.xml.validation.SchemaFactory

import XmlTest.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.xml.Elem
import scala.Seq
import com.databricks.spark.avro.SchemaConverters
import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.avro.Schema._
import org.apache.avro.TestProtocolReflect.TestRecord
import org.apache.avro.generic.GenericData

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._



object XmlTest {

  val spark = SparkSession.builder().appName("Test").master("local").getOrCreate()
  val sc = spark.sparkContext
  import spark.sqlContext.implicits._

 val t= List("c1","c2","c3")

  val dfs=Seq(("col1","col2","col3"),("col1","col2","col3"))
  dfs.toDF(t:_*).show()
  import scala.xml.XML
  import java.io.BufferedReader
  def main(args: Array[String]): Unit = {
    val avroSchema= "{" + "\"type\":\"record\"," + "\"name\":\"myrecord\"," + "\"fields\":[" + "  { \"name\":\"str1\", \"type\":\"string\" }," + "  { \"name\":\"str2\", \"type\":\"string\" }," + "  { \"name\":\"int1\", \"type\":\"int\" }" + "]}"

    println(avroSchema)

    val schema = new Schema.Parser().parse(avroSchema)
    import org.apache.avro.generic.GenericRecord
    val record1 = new GenericData.Record(schema)
    record1.put("str1", "str1val1")
    record1.put("str2", "str2val1")
    record1.put("int1", 1)

    val record = new GenericData.Record(schema)
    record.put("str1", "str1val2")
    record.put("str2", "str2val2")
    record.put("int1", 5)

    val rdd= sc.parallelize(Seq(record1, record))
    val ex="rec.get(\"str1\").toString,rec.get(\"str2\").toString,rec.get(\"int1\").toString)"
    val result= rdd.map(rec=> ex)


result.toDF()



    val sqlType=SchemaConverters.toSqlType(schema)
    //val obj :Object =4
var qry= ""
    val fields= schema.getFields

    fields.foreach(field =>{
      qry+="rec.get(\""+field.name()+"\").toString,"
    })

    qry=qry.substring(0,qry.length-1)

    println(qry)
    /*fields.foreach(field =>{
     println("name "+field.name())
      println("type "+field.schema().getType)
      field.schema().getType match {
        case Schema.Type.STRING => val x= record.get( field.name()).toString
          println("Converted string value" + x)
        case Schema.Type.BYTES =>
        case Schema.Type.INT => val x= record.get( field.name()).asInstanceOf[IntegerType]
          println("Converted int value" + x)
        case Schema.Type.LONG =>
        case Schema.Type.FLOAT =>
        case Schema.Type.DOUBLE =>
        case Schema.Type.BOOLEAN =>
        case Schema.Type.NULL =>
        case _ =>
      }


    })*/





    /*val df = spark.read.
      format("com.databricks.spark.xml")
      .option("rowTag","LogEntry")
      .load("C:\\tmp\\logentries_2018-03-22.xml")

    df.show(false)
    df.select("Application","TMADetails.AssessmentByCriterion.CustomerCriterion.CategoryName").show(false)

    df.select(df.col("Application"),df.col("CoRIAID"), explode(df.col("TMADetails.AssessmentByCriterion"))
      .as("TMADetails_AssessmentByCriterion"))
        .select("Application","CoRIAID","TMADetails_AssessmentByCriterion")
      .show(false)

    df.select(df.col("Application"),df.col("CoRIAID"), explode(df.col("TMADetails.AssessmentByCriterion"))
      .as("TMADetails_AssessmentByCriterion"))
      .select("Application","CoRIAID","TMADetails_AssessmentByCriterion.CustomerCriterion.CategoryName",
        "TMADetails_AssessmentByCriterion.CustomerCriterion.CriterionName",
        "TMADetails_AssessmentByCriterion.ProductCriterion.CategoryName",
        "TMADetails_AssessmentByCriterion.ProductCriterion.CriterionName",
        "TMADetails_AssessmentByCriterion.Result")
      .show(false)

    val fields = Map("app" -> "Application",
      "coria" -> "CoRIAID",
      "AssessmentProductCategory" -> "TMADetails_AssessmentByCriterion.ProductCriterion.CategoryName",
      "AssessmentProductCriterion" -> "TMADetails_AssessmentByCriterion.ProductCriterion.CriterionName",
      "AssessmentProductResult" -> "TMADetails_AssessmentByCriterion.Result"
    )

    // Transform fields list into list of cols that can be used for .select
    val colNames = fields.map(x => col(x._2).alias(x._1)) toList


    import org.apache.spark.sql.types._

    def findArrayTypes(parents:Seq[String],f:StructField) : Seq[String] = {
      f.dataType match {
        case array: ArrayType => println("arry "+f.name)
          parents
        case struct: StructType => println("strct "+f.name)
          struct.fields.toSeq.map(f => findArrayTypes(parents:+f.name,f)).flatten
        case _ => println("def "+f.name)
          Seq.empty[String]
      }
    }
var qry=new java.lang.String()
    def explodeExpr(parents:Seq[String],f:StructField) : java.lang.String= {
      f.dataType match {
        case array: ArrayType => println("arry "+f.name)
          qry+= "explode(df.col(\""+ f.name + "\")),"
          qry
        case struct: StructType => println("strct "+f.name)
          struct.fields.toSeq.map(f => explodeExpr(parents:+f.name,f))
          qry
        case _ => println("def "+f.name)
        qry+= "df.col(\""+ f.name + "\"),"
          qry
      }
    }


   /* val arrayTypeColumns = df.schema.fields.toSeq
      .map(f => findArrayTypes(Seq(f.name),f))
      .filter(_.nonEmpty).map(_.mkString("."))
    import scala.io.Source*/


    val e = df.schema.fields.toSeq
      .map(f => explodeExpr(Seq(f.name),f))

    println("final op "+qry.substring(0,qry.length-1))

    df.schema.fields.foreach(println)

    val q="\"Application\", \"CoRIAID\", \"explode(df.col(\"TMADetails.AssessmentByCriterion\"))\""


    qry=qry.substring(0,qry.length-1)
    df.select(qry).show()
  /*  val source1 = Source.fromFile("C:\\tmp\\twoXmls.xml")
    for (line <- source1.getLines())
    {
      println(line)
    }


    /*var someDF = Seq(
      (8, "bat"),
      (64, "mouse"),
      (-27, "horse")
    )
    import spark.implicits._
    someDF= someDF:+ (10, "test")
    someDF.toDF().show()*/

    var sb=new StringBuffer();
    val source = Source.fromFile("C:\\tmp\\twoXmls.xml")
    for (line <- source.getLines())
     {
       if(line.contains("?xml version=\"1.0\"")) {
         println("sb content " +sb.toString)
         if (sb.length() > 0) {
           println("sb.length" +sb.length())
           import spark.implicits._
           var testxml = XML.loadString(sb.toString)
           var mySeq = Seq(((testxml\\"user"\\"id").text, sb.toString))
           val df= mySeq.toDF()
           df.printSchema()
           df.write.csv("C:\\tmp\\test6")

         }
         sb = new StringBuffer();
       }

    sb.append(line+"\n")
     }
    println("sb content " +sb.toString)
    var testxml = XML.loadString(sb.toString)
    println((testxml\\"user"\\"id").text)
    source.close*/
*/



  }
}
