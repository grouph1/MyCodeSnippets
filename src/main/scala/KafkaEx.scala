import  org.apache.spark.sql.SparkSession
import  org.apache.spark.streaming._
import  org.apache.spark.streaming.kafka.KafkaUtils
import _root_.kafka.serializer.DefaultDecoder
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
object KafkaEx {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
    .getOrCreate()

   /* // create streaming context with a 30-second interval - adjust as required
    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(30))

    // this uses Kafka080 client. Kafka010 has some subscription differences

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> kafkaBootstrapServer,
      "group.id" -> "job-group-id",
      "auto.offset.reset" -> "largest",
      "enable.auto.commit" -> (false: java.lang.Boolean).toString
    )

    // create a kafka direct stream
    val topics = Set("topic")
    val stream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext, kafkaParams, topics)

    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      streamingContext,
      kafkaParams,
      Map("spark-test-topic" -> 1),   // subscripe to topic and partition 1
      StorageLevel.MEMORY_ONLY
    )
*/

  }
}
