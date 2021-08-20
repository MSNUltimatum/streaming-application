import com.datastax.spark.connector.CassandraSparkExtensions

import java.sql.Timestamp
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

object SparkUtils {
  case class ReqData(ip: String, eventTime: String, eventType: String, url: String)

  case class EventDetails(ip: String, clickRate: Long, transitionsRate: Long, requestsCount: Long) {
    override def toString: String = s"{ip: $ip, clickRate: $clickRate, transitionsRate: $transitionsRate, requestsCount: $requestsCount}"
  }

  case class EventDetailsWithTS(ip: String, clickRate: Long, transitionsRate: Long, requestsCount: Long, time: String) {
    override def toString: String = s"{ip: $ip, clickRate: $clickRate, transitionsRate: $transitionsRate, requestsCount: $requestsCount, time: $time}"
  }

  val kafkaHost = "localhost:9092,localhost:9093,localhost:9094"
  val kafkaTopic = "requests-data"
  val batchInterval = 10

  val requestDetailsSchema: StructType = StructType(Array(
    StructField("ip", StringType),
    StructField("window", StringType),
    StructField("transitionsRate", IntegerType),
    StructField("clickRate", IntegerType),
    StructField("requestsCount", IntegerType),
  ))

  val clickToTransitions = 2L
  val requestsCount = 20
  val windowSize = 10
  val watermark = 10

  val redisTTL = 600
  val redisBotsDs = "bots"

  val cassandraKeySpace = "botdetector"
  val cassandraTable = "bots"
  val cassandraHost = "127.0.0.1"
  val cassandraPort = "9042"

  lazy val spark: SparkSession = SparkSession.builder()
    .appName("spark-streaming")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .config("spark.redis.host", "localhost")
    .config("spark.redis.port", "6379")
    .config("spark.cassandra.connection.host", cassandraHost)
    .withExtensions(new CassandraSparkExtensions)
    .config("spark.sql.catalog.mycatalog", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .config("spark.cassandra.output.consistency.level", "ONE")
    .config("spark.cassandra.auth.username", "cassandra")
    .config("spark.cassandra.auth.password", "cassandra")
    .getOrCreate()

  lazy val kafkaProps: Map[String, Object] = Map[String, Object](
    "bootstrap.servers" -> kafkaHost,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "bots-group",
    "auto.offset.reset" -> "latest"
  )

  def parseJsonString(json: String): ReqData = {
    val values: Array[String] = json.replace("{", "")
      .replace("}", "")
      .replace("\"", "")
      .replace(" ", "")
      .split(",")
    val parsedValue = if (values.length < 4) Nil
    else if (!values.forall(_.split(":", 2).length == 2)) Nil
    else values.map(x => x.split(":", 2)(1))
    parsedValue match {
      case Nil => null
      case arr: Array[String] => ReqData(arr(2), arr(0), arr(1), arr(3))
    }
  }

  def checkEventType(data: ReqData): EventDetailsWithTS = {
    if (data.eventType.equals("click"))
      EventDetailsWithTS(data.ip, 1, 0, 1, data.eventTime)
    else
      EventDetailsWithTS(data.ip, 0, 1, 1, data.eventTime)
  }
}
