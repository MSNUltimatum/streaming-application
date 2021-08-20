import SparkUtils._
import SparkUtils.spark.implicits._
import net.liftweb.json._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

object StructuredStreamFilter {

  implicit val extractionFormat: DefaultFormats.type = DefaultFormats
  val uuidGenerator: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val stream = readStreamFromKafka
    val windowedStream = windowStream(stream)
    writeToRedis(windowedStream)
    writeToCassandra(stream)
    spark.streams.awaitAnyTermination()
  }

  def readStreamFromKafka: Dataset[ReqData] = {
    val kafkaStream: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaHost)
      .option("subscribe", kafkaTopic)
      .load()
    convertKafkaStream(kafkaStream)
  }

  def convertKafkaStream(df: DataFrame): Dataset[ReqData] = {
    df.selectExpr("CAST(value AS STRING) as value")
      .filter(row => row.getString(0).nonEmpty)
      .map { case Row(value) =>
        value match {
          case str: String => parseJsonString(str)
        }
      }.filter(_ != null)
  }

  def windowStream(stream: Dataset[ReqData]): DataFrame = {
    stream
      .map(checkEventType)
      .select($"ip",
        $"clickRate",
        $"transitionsRate",
        $"requestsCount",
        to_timestamp(from_unixtime($"time")).alias("time"))
      .withWatermark("time", s"$watermark seconds")
      .groupBy($"ip", window($"time",
        s"$windowSize seconds",
        s"$windowSize seconds"))
      .agg(
        count("*").as("requestsCount"),
        sum("clickRate").as("clickRate"),
        sum("transitionsRate").as("transitionsRate")
      )
  }

  def writeToRedis(windowedDf: DataFrame): Unit = {
    windowedDf.writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch(writeBathToRedis _).start
  }

  def writeBathToRedis(ds: DataFrame, batchId: Long): Unit = {
    ds.filter($"requestsCount" >= requestsCount)
      .write
      .format("org.apache.spark.sql.redis")
      .option("table", "bots")
      .option("key.column", "ip")
      .option("ttl", redisTTL)
      .mode(SaveMode.Append)
      .save()
  }

  def writeToCassandra(valueDf: Dataset[ReqData]): Unit = {
    valueDf.writeStream
      .outputMode(OutputMode.Update)
      .foreachBatch(writeBathToCassandra _).start
  }

  def writeBathToCassandra(ds: Dataset[ReqData], batchId: Long): Unit = {
    val cachedBots = extractCacheBots()
    ds.join(cachedBots, ds("ip") === cachedBots("rIp"), "left")
      .withColumn("is_bot", $"rIp".isNotNull)
      .select($"ip",
        $"eventTime".alias("event_time"),
        $"eventType".alias("event_type"),
        $"url",
        $"is_bot")
      .withColumn("id", uuidGenerator())
      .write
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", cassandraKeySpace)
      .option("table", cassandraTable)
      .mode(SaveMode.Append)
      .save()
  }

  def extractCacheBots(): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.redis")
      .schema(requestDetailsSchema)
      .option("keys.pattern", "bots:*")
      .option("key.column", "ip")
      .load().toDF("rIp", "rWindow", "rTransitionsRate", "rClickRate", "rRequestsCount")

  }
}
