import SparkUtils._
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import com.redis.RedisClient
import com.redislabs.provider.redis.toRedisContext
import net.liftweb.json._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}

import java.util.UUID

object DStreamFilter {
  case class ReportData(id: String, ip: String, event_time: String, event_type: String, url: String, is_bot: Boolean)

  implicit val extractionFormat: DefaultFormats.type = DefaultFormats
  val redis = new RedisClient(cassandraHost, 6379)

  def main(args: Array[String]): Unit = {
    val streamingContext = new StreamingContext(spark.sparkContext, streaming.Seconds(batchInterval))
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](Array(kafkaTopic), kafkaProps))

    val rddStream = kafkaStream.transform { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    saveEvents(rddStream)
    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def saveEvents(rdd: DStream[ConsumerRecord[String, String]]): Unit = {
    val requestsData = rdd.filter(_.value().nonEmpty)
      .map(event => parseJsonString(event.value()))

    detectBots(requestsData)
    saveToCassandra(requestsData)
  }

  def detectBots(requestsData: DStream[ReqData]): Unit = {
    val eventDetails = calculateEvents(requestsData)

    writeBotsToRedis(eventDetails)
  }

  def calculateEvents(requestsData: DStream[ReqData]) = {
    requestsData
      .map(data => (data.ip, getEventDetails(data))).persist(StorageLevel.MEMORY_AND_DISK)
      .reduceByKeyAndWindow((sumEvents: EventDetails, event: EventDetails) =>
        EventDetails(sumEvents.ip,
          sumEvents.clickRate + event.clickRate,
          sumEvents.transitionsRate + event.transitionsRate,
          sumEvents.requestsCount + event.requestsCount),
        streaming.Seconds(windowSize), streaming.Seconds(windowSize))
  }

  def getEventDetails(data: ReqData): EventDetails = {
    if (data.eventType.equals("click"))
      EventDetails(data.ip, 1, 0, 1)
    else
      EventDetails(data.ip, 0, 1, 1)
  }

  def writeBotsToRedis(eventsDetails: DStream[(String, EventDetails)]): Unit = {
    val bots = eventsDetails
      .filter(data => data._2.requestsCount > requestsCount)
      .map(_._2.toString)


    bots.foreachRDD { rdd =>
      spark.sparkContext.toRedisSET(rdd, redisBotsDs,redisTTL)
    }
  }

  def saveToCassandra(requestsData: DStream[ReqData]): Unit = {
    val botsSet = redis.smembers(redisBotsDs).get
    requestsData.map(req => createFinalReport(req, botsSet))
      .foreachRDD { rdd =>
      rdd.saveToCassandra(cassandraKeySpace, cassandraTable, SomeColumns("id", "ip", "event_time", "event_type", "url", "is_bot"))
    }
  }
  def createFinalReport(data: ReqData, botsSet: Set[Option[String]]): ReportData = {
    val is_bot = botsSet.exists(rec => rec.nonEmpty && rec.get.contains(data.ip))
    ReportData(UUID.randomUUID().toString, data.ip, data.eventTime, data.eventType, data.url, is_bot)
  }
}
