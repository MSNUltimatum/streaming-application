import SparkUtils._
import com.datastax.spark.connector.{SomeColumns, toRDDFunctions}
import com.redis.RedisClient
import com.redislabs.provider.redis.toRedisContext
import net.liftweb.json._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}

import java.util.UUID

object DStreamFilter {
  spark.sparkContext.setLogLevel("WARN")

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
    requestsData
      .map(data => (data.ip, checkEventType(data))).persist(StorageLevel.MEMORY_AND_DISK)
      .foreachRDD { rdd =>
        if (!rdd.isEmpty()) {
          val reducedRdd: RDD[(String, EventDetails)] = windowedEventDetails(rdd)

          val filteredRdd = reducedRdd.filter(row => row._2.requestsCount > requestsCount)
          spark.sparkContext.toRedisHASH(filteredRdd.map(row => (row._1, row._2.toString)), redisBotsDs, redisTTL)
        }
      }
  }

  def windowedEventDetails(rdd: RDD[(String, EventDetailsWithTS)]): RDD[(String, EventDetails)] = {
    val minTime = rdd.map(data => data._2.time).min()
    val newRdd = rdd.map(data => createWindow(data, minTime))
    val reducedRdd = newRdd.reduceByKey(reduceEventDetails)
      .map(createEventDetails)
    reducedRdd
  }

  def createEventDetails(eventDet: (String, EventDetailsWithTS)): (String, EventDetails) = {
    (eventDet._1.split(":")(0), EventDetails(eventDet._2.ip, eventDet._2.clickRate,
      eventDet._2.transitionsRate, eventDet._2.requestsCount))
  }

  private def reduceEventDetails = {
    (sumEvents: EventDetailsWithTS, event: EventDetailsWithTS) =>
      EventDetailsWithTS(sumEvents.ip,
        sumEvents.clickRate + event.clickRate,
        sumEvents.transitionsRate + event.transitionsRate,
        sumEvents.requestsCount + event.requestsCount, sumEvents.time)
  }

  def createWindow(tuple: (String, SparkUtils.EventDetailsWithTS), minTime: String): (String, EventDetailsWithTS) = {
    val newKey = f"${tuple._1}:${(tuple._2.time.toLong - minTime.toLong) / windowSize}"
    (newKey, tuple._2)
  }

  def saveToCassandra(requestsData: DStream[ReqData]): Unit = {
    requestsData
      .foreachRDD { rdd =>
        val currentRedisState = redis.hgetall(redisBotsDs)
        val newRdd = rdd.map(row => createFinalReport(row, currentRedisState))
        newRdd.saveToCassandra(cassandraKeySpace, cassandraTable, SomeColumns("id", "ip", "event_time", "event_type", "url", "is_bot"))
      }
  }

  def createFinalReport(data: ReqData, redisState: Option[Map[String, String]]): ReportData = {
    val is_bot = redisState.isDefined && redisState.get.contains(data.ip)
    ReportData(UUID.randomUUID().toString, data.ip, data.eventTime, data.eventType, data.url, is_bot)
  }
}
