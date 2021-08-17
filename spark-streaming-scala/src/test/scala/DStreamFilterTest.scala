import org.apache.spark.SparkConf
import DStreamFilter._
import SparkUtils.{ReqData, parseJsonString}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

import scala.collection.mutable

class DStreamFilterTest {
  val mockedData =
    """
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3003"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3003"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3000"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3019"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3011"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3017"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3010"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3017"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3005"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3006"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3017"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3000"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3011"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3010"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3000"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3017"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "26.16.82.1", "url": "https://zen.yandex.ru/post/3002"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "66.16.122.1", "url": "https://zen.yandex.ru/post/3012"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "171.16.160.0", "url": "https://zen.yandex.ru/post/3007"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "79.16.108.3", "url": "https://zen.yandex.ru/post/3008"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "135.16.30.1", "url": "https://zen.yandex.ru/post/3018"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "61.16.117.1", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "48.16.10.2", "url": "https://zen.yandex.ru/post/3011"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "137.16.5.3", "url": "https://zen.yandex.ru/post/3013"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "73.16.223.0", "url": "https://zen.yandex.ru/post/3006"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "33.16.22.0", "url": "https://zen.yandex.ru/post/3001"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "15.16.4.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "46.16.8.2", "url": "https://zen.yandex.ru/post/3010"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "39.16.28.0", "url": "https://zen.yandex.ru/post/3009"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "75.16.198.2", "url": "https://zen.yandex.ru/post/3018"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "158.16.120.2", "url": "https://zen.yandex.ru/post/3016"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "20.16.170.0", "url": "https://zen.yandex.ru/post/3010"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "29.16.219.3", "url": "https://zen.yandex.ru/post/3017"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "155.16.184.3", "url": "https://zen.yandex.ru/post/3006"}
  {"eventTime": 1629119999, "eventType": "view", "ip": "42.16.192.0", "url": "https://zen.yandex.ru/post/3001"}
  {"eventTime": 1629119999, "eventType": "click", "ip": "18.16.168.0", "url": "https://zen.yandex.ru/post/3009"}
  """
  val listData: Array[String] = mockedData.split("\n")

  @Test
  def `check detection`: Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("DStream test")

    val ssc = new StreamingContext(conf, Seconds(1))

    val sparkSession = SparkSession.builder()
      .config(ssc.sparkContext.getConf)
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val inputStream: InputDStream[String] = ssc.queueStream(inputData)
    inputData += sc.makeRDD(listData)
    val inputStreamReq: DStream[ReqData] = inputStream.map(str => parseJsonString(str))
    val eventsStream = calculateEvents(inputStreamReq)
    eventsStream.map(_._2).foreachRDD(rdd => {
      val arrTupleOfIds = rdd.collect().partition(_.ip == "117.17.0.0")
      assert(arrTupleOfIds._1.forall(_.requestsCount > 20))
      assert(arrTupleOfIds._2.forall(_.requestsCount < 20))
    })

  }

}
