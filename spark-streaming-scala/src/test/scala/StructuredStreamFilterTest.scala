import SparkUtils.{ReqData, parseJsonString}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Test
import StructuredStreamFilter._
import org.junit.Assert._

class StructuredStreamFilterTest {
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
  def `check parse`: Unit = {
    val req = parseJsonString("""{"eventTime": 1629119999, "eventType": "click", "ip": "117.17.0.0", "url": "https://zen.yandex.ru/post/3003"}""")
    assertNotNull(req)
    assertEquals(req, ReqData("117.17.0.0", "1629119999", "click", "https://zen.yandex.ru/post/3003"))
  }

  @Test
  def `check detection`: Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("bot-detection")
      .getOrCreate()

    val rdd = sparkSession.sparkContext.parallelize(listData)
    val rowRdd = rdd.map(v => Row(v))
    val df = sparkSession.createDataFrame(rowRdd, StructType(StructField("value", StringType, true) :: Nil))

    val valueDs = convertKafkaStream(df)
    val windowedDf = windowStream(valueDs)

    val arrTupleOfIds = windowedDf.collect().partition(_.getAs[String]("ip") == "117.17.0.0")
    assertEquals(arrTupleOfIds._1.length, 1)
    assert(arrTupleOfIds._1.forall(_.getAs[Long]("requestsCount") > 20))
    assert(arrTupleOfIds._2.forall(_.getAs[Long]("requestsCount") < 20))
  }
}
