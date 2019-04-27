package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 这种方法为Receiver方式
  * 使用kafka提供的高级API，消费者读取数据的偏移量由kafka自动记录
  * 而且效率比较慢，容易丢失数据
  * 若程序
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //hdp-02:2181,hdp-03:2181,hdp-04:2181
    //val zkQuorum = "116.56.140.131:50007,116.56.140.131:50008,116.56.140.131:50009"
    val zkQuorum = "localhost:2181"
    val groupId = "g1"
    val topic = Map[String,Int]("test"->1) //使用一个线程来读取
    //创建DStream，需要KafkaDStream
    val data : ReceiverInputDStream[(String,String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topic)
    //对数据进行处理
    //Kafak的ReceiverInputDStream[(String, String)]里面装的是一个元组（key是写入的key(topic)，value是实际写入的内容）
    data.map(_._2).print()
    val lines : DStream[String] = data.map(_._2)
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和一组合在一起
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //打印结果(Action)
    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()
  }
}
