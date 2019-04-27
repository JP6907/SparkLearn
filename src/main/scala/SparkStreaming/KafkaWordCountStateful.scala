package SparkStreaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 可以累加中间结果的wc
  */
object KafkaWordCountStateful {

  /**
    * 第一个参数：聚合的key，就是单词
    * 第二个参数：当前批次产生批次该单词在每一个分区出现的次数
    * 第三个参数：初始值或累加的中间结果
    */
  val updateFunc = (iter: Iterator[(String,Seq[Int],Option[Int])]) => {
    iter.map{
      case (x,y,z) => (x,y.sum + z.getOrElse(0))
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    /**
      * 设置累加的历史数据保存的位置
      */
      ssc.checkpoint("E:\\SparkLearn\\src\\main\\resources\\wc_ck")

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
    //val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //打印结果(Action)
    reduced.print()
    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()
  }
}
