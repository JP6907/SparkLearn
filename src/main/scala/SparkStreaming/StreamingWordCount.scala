package SparkStreaming

import SparkStreaming.KafkaWordCountStateful.updateFunc
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * 一个批次计算一次，不会累加多个批次的数据
  */
object StreamingWordCount {

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
    //创建StreamingContext
    //至少需要两个线程，一个读数据线程，一个输出数据线程，如果只有一个线程，则只会读取数据，不会输出
    val conf = new SparkConf().setAppName("StreamingWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //第二个参数的最小批次产生的时间间隔
    val ssc = new StreamingContext(sc,Milliseconds(5000))

    /**
      * 设置中间结果累加保存位置
      */
    ssc.checkpoint("E:\\SparkLearn\\src\\main\\resources\\wc_ck1")

    //有了StreamingContext，就可以创建SparkStreaming的抽象了DSteam
    //从一个socket端口中读取数据
    //在Linux上使用nc工具
    //nc -lk 8888
    val lines : ReceiverInputDStream[String] = ssc.socketTextStream("localhost",8888)

    val words : DStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne : DStream[(String,Int)] = words.map((_,1))

    //val reduced : DStream[(String,Int)] = wordAndOne.reduceByKey(_+_)
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)

    reduced.print()

    //启动sparksteaming程序
    ssc.start()
    //等待优雅的退出
    ssc.awaitTermination()
  }
}
