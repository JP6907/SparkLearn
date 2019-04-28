package SparkStreaming.Kafka10

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 在kafka中保存偏移量
  */
object DirectStream {
  def main(args: Array[String]): Unit = {
    val group = "g0011"
    val topic = "kafka10"
    //创建SparkConf，如果将任务提交到集群中，那么要去掉.setMaster("local[2]")
    val conf = new SparkConf().setAppName("DirectStream").setMaster("local[2]")
    //创建一个StreamingContext，其里面包含了一个SparkContext
    val streamingContext = new StreamingContext(conf, Seconds(5));

    //配置kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "earliest", // lastest
      "enable.auto.commit" -> (false: java.lang.Boolean)  //是否自动提交偏移量
    )

    val topics = Array(topic)
    //在Kafka中记录读取偏移量
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      //位置策略
      PreferConsistent,
      //订阅的策略
      Subscribe[String, String](topics, kafkaParams)
    )

    //迭代DStream中的RDD，将每一个时间点对于的RDD拿出来
    stream.foreachRDD { rdd =>
      //获取该RDD对于的偏移量
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      /**
        * 业务逻辑
        */

      //拿出对于的数据，foreach是一个aciton
      rdd.foreach{ line =>
        println(line.key() + " " + line.value())
      }
      //更新偏移量
      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
