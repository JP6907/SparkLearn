package SparkStreaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

/**
  * zookeeper保存偏移量改进
  */
object KafkaDirectWordCount2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf,Duration(5000))

    val groupId = "group1"
    val topic = "spark"
    ////指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    //hdp-02:9092,hdp-03:9092,hdp-04:9092
    //val brokerList = "116.56.140.131:50010,116.56.140.131:50011,116.56.140.131:50012"
    val brokerList = "localhost:9092"
    //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
    //hdp-02:2181,hdp-03:2181,hdp-04:2181
    //val zkQuorum = "116.56.140.131:50007,116.56.140.131:50008,116.56.140.131:50009"
    val zkQuorum = "localhost:2181"
    //创建 stream 时使用的 topic 名字集合，SparkStreaming可同时消费多个topic
    val topics : Set[String] = Set(topic)

    //创建一个 ZKGroupTopicDirs 对象,其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs = new ZKGroupTopicDirs(groupId,topic)
    //获取 zookeeper 中的路径 "/g001/offsets/wordcount/"
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //准备kafka的参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      //从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    //zookeeper 的host 和 ip，创建一个 client,用于跟新偏移量量的
    //是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
    val zkClient = new ZkClient(zkQuorum)  //不是ZKClient，k为小写

    //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    //在zookeeper中保存的路径为：/consumers/g1/offsets/test
    // /g001/offsets/wordcount/0/10001"
    // /g001/offsets/wordcount/1/30001"
    // /g001/offsets/wordcount/2/10001"
    //zkTopicPath  -> /g001/offsets/wordcount/
    //获取各个分区的偏移量，会按0、1、2顺序保存
    val children = zkClient.countChildren(zkTopicPath)

    var kafkaStream : InputDStream[(String,String)] = null

    //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置
    var fromOffsets : Map[TopicAndPartition,Long] = Map()

    /**
      * 获取偏移量
      */
    //如果保存过 offset
    if(children > 0){
      //将各个分区的偏移量读取出来，保存在 fromOffsets 中
      for(i <- 0 until children){
        // /g001/offsets/wordcount/0/10001
        // /g001/offsets/wordcount/0
        val partitionOffset = zkClient.readData[String](s"$zkTopicPath/${i}")
        // wordcount/0
        val tp = TopicAndPartition(topic, i)
        //将不同 partition 对应的 offset 增加到 fromOffsets 中
        // wordcount/0 -> 10001
        fromOffsets += (tp -> partitionOffset.toLong)
        println(tp.toString() + ":" + partitionOffset.toLong)
      }

      //根据记录的偏移量创建kafkaStream
      //Key: kafka的key   values: "hello tom hello jerry"
      //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (kafka的key, message) 这样的 tuple
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      //通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
      //[String, String, StringDecoder, StringDecoder,     (String, String)]
      //  key    value    key的解码方式   value的解码方式
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }else{
      println("未保存偏移量。。。")
      //如果未保存，根据 kafkaParam 的配置使用最新(largest)或者最旧的（smallest） offset
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    /**
      * 读取数据
      * 记录偏移量
      */
    //记录当前读取批次RDD数据的偏移量的范围(包含多个分区)
    var offsetRanges = Array[OffsetRange]()

    //从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
    //该transform方法计算获取到当前批次RDD,然后将RDD的偏移量取出来，然后在将RDD返回到DStream
//    val transform : DStream[(String,String)] = kafkaStream.transform{ rdd =>
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    }

    //直连方式只有在KafkaDStream的RDD（KafkaRDD）中才能获取偏移量，那么就不能到调用DStream的Transformation
    //所以只能子在kafkaStream调用foreachRDD，获取RDD的偏移量，然后就是对RDD进行操作了
    //依次迭代KafkaDStream中的KafkaRDD
    //如果使用直连方式累加数据，那么就要在外部的数据库中进行累加（用KeyVlaue的内存数据库（NoSQL），Redis）
    kafkaStream.foreachRDD{kafkaRDD =>
      //只有KafkaRDD可以强转成HasOffsetRanges，并获取到偏移量
      offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      val lines: RDD[String] = kafkaRDD.map(_._2)
      //对RDD进行操作，触发Action
      lines.foreachPartition(partition =>
        partition.foreach(x => {
          println(x)
        })
      )

      //保存各个分区的偏移量到zookeeper
      for(o <- offsetRanges){
        //  /g001/offsets/wordcount/0
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient,zkPath,o.untilOffset.toString)
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
