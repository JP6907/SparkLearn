package IpLocation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//ip规则在hdfs读取
//将结果存放到mysql中

object IpLoaction2 {

  def main(args: Array[String]): Unit = {
    //conf
    val conf = new SparkConf().setAppName("IpLoaction2").setMaster("spark://hdp-01:7077")
      .setJars(List("/home/zjp/Documents/Sparklearn/target/Sparklearn-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    //从hdfs读取ip规则数据
    val rulesLines = sc.textFile(args(0))
    //整理ip规则数据
    //整理ip规则数据
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //从各个executor收集ip规则到driver端
    val rulesInDriver : Array[(Long,Long,String)] = ipRulesRDD.collect()
    //从driver端广播ip规则到各个executor
    val broadcastRef = sc.broadcast(rulesInDriver)
    //读取日志文件
    val accessLines = sc.textFile(args(1))
    //整理数据，映射，查找获取省份
    //整理数据
    val proviceAndOne: RDD[(String, Int)] = accessLines.map(log => {
      //将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      //Driver端广播变量的引用是怎样跑到Executor中的呢？
      //Task是在Driver端生成的，广播变量的引用是伴随着Task被发送到Executor中的
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //查找
      var province = "未知"
      val index = MyUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    })
    //收集聚合
    val reduced : RDD[(String,Int)] = proviceAndOne.reduceByKey(_+_)
    //收集数据
    reduced.foreachPartition(it => MyUtils.data2Mysql(it))
    //关闭sparkcontext
    sc.stop()
  }
}
