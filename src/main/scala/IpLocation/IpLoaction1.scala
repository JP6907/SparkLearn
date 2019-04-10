package IpLocation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//     起始ip|终止ip |起始ip十进制|终止ip十进制|.....................|邮编|...........|经度      | 纬度
//ip:  1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302
//log: 20090121000132095572000|125.213.100.123|show.51.com|/shoplist.php?phpfile=shoplist2.php&style=1&sex=137|Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; Mozilla/4.0(Compatible Mozilla/4.0(Compatible-EmbeddedWB 14.59 http://bsalsa.com/ EmbeddedWB- 14.59  from: http://bsalsa.com/ )|http://show.51.com/main.php|
//ip规则表对应某个地区的ip地址范围
//要求：读取日志文件，按省份统计访问的ip地址数量

//ip规则在本地读取

object IpLoaction1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //在Driver端获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟Driver在同一台机器上）
    //全部的IP规则在Driver端了（在Driver端的内存中了）
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    //将driver端的数据广播到Executor中
    //调用sc上的广播方法
    //广播变量的引用（还在Driver端）
    val broadcastRef : Broadcast[Array[(Long,Long,String)]] = sc.broadcast(rules)

    //创建RDD读取访问日志
    val accessLines : RDD[String] = sc.textFile(args(1))

    //由日志数据查找所在省份的函数
    val func = (line : String) =>{
      val fields = line.split("[|]")
      val ip = fields(1)
      //将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      //进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //查找
      var province = "unknown"
      val index = MyUtils.binarySearch(rulesInExecutor,ipNum)
      if(index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)
    }
    //整理数据
    val provinceAndOne : RDD[(String,Int)] = accessLines.map(func)

    //聚合
    val reduced : RDD[(String,Int)] = provinceAndOne.reduceByKey(_+_)

    val result = reduced.collect()
    println(result.toBuffer)
    sc.stop()
  }

}
