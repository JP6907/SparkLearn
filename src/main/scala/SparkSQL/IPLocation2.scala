package SparkSQL

import IpLocation.MyUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IPLocation2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocation2").setMaster("local[4]")
    val session = SparkSession.builder().config(conf).getOrCreate()

    val rulelines : Dataset[String] = session.read.textFile("E:\\SparkLearn\\src\\main\\resources\\ip\\ip.txt")


    //获取ip规则表
    import session.implicits._
    val ruleDataset  = rulelines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val provice = fields(6)
      (startNum,endNum,provice)
    })

    //收集ip规则到driver端
    val rulesInDriver : Array[(Long,Long,String)]= ruleDataset.collect()
    //广播
    val broadcastRef : Broadcast[Array[(Long,Long,String)]] = session.sparkContext.broadcast(rulesInDriver)

    //获取访问日志
    val accessLines : Dataset[String] = session.read.textFile("E:\\SparkLearn\\src\\main\\resources\\ip\\access.log")
    val ipDF : DataFrame = accessLines.map(log => {
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ipDF.createTempView("v_ips")

    //定义一个自定义函数（UDF），并注册
    //该函数的功能是（输入一个IP地址对应的十进制，返回一个省份名称）
    session.udf.register("ip2Province", (ipNum: Long) => {
      //查找ip规则（事先已经广播了，已经在Executor中了）
      //函数的逻辑是在Executor中执行的，怎样获取ip规则的对应的数据呢？
      //使用广播变量的引用，就可以获得
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //根据IP地址对应的十进制查找省份名称
      val index = MyUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if(index != -1) {
        province = ipRulesInExecutor(index)._3
      }
      province
    })

    //join这种方式效率比较低下，需要逐条数据比对
    //改进：将小表缓存，自定义count函数
    val result = session.sql("select ip2Province(ip_num) province, count(*) counts from v_ips " +
      "group by province order by counts desc")

    result.show()

    session.stop()
  }
}
