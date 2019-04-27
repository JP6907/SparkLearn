package SparkSQL

import IpLocation.MyUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object IPLocation1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocation1").setMaster("local[4]")
    val session = SparkSession.builder().config(conf).getOrCreate()

    val rulelines : Dataset[String] = session.read.textFile("E:\\SparkLearn\\src\\main\\resources\\ip\\ip.txt")


    //获取ip规则表
    import session.implicits._
    val ruleDF : DataFrame = rulelines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val provice = fields(6)
      (startNum,endNum,provice)
    }).toDF("snum","enum","province")

    //获取访问日志
    val accessLines : Dataset[String] = session.read.textFile("E:\\SparkLearn\\src\\main\\resources\\ip\\access.log")
    val ipDF : DataFrame = accessLines.map(log => {
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ruleDF.createTempView("v_rules")
    ipDF.createTempView("v_ips")

    //join这种方式效率比较低下，需要逐条数据比对
    //改进：自定义匹配函数，使用二分查找法，代替join匹配过程
    val result = session.sql("select province,count(*) counts from v_ips join v_rules " +
      "on (ip_num >= snum and ip_num <= enum) group by province order by counts desc")

    result.show()

    session.stop()
  }
}
