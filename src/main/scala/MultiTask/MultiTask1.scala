package MultiTask

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object MultiTask1 {
  def main(args: Array[String]): Unit = {

    val startDate = "2016-09-16"
    val endDate = "2016-09-18"

    val dateFormat1 = new SimpleDateFormat("yyyy-MM-dd")
    val dateFormat2 = new SimpleDateFormat("yyyy年MM月dd日,E,HH:mm:ss")

    val startTime = dateFormat1.parse(startDate).getTime
    val endTime = dateFormat1.parse(endDate).getTime

    val conf = new SparkConf().setAppName("MultiTask").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines : RDD[String] = sc.textFile("E:\\SparkLearn\\src\\main\\resources\\GameLog.txt")

    //整理数据
    val splited : RDD[Array[String]] = lines.map(line => line.split("[|]"))

    //过滤数据
    val filtered = splited.filter(fields => {
      val t = fields(0)
      val time = fields(1)
//      val fu = FilterUtils
//      fu.filterByTime(fields,startTime,endTime)
      FilterUtils.filterByTime(fields,startTime,endTime)
    })

    val count = filtered.count()
    println(count)
    sc.stop()

  }
}
