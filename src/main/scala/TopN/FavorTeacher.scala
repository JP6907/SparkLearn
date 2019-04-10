package TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求最受欢迎的老师TopN
  */
object FavorTeacher {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavorTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    //1.获取数据
    val lines = sc.textFile(args(0))
    //2.切分，整理数据
    val teacherAndOne = lines.map(line => {
      //http://bigdata.edu360.cn/laozhang
      val splits :Array[String] = line.split("/")
      val teacher = splits(3)
      (teacher,1)
    })
    //3.聚合
    val reduced : RDD[(String,Int)] = teacherAndOne.reduceByKey(_+_)
    //4.排序
    val sorted : RDD[(String,Int)] = reduced.sortBy(_._2,false)
    //5.colleat,触发action
    val result = sorted.collect()
    //6.打印
    println(result.toBuffer)

    sc.stop()
  }
}
