package TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分组求TopN
  * 按subject+teacher聚合之后
  * 依此过滤出每个学科的数据
  * 调用RDD的sortby方法，使用内存+磁盘进行排序
  */
object GroupFavorTeacher2 {
  def main(args: Array[String]): Unit = {
    val topN = args(1).toInt

    val conf = new SparkConf().setAppName("GroupFavorTeacher2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //获取数据
    val lines = sc.textFile(args(0))
    //整理数据
    val subjectTeacherAndOne = lines.map(line => {
      val splits :Array[String] = line.split("/")
      val subject = splits(2).split("[.]")(0)
      val teacher = splits(3)
      ((subject,teacher),1)
    })
    //(subject,teacher)为key做聚合
    val reduced : RDD[((String,String),Int)] = subjectTeacherAndOne.reduceByKey(_+_)

    val subjects = Array("bigdata", "javaee", "php")

    //依此将每个学科的数据过滤出来，减少数据量
    //调用RDD的sortby方法，内存+磁盘排序
    for(sb <- subjects){
      val filtered : RDD[((String,String),Int)] = reduced.filter(_._1._1 == sb)
      val favTeacher = filtered.sortBy(_._2,false).take(topN)
      //filtered.saveAsTextFile("/home/zjp/Documents/Sparklearn/src/main/resources/topn/"+sb)
      println(favTeacher.toBuffer)
    }

    sc.stop()
  }
}
