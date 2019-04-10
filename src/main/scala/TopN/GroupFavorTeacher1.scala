package TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 分组求TopN
  * 将数据按照学科分组之后转化成list
  * 调用scala的sortby方法在本地内存中进行排序
  * 缺点：可能会导致内存不足
  */
object GroupFavorTeacher1 {
  def main(args: Array[String]): Unit = {

    val topN = args(1).toInt

    val conf = new SparkConf().setAppName("GroupFavorTeacher1").setMaster("local[4]")
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
    //以subject做聚合
    val grouped : RDD[(String,Iterable[((String,String),Int)])]= reduced.groupBy(_._1._1)
    //排序
    //一个学科是一个迭代器
    //将迭器转化成list
    //在内存中做排序,使用scala的集合排序，在本地内存中排序，可能会导致内存不够用
    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(topN))

    val result = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
}
