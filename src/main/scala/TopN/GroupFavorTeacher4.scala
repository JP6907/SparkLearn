package TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 方法三的改进
  * 聚合和分区合并
  * 在聚合的时候指定分区规则
  * 该方法是在内存中排序
  */
object GroupFavorTeacher4 {
  def main(args: Array[String]): Unit = {
    val topN = args(1).toInt
    val conf = new SparkConf().setAppName("GroupFavorTeacher3").setMaster("local[4]")
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
    //计算有多少学科
    val subjects :Array[String] = subjectTeacherAndOne.map(_._1._1).distinct().collect()
    //按照学科进行分区
    val sbPartitioner = new SubjectPartitioner(subjects)

    //聚合，聚合是就按照指定的分区器进行分区
    //该RDD一个分区内仅有一个学科的数据
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(sbPartitioner, _+_)

    val sorted: RDD[((String, String), Int)] = reduced.mapPartitions(it => {
      //将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(topN).iterator

      //即排序，有不全部加载到内存



    })

    //收集结果
    val r: Array[((String, String), Int)] = sorted.collect()
    println(r.toBuffer)

    sc.stop()
  }
}
