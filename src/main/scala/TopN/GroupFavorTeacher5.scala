package TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 方法四的改进
  * 聚合和分区合并
  * 在聚合的时候指定分区规则
  * 求topn：
  * 长度为5的一个可以排序的集合 immutable.TreeMap
  * 要求top5，每次移除最小的，再添加数据，最后只剩下5个最大的
  * https://blog.csdn.net/liangyihuai/article/details/54925737
  *
  */
object GroupFavorTeacher5 {
  def main(args: Array[String]): Unit = {
    val topN = args(1).toInt
    val conf = new SparkConf().setAppName("GroupFavorTeacher5").setMaster("local[4]")
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

      //长度为5的一个可以排序的集合
      //要求top5，每次移除最小的，再添加数据，最后只剩下5个最大的

    })

    //收集结果
    val r: Array[((String, String), Int)] = sorted.collect()
    println(r.toBuffer)

    sc.stop()
  }
}
