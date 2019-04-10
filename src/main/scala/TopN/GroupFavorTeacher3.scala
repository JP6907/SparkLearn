package TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 分组求topN
  * 将聚合后的数据按照学科进行分区
  * 每次操作一个分区进行排序
  * 该方法是在内存中排序
  */
object GroupFavorTeacher3 {
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
    //(subject,teacher)为key做聚合
    val reduced : RDD[((String,String),Int)] = subjectTeacherAndOne.reduceByKey(_+_)

    //计算有多少学科
    val subjects :Array[String] = reduced.map(_._1._1).distinct().collect()
    //按照学科进行分区
    val sbPartitioner = new SubjectPartitioner(subjects)
    val partitioned : RDD[((String,String),Int)] = reduced.partitionBy(sbPartitioner)
    //对每个分区进行排序
    //val sorted : RDD[((String,String),Int)] = partitioned.sortBy(_._2)
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      //将迭代器转换成list，然后排序，在转换成迭代器返回
      it.toList.sortBy(_._2).reverse.take(topN).iterator
    })

    val result = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
}


//自定义分区器
class SubjectPartitioner(subjects : Array[String]) extends Partitioner{
  val rules = new mutable.HashMap[String,Int]()

  var i = 0
  for(sb <- subjects){
    rules.put(sb,i)
    i += 1
  }

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = {
    val subject = key.asInstanceOf[(String,String)]._1
    rules(subject)
  }
}
