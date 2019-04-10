package Wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount{

  def main(args: Array[String]):Unit = {
    //创建spark配置，设置应用程序名字
    //val conf = new SparkConf().setAppName("ScalaWordCount")
    //本地运行
    val conf = new SparkConf().setAppName("ScalaWordCount").setMaster("spark://hdp-01:7077")  //本地4线程
      .setJars(List("/home/zjp/Documents/Sparklearn/target/Sparklearn-1.0-SNAPSHOT.jar"))
    //创建spark执行入口
    val sc = new SparkContext(conf)
    //创建RDD 弹性分布式数据集
//    sc.textFile(args(0))
//      .flatMap(_.split(" "))
//      .map((_,1))
//      .reduceByKey(_+_)
//      .sortBy(_._2,false)
//      .saveAsTextFile(args(1))

    val lines: RDD[String] = sc.textFile(args(0),2)     //HadoopRDD
    //lines.count()
    //val partlen = lines.partitions.length
    //lines.saveAsTextFile(args(1))
    //lines.count()
    val words: RDD[String] = lines.flatMap(_.split(" "))       //MapPartitionsRDD
    val wordAndOne: RDD[(String,Int)] = words.map((_,1))      //MapPartitionsRDD
    val reduced: RDD[(String,Int)] = wordAndOne.reduceByKey(_+_)  //ShuffledRDD
    val sorted: RDD[(String,Int)] = reduced.sortBy(_._2,false)  //ShuffledRDD
    sorted.saveAsTextFile(args(1)) //MapPartitionsRDD
    sc.stop()
  }
}
