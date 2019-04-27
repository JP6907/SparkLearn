package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果数据时自定义的类对象
  * 那么也必须实现序列化
  */
object UserSort3 {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("UserSort1").setMaster("spark://116.56.140.131:50005")  //本地4线程
    //      .setJars(List("E:\\SparkLearn\\target\\Sparklearn-1.0-SNAPSHOT.jar"))
    val conf = new SparkConf().setAppName("UserSort1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val users = Array("zhou 19 170","Li 20 166","Zhang 10 150","Zhang 10 170")
    //将Driver端数据并行化变成RDD
    val lines : RDD[String] = sc.parallelize(users)

    val userRDD: RDD[(String,Int,Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val height = fields(2).toInt
      (name,age,height)
    })
    //规则和数据都是需要实现序列化的
    //隐式转换
    import SortRules.OrderingUser
    val sorted : RDD[(String,Int,Int)] = userRDD.sortBy(u => User3(u._1,u._2,u._3))

    val result = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
}

/**
  * 不写死规则
  * 使用隐式转换
  * @param name
  * @param age
  * @param height
  */
case class User3(name:String,age:Int,height:Int)