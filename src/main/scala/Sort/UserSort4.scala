package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果数据时自定义的类对象
  * 那么也必须实现序列化
  */
object UserSort4 {
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
    //充分利用元组的比较规则，元组的比较规则：先比第一，相等再比第二个
    //Ordering[(Int, Int)]最终比较的规则格式
    //on[(String, Int, Int)]未比较之前的数据格式
    //(t =>(-t._3, t._2))怎样将规则转换成想要比较的格式
    implicit val rules = Ordering[(Int, Int)].on[(String, Int, Int)](t =>(-t._3, t._2))
    val sorted : RDD[(String,Int,Int)] = userRDD.sortBy(u => u)

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
case class User4(name:String,age:Int,height:Int)