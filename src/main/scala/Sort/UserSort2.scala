package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果数据时自定义的类对象
  * 那么也必须实现序列化
  */
object UserSort2 {
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
    val sorted : RDD[(String,Int,Int)] = userRDD.sortBy(u => User2(u._1,u._2,u._3))

    val result = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
}

/**
  * 没有实现序列化！！！
  * 自定义排序规则
  * 使用case class 默认是多例的
  * 而且默认实现序列化
  * u => User2(u._1,u._2,u._3)不需要new
  * @param name
  * @param age
  * @param height
  */
case class User2(name:String,age:Int,height:Int) extends Ordered[User2]{
  override def compare(that: User2): Int = {
    if(this.age == that.age){
      this.height - that.height
    }else{
      -(this.age - that.age)
    }
  }

  override def toString: String = s"name: $name, age: $age, height: $height"
}