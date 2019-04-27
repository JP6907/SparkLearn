package Sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果数据时自定义的类对象
  * 那么也必须实现序列化
  */
object UserSort1 {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("UserSort1").setMaster("spark://116.56.140.131:50005")  //本地4线程
//      .setJars(List("E:\\SparkLearn\\target\\Sparklearn-1.0-SNAPSHOT.jar"))
    val conf = new SparkConf().setAppName("UserSort1").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val users = Array("zhou 19 170","Li 20 166","Zhang 10 150","Zhang 10 170")
    //将Driver端数据并行化变成RDD
    val lines : RDD[String] = sc.parallelize(users)

    //切分整理数据
//    val userRDD: RDD[(String,Int,Int)] = lines.map(line => {
//      val fields = line.split(" ")
//      val name = fields(0)
//      val age = fields(1).toInt
//      val height = fields(2).toInt
//      (name,age,height)
//      //new User(name,age,height)
//    })
//    val sorted : RDD[(String,Int,Int)] = userRDD.sortBy(_._3)

//    val userRDD: RDD[User] = lines.map(line => {
//      val fields = line.split(" ")
//      val name = fields(0)
//      val age = fields(1).toInt
//      val height = fields(2).toInt
//      new User(name,age,height)
//    })
//      val sorted : RDD[User] = userRDD.sortBy(u => u)

//      val userRDD: RDD[(String,Int,Int)] = lines.map(line => {
//        val fields = line.split(" ")
//        val name = fields(0)
//        val age = fields(1).toInt
//        val height = fields(2).toInt
//        (name,age,height)
//      })
//      val sorted : RDD[(String,Int,Int)] = userRDD.sortBy(u => new User(u._1,u._2,u._3))

      val userRDD: RDD[(String,Int,Int)] = lines.map(line => {
        val fields = line.split(" ")
        val name = fields(0)
        val age = fields(1).toInt
        val height = fields(2).toInt
        (name,age,height)
      })
    //充分利用元组的比较规则，元组的比较规则：先比第一，相等再比第二个
      val sorted : RDD[(String,Int,Int)] = userRDD.sortBy(u => (-u._3,u._2))

    val result = sorted.collect()
    println(result.toBuffer)
    sc.stop()
  }
}

/**
  * 实现序列化
  * 自定义排序规则
  * @param name
  * @param age
  * @param height
  */
class User(val name:String,val age:Int,val height:Int) extends Ordered[User] {
  override def compare(that: User): Int = {
    if(this.age == that.age){
      this.height - that.height
    }else{
      -(this.age - that.age)
    }
  }

  override def toString: String = s"name: $name, age: $age, height: $height"
}