package SparkSQL

import org.apache.parquet.format.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用SparkSQL 1.x的接口
  * sqlContext
  */
object SQLDemo1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("spark://116.56.140.131:50005")
      .setJars(List("E:\\SparkLearn\\target\\Sparklearn-1.0-SNAPSHOT.jar"))
    val sc = new SparkContext(conf)
    //将SparkContext增强获得sqlcontext
    val sqlContext = new SQLContext(sc)
    //创建特殊的RDD（DataFrame）,就是有schema信息的RDD
    //先创建普通的RDD，关联上schema，进而转化成DataFrame
    val lines = sc.textFile("hdfs://116.56.140.131:50006/person/")

    /**
      * 创建DataFrame的两种方式
      */
    /**
      * DataFrame：方法1
      * 创建普通RDD，调用.toDF方法转化成DataFrame
      */
//    val personRDD : RDD[Person] = lines.map(line => {
//      val fields = line.split("[,]")
//      val id = fields(0).toLong
//      val name = fields(1)
//      val age = fields(2).toInt
//      Person(id,name,age)
//    })

    //该RDD装的是Boy类型的数据，有了shcma信息，但是还是一个RDD
    //将RDD转换成DataFrame
    //导入隐式转换
//    import sqlContext.implicits._
//    val pdf : DataFrame = personRDD.toDF
//
//    //变成DF后就可以使用两种API进行编程了
//    //把DataFrame先注册临时表
//    pdf.registerTempTable("t_person")


    /**
      * DataFrame：方法2
      * 创建RowRDD，和StructType表描述信息
      * 关联RowRDD和StructType，创建DataFrame
      */
    val rowRDD : RDD[Row] = lines.map(line => {
      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      Row(id,name,age)
    })
      //结果类型，创建表结构描述，即表头，用于描述DataFrame
      val sch : StructType = StructType(List(
        StructField("id",LongType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      ))
      //将RowRDD关联schema
      val pdf : DataFrame = sqlContext.createDataFrame(rowRDD,sch)

    /**
      * 使用DataFrame进行数据查找的两种方式
      */
    /**
      * 方法1
      * 注册临时表，是用sql语句
      */
    //注册临时表
//    pdf.registerTempTable("t_person")
//    //SQL方式，SQL方法其实是Transformation
//    val result : DataFrame = sqlContext.sql("select * from t_person order by age desc")

    /**
      * 方法2
      * DSL方式
      */
    val df1 : DataFrame = pdf.select("name","age")
    //隐式转化
    import sqlContext.implicits._
    val df2 : DataFrame = df1.orderBy($"age" desc)
    //查看结果，触发action
    df2.show()

    sc.stop()
  }
}


case class Person(id:Long,name:String,age:Int)
