package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 使用SparkSQL 2.x 接口
  * SparkSessiom
  */
object SQLDemo2 {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
//    val session = SparkSession.builder()
//      .appName("SQLDemo2")
//      .master("spark://116.56.140.131:50005")
//      .getOrCreate()
    val conf = new SparkConf().setAppName("SQLDemo2").setMaster("spark://116.56.140.131:50005")
       .setJars(List("E:\\SparkLearn\\target\\Sparklearn-1.0-SNAPSHOT.jar"))
    val session = SparkSession.builder().config(conf).getOrCreate()

    //使用SparkSession创建RDD
    //SparkContext被封装在sparkSession里面
    val lines : RDD[String] = session.sparkContext.textFile("hdfs://116.56.140.131:50006/person/")

    //整理数据
    //行数据
    val rowRDD : RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      Row(id,name,age)
    })

    //结构数据，表头
    val schema : StructType = StructType(List(
      StructField("id",LongType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)
    ))

    //创建DataFrame
    val df : DataFrame = session.createDataFrame(rowRDD,schema)

    //获取Dataset
    import session.implicits._
    val result : Dataset[Row] = df.where($"id" > 2).orderBy($"age"desc)

    result.show()

    session.stop()
  }
}
