package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object WC_Dataset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SQLDemo2").setMaster("spark://116.56.140.131:50005")
      .setJars(List("E:\\SparkLearn\\target\\Sparklearn-1.0-SNAPSHOT.jar"))
    val session = SparkSession.builder().config(conf).getOrCreate()

    //session.read支持读取多种数据
    val lines : Dataset[String] = session.read.textFile("hdfs://116.56.140.131:50006/wc2/input")

    import session.implicits._
    val words : Dataset[String] = lines.flatMap(_.split(" "))

    //注册视图
    words.createTempView("v_wc")

    //使用Dataset的API（DSL）
    //val result = words.groupBy($"value" as "word").count().orderBy($"count" asc)

    import org.apache.spark.sql.functions._
    val result = words.groupBy($"value".as("word")).agg(count("*") as "counts").orderBy($"counts" desc)

    result.show()

    session.stop()

  }
}
