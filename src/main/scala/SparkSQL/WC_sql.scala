package SparkSQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * sql方式计算wordcount
  */
object WC_sql {
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

    //执行sql，Transformation,lazy
    //列名默认为 value
    //val result : DataFrame = session.sql("select * from v_wc")
    val result : DataFrame = session.sql("select value word,COUNT(*) counts from v_wc group by word order by counts desc")

    result.show()

    session.stop()

  }
}
