package SparkSQL.DataSource

import org.apache.spark.sql.{DataFrame, SparkSession}

object JSONDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JSONDataSource")
      .master("local[*]")
      .getOrCreate()

    val json : DataFrame = spark.read.csv("E:\\SparkLearn\\src\\main\\resources\\out\\json")

    json.printSchema()
    json.show()

    spark.stop()
  }
}
