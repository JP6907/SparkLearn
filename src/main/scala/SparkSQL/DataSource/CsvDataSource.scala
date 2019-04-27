package SparkSQL.DataSource

import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CsvDataSource")
      .master("local[*]")
      .getOrCreate()

    val csv : DataFrame = spark.read.csv("E:\\SparkLearn\\src\\main\\resources\\out\\csv")

    csv.printSchema()

    val df : DataFrame = csv.toDF("id","name","age")

    df.show()

    spark.stop()
  }
}
