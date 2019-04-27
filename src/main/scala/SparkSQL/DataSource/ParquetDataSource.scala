package SparkSQL.DataSource

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

object ParquetDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ParquetDataSource")
      .master("local[*]")
      .getOrCreate()

    //val parquet : DataFrame = spark.read.parquet("E:\\SparkLearn\\src\\main\\resources\\out\\parquet")
    val parquet : DataFrame = spark.read.format("parquet").load("E:\\SparkLearn\\src\\main\\resources\\out\\parquet")

    parquet.printSchema()
    parquet.show()

    spark.stop()
  }
}
