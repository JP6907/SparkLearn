package Hive

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      .enableHiveSupport()  ////启用spark对hive的支持(可以兼容hive的语法了)
      .getOrCreate()

    //想要使用hive的元数据库，必须指定hivd元数据库的位置，添加一个hive-site.xml到classpath下

    val sql: DataFrame = spark.sql("show tables")

    sql.show()

    spark.close()
  }
}
