package SparkSQL.DataSource

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

object JDBCDataSource {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JDBCSataSource")
      .master("local[*]")
      .getOrCreate()

    //load方法并不会真正读取mysql数据
    //只会获取表的元数据信息，如表头
    val users : DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://116.56.140.131:13306/sparktest",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "user",
        "user" -> "root",
        "password" -> "root")
    ).load()

    //打印表结构信息
    //lines.printSchema()

//    val filtered : Dataset[Row] = users.filter(u => {
//      u.getAs[Int]("id") > 2
//    })
//    filtered.show()

    /**
      * lambda表达式
      */
    import spark.implicits._
//    val result = users.filter($"id" > 2)
//    val result1 = users.where($"id">3)

    val result : DataFrame = users.select($"id",$"name",$"age"*10 as "age")

    //result.show()

    /**
      * 写入数据库
      */
    val props = new Properties()
    props.put("user","root")
    props.put("password","root")
//    默认为SaveMode.ErrorIfExists模式，该模式下，如果数据库中已经存在该表，则会直接报异常，导致数据不能存入数据库.另外三种模式如下：
//    SaveMode.Append 如果表已经存在，则追加在该表中；若该表不存在，则会先创建表，再插入数据；
//    SaveMode.Overwrite 重写模式，其实质是先将已有的表及其数据全都删除，再重新创建该表，最后插入新的数据；
//    SaveMode.Ignore 若表不存在，则创建表，并存入数据；在表存在的情况下，直接跳过数据的存储，不会报错
    //result.write.mode(SaveMode.Ignore).jdbc("jdbc:mysql://116.56.140.131:13306/sparktest","user",props)
    //result.write.mode(SaveMode.ErrorIfExists).jdbc("jdbc:mysql://116.56.140.131:13306/sparktest","user",props)
    //result.write.mode(SaveMode.Append).jdbc("jdbc:mysql://116.56.140.131:13306/sparktest","user",props)

    /**
      * 写入其它文件格式
      */
//    DataFrame保存成text时出错(只能保存一列)
//    错误信息：Text data source does not support int data type.
//    result.write.text("E:\\SparkLearn\\src\\main\\resources\\out\\text")
//    json保存了数据格式,表头
//    result.write.json("E:\\SparkLearn\\src\\main\\resources\\out\\json")
//    csv没有保存表头信息
//    result.write.csv("E:\\SparkLearn\\src\\main\\resources\\out\\csv")
//    parquet保存了表头信息
//    result.write.parquet("E:\\SparkLearn\\src\\main\\resources\\out\\parquet")

    spark.stop()
  }
}
