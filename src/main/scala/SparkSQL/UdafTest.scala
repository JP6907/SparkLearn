package SparkSQL

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession, types}

/**
  * 求几何平均数
  */
object UdafTest {
  def main(args: Array[String]): Unit = {

    val session = SparkSession.builder()
      .appName("UdafTest")
      .master("local[4]")
      .getOrCreate()

    val geoMean = new GeoMean

    val range: Dataset[lang.Long] = session.range(1,11)

    //注册UDAF函数
    session.udf.register("gm",geoMean)

    /**
      * sql方式
      */
    //创建视图
//    range.createTempView("v_range")
//
//    val result = session.sql("select gm(id) result from v_range")

    /**
      * DSL方式
      */
    import session.implicits._
    val result = range.agg(geoMean($"id").as("geomean"))

    result.show()
    val a = 1*2*3*4*5*6*7*8*9*10
    println(math.pow(a,1.toDouble/10))

    session.stop()
  }
}

class GeoMean extends UserDefinedAggregateFunction{
  //输入的数据类型
  override def inputSchema: StructType = StructType(List(
    StructField("value",DoubleType)
  ))

  //产生中间结果的数据类型
  override def bufferSchema: StructType = StructType(List(
    //相乘之后返回的积
    StructField("product",DoubleType),
    //参与运算的数据的个数
    StructField("counts",LongType)
  ))

  //最终返回的结果类型
  override def dataType: DataType = DoubleType

  //确保一致性，一般用true
  //如果输入相同的数据，则直接返回结果，不需要重新计算
  override def deterministic: Boolean = true

  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.0
    buffer(1) = 0L
  }
  //每有一条数据参与运算就更新一下中间结果，update相当于在每一个分区中的运算
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1L
  }

  //全局聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每个分区计算结果相乘
    buffer1(0) = buffer1.getDouble(0) * buffer2.getDouble(0)
    //参与运算的数据的个数更新
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终的结果
  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(0),1.toDouble/buffer.getLong(1))
  }
}
