package SparkStreaming.Order_MultiIndex

import IpLocation.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object CalculateUtil {
  /**
    * 计算总收入
    * 中间结果保存到redis中
    * @param fields
    */
  def calculateIncome(fields : RDD[Array[String]]) ={
    val priceRDD : RDD[Double] = fields.map(arr => {
      val price = arr(4).toDouble
      price
    })
    //当前批次的总金额
    val sum : Double = priceRDD.reduce(_+_)
    val conn = JedisConnectionPool.getConnection()
    val r = conn.incrByFloat(Constant.TOTAL_INCOME,sum)
    println("当前总金额为：" + r)
    conn.close()
  }

  /**
    * 计算分类的成交金额
    * 累加到redis
    * @param fields
    */
  def calculateItem(fields : RDD[Array[String]]) = {
    val itemAndPrice: RDD[(String, Double)] = fields.map(arr => {
      //分类
      val item = arr(2)
      //金额
      val parice = arr(4).toDouble
      (item, parice)
    })
    val reduced : RDD[(String,Double)] = itemAndPrice.reduceByKey(_+_)

    //在driver端创建连接池
    //这种方式不好，Jedis没有实现序列化
    //val conn = JedisConnectionPool.getConnection()

    //累加到redis
    reduced.foreachPartition(part => {
      //获取连接池
      val conn = JedisConnectionPool.getConnection()
      part.foreach(t => {
        val r = conn.incrByFloat(t._1,t._2)
        println("根据种类划分:" + t._1 + ":" + r)
      })
      conn.close()
    })
  }

  /**
    * 根据省份分类
    * @param fields
    * @param broadcastRef
    */
  def calculateZone(fields: RDD[Array[String]], broadcastRef: Broadcast[Array[(Long, Long, String)]]) = {
    val provinceAndPrice: RDD[(String, Double)] = fields.map(arr => {
      val ip = arr(1)
      val price = arr(4).toDouble
      val ipNum = MyUtils.ip2Long(ip)
      //在Executor中获取到广播的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value
      //二分法查找
      val index = MyUtils.binarySearch(allRules, ipNum)
      var province = "未知"
      if (index != -1) {
        province = allRules(index)._3
      }
      //省份，订单金额
      (province, price)
    })

    //按省份进行聚合
    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)
    //将数据跟新到Redis
    reduced.foreachPartition(part => {
      val conn = JedisConnectionPool.getConnection()
      part.foreach(t => {
        val r = conn.incrByFloat(t._1, t._2)
        println("根据省份分类: " + t._1 + ":" + r)
      })
      conn.close()
    })
  }
}
