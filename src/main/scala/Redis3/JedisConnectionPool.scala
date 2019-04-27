package Redis3

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

  val config = new JedisPoolConfig()
  //最大连接数量
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //对拿到的connection进行validateObject校验
  config.setTestOnBorrow(true)
  //10000代表超时时间10秒
  val pool = new JedisPool(config,"localhost",6379,10000,"123456")

  def getConnection() : Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnection()

    val r1 = conn.get("spark")
    println(r1)

    conn.incrBy("spark",100)

    val r2 = conn.get("spark")
    println(r2)

    val r = conn.keys("*")
    import scala.collection.JavaConversions._
    for(p <- r){
      println(p + ":" + conn.get(p))
    }
  }
}
