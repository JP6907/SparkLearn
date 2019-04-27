package Serializable

import java.net.InetAddress

//class Rules extends Serializable{
//  val rulesMap = Map("hadoop" -> 2.7 , "spark" ->2.4)
//
//}

//第三种方式，希望Rules在EXecutor中被初始化（不走网络了，就不必实现序列化接口）
object Rules extends{
  val rulesMap = Map("hadoop" -> 2.7 , "spark" ->2.4)
  val hostname = InetAddress.getLocalHost.getHostName
  println(hostname + "@@@@@@@@@@@@@@@@！！！！")
}
