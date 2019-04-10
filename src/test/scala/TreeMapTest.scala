object TreeMapTest extends App{

  val data = scala.collection.mutable.Set.empty[Int]
  data ++= List(1,2,3)
  data += 4
  println(data)

  data --= List(2,3)
  println(data)

  //Set里面的元素不会重复
  data += 1
  println(data)

  data.clear()
  println(data)

  val map = scala.collection.mutable.Map.empty[String,String]
  map("Java") = "Hadoop"
  map("Scala") = "Spark"
  println(map)
  println(map("Java"))

  //自动排序
  val treeSet = scala.collection.mutable.TreeSet(9, 3, 1, 8, 0, 2, 7, 4, 6, 5,5,5)
  println(treeSet)

  val treeSetForChar = scala.collection.mutable.TreeSet("Spark","Scala","Hadoop")
  println(treeSetForChar)

  //按照key自动排序
  val treeMap = scala.collection.immutable.TreeMap("Spark"->"1","Java"->"2")
  println(treeMap)

  val treeMap2 = treeMap("Spark")

}
