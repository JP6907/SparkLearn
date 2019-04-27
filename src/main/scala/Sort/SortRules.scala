package Sort

object SortRules {
  implicit object OrderingUser extends Ordering[User3] {
    override def compare(x: User3, y: User3): Int = {
      if(x.age == y.age) {
        x.height - y.height
      } else {
        y.age - x.age
      }
    }
  }
}
