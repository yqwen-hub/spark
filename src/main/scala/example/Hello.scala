package example

object Hello extends Greeting with App {
  println(greeting)

  val Yiqing = human("Da Yang Mo Tuo!")
  Yiqing.makeSound()
}

trait Greeting {
  lazy val greeting: String = "hello"
}

case class human(sound: String) extends animal {
}

trait animal {
  def sound : String

  def makeSound(): Unit = {
    println(sound)
  }
}
