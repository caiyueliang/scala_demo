// package com.test
// ===============================================================
class ApplyTest{
  def apply(): Unit = {
    println("apply method in class is called!")
  }
  def greetingOfClass: Unit = {
    println("Greeting method in class is called.")
  }
  def update(string: String): Unit = {
    println("class update: " + string)
  }
}

object ApplyTest{
  def apply() = {
    println("apply method in object is called")
    new ApplyTest()
  }
  def update(string: String): Unit = {
    println("object update: " + string)
  }
}

// ===============================================================
class TestApplyClass {

  def apply(param: String): String = {

    println("apply method called, parameter is: " + param)

    "Hello World!"
  }
}

object TestApplyObject {

  def apply(param: String): String = {

    println("apply method called, parameter is: " + param)

    "Hello World!"
  }

  def apply(param: String, int: Int): String = {

    println("apply method called, parameter is: " + param + int.toString)

    "Hello World!"
  }
}

// ===============================================================
// 抽象类
abstract class Car{       // 是抽象类，不能直接被实例化
  val carBrand: String    // 字段没有初始化值，就是一个抽象字段
  def info()              // 抽象方法，不需要使用abstract关键字
  def greeting() {println("Welcome to my car!")}
}

// 扩展类：继承抽象类
class BMWCar extends Car {
  override val carBrand = "BMW"                                     // 重写超类字段，需要使用override关键字，否则编译会报错
  def info() {printf("This is a %s car. It is on sale", carBrand)}  // 重写超类的抽象方法时，不需要使用override关键字，不过，如果加上override编译也不错报错
  override def greeting() {println("Welcome to my BMW car!")}       // 重写超类的非抽象方法，必须使用override关键字
}

// 扩展类：继承抽象类
class BYDCar extends Car {
  override val carBrand = "BYD"                                     // 重写超类字段，需要使用override关键字，否则编译会报错
  def info() {printf("This is a %s car. It is cheap.", carBrand)}   // 重写超类的抽象方法时，不需要使用override关键字，不过，如果加上override编译也不错报错
  override def greeting() {println("Welcome to my BYD car!")}       // 重写超类的非抽象方法，必须使用override关键字
}

// ===============================================================
class Person {
  private val lastId = 0
  private val id = Person.newPersonId()     // 调用了伴生对象中的方法，返回lastId，会自增
  private var name = ""

  def this(name: String) {
    this()
    this.name = name
  }
  def info() {
    printf("The id of %s is %d. %d. %d\n", name, id, lastId, Person.newPersonId())
  }
}

object Person {
  private var lastId = 0                    // 一个人的身份编号
  // private def newPersonId() = {
  def newPersonId() = {
    lastId = lastId + 1
    lastId
  }

  def main(args: Array[String]){
    val person1 = new Person("Ziyu")
    val person2 = new Person("Minxing")
    person1.info()
    person2.info()

    val myObject = new TestApplyClass
    println(myObject("param1"))
    println(TestApplyObject("param2"))
    println(TestApplyObject("param2", 123))

    val a = ApplyTest() // 这里会调用伴生对象中的apply方法，返回伴生类的实例
    ApplyTest() = "CYL"
    println("111111")
    a.greetingOfClass   // 调用伴生类的实例的方法
    println("222222")
    a()                 // 这里会调用伴生类中的apply方法
    a() = "CCCCC"

    println("333333")
    val b = new ApplyTest()
    println("444444")
    b.greetingOfClass
    println("555555")
    b()

    // Array中含有apply和update
    Array("BigData","Hadoop","Spark")
  }
}