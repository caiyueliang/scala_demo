//package com.test

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
  private def newPersonId() = {
    lastId = lastId + 1
    lastId
  }

  def main(args: Array[String]){
    val person1 = new Person("Ziyu")
    val person2 = new Person("Minxing")
    person1.info()
    person2.info()
  }
}