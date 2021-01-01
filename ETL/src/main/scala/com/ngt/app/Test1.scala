package com.ngt.app


import scala.collection.mutable.ListBuffer

/**
 * @author ngt
 * @create 2020-12-26 1:41
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    val outList: ListBuffer[Int] = ListBuffer()
    println(outList.size)
    outList.append(1)
    outList.append(2)

    println(outList.length)
    println(outList.size)

//    println(outList.size)
//    outList.clear()
//    println(outList.size)
  }
}
