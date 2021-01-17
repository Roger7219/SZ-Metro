package com.ngt.sparkhive

import org.apache.spark.sql.SparkSession
/**
 * @author ngt
 * @create 2021-01-16 21:32
 */
case class SparkOnHive() {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("SparkOnHive")
    .enableHiveSupport()
    .config("spark.driver.host", "lx")
    .getOrCreate()

  @Test //ok
  def testLocal() {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("AppLocal2")
      .config("spark.driver.host", "lx") // 多网卡机器，会识别到错误的主机名，要写绝对地址，不要 localhost
      .enableHiveSupport()
      .getOrCreate()

    //spark.sparkContext.setLogLevel("warn")
    import spark.sql

    //hdfs 启用 LZO 压缩以后，需要在客户端添加 LZO 依赖库的环境变量和 maven 坐标，windows本地配置lzo读取所需相关组件_运维_coolerzZ的博客-CSDN博客 https://blog.csdn.net/coolerzz/article/details/103952188
    sql(
      """
				|select * from szt.ods_szt_data limit 100
				|""".stripMargin)
      .show(200, false)

    spark.stop()
  }
}
