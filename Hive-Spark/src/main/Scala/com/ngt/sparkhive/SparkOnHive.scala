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
}
