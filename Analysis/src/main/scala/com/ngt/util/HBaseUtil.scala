package com.ngt.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.RichSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HConstants, TableName}

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * @author ngt
 * @create 2021-01-03 1:52
 * 用于Flink 向 Hbase 中读写实时站点客流信息
 */
object HBaseUtil {

  /**
   *
   * @param tablename   表名
   * @param familyname  列族名
   */
  class HBaseWriter(tablename:String, familyname:String) extends RichSinkFunction[String] {

    var conn: Connection = null
    val scan: Scan = null
    var mutator: BufferedMutator = null
    var count = 0

    /**
     * 建立HBase连接
     *
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = {
      val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create
      config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.100.102,192.168.100.103,192.168.100.104")
      config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
      config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)
      conn = ConnectionFactory.createConnection(config)

      val tableName: TableName = TableName.valueOf(tablename)
      val params: BufferedMutatorParams = new BufferedMutatorParams(tableName)
      //设置缓存1m，当达到1m时数据会自动刷到hbase
      params.writeBufferSize(1024 * 1024) //设置缓存的大小
      mutator = conn.getBufferedMutator(params)
      count = 0
    }

    /**
     * 处理获取的hbase数据
     *
     * @param value
     * @param context
     */
    override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {

      val cf1 = familyname
      val array: Array[String] = value.split(",")
      val put: Put = new Put(Bytes.toBytes(array(0)))
      put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("name"), Bytes.toBytes(array(1)))
      put.addColumn(Bytes.toBytes(cf1), Bytes.toBytes("count"), Bytes.toBytes(array(2)))
      mutator.mutate(put)
      //      每满2000条刷新一下数据
      if (count >= 2000) {
        mutator.flush()
        count = 0
      }
      count = count + 1
    }

    /**
     * 关闭
     */
    override def close(): Unit = {
      if (conn != null) conn.close()
    }
  }

  /**
   *
   * @param tablename    表名
   * @param familyname   列族名
   * @param withStartRow 开始Row
   * @param withStopRow  终止Row，取不到
   */

  class HBaseReader(tablename:String, familyname:String,withStartRow:String, withStopRow:String) extends RichSourceFunction[(String, String)] {

    private var conn: Connection = null
    private var table: Table = null
    private var scan: Scan = null

    /**
     * 在open方法使用HBase的客户端连接
     *
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = {
      val config: org.apache.hadoop.conf.Configuration = HBaseConfiguration.create()

      config.set(HConstants.ZOOKEEPER_QUORUM, "192.168.100.102,192.168.100.103,192.168.100.104")
      config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
      config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 30000)
      config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30000)

      val tableName: TableName = TableName.valueOf(tablename)
      val cf1: String = familyname
      conn = ConnectionFactory.createConnection(config)
      table = conn.getTable(tableName)
      scan = new Scan()
      // 设置读取的范围 左闭右开
      scan.withStartRow(Bytes.toBytes(withStartRow))
      scan.withStopRow(Bytes.toBytes(withStopRow))
      scan.addFamily(Bytes.toBytes(cf1))
    }

    /**
     *
     * @param sourceContext
     */
    override def run(sourceContext: SourceContext[(String, String)]): Unit = {
      val rs = table.getScanner(scan)
      val iterator = rs.iterator()
      while (iterator.hasNext) {
        val result = iterator.next()
        val rowKey = Bytes.toString(result.getRow)
        val sb: StringBuffer = new StringBuffer()
        for (cell: Cell <- result.listCells().asScala) {
          val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          sb.append(value).append("_")
        }
        val valueString = sb.replace(sb.length() - 1, sb.length(), "").toString
        sourceContext.collect((rowKey, valueString))
      }
    }

    /**
     * 必须添加
     */
    override def cancel(): Unit = {

    }

    /**
     * 关闭hbase的连接，关闭table表
     */
    override def close(): Unit = {
      try {
        if (table != null) {
          table.close()
        }
        if (conn != null) {
          conn.close()
        }
      } catch {
        case e: Exception => println(e.getMessage)
      }
    }
  }

}
