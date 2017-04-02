package cn.edu.fudan.mmdb.sparkIndex.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{HBaseAdmin, HConnectionManager}

/**
  * Created by gql on 16-6-16.
  */
object HBaseUtils {

  private var configuration=HBaseConfiguration.create()
  private var conn=HConnectionManager.createConnection(configuration)
  private var admin=new HBaseAdmin(conn)
  def setConfAndConnection(conf:Configuration){
     configuration=conf
     conn=HConnectionManager.createConnection(configuration)
     admin=new HBaseAdmin(conn)
  }
  def DoesTableExist(tableName:String):Boolean= admin.tableExists(tableName)
  def CreateNewTable(tableName:String):Unit={
    val table_desc=new HTableDescriptor(tableName)
    val col_family: HColumnDescriptor = new HColumnDescriptor("node")
    col_family.setMaxVersions(1)
    table_desc.addFamily(col_family)
    admin.createTable(table_desc)
  }
  def Put(key:String,tableName:String,columnFamily:String,columnName:String,value: Array[Byte]):Unit={
    val hbase_table=conn.getTable(tableName)
    val put=new Put(key.getBytes)
    put.add(columnFamily.getBytes,columnName.getBytes,value)
    hbase_table.put(put)
    hbase_table.close()
  }
  def Get(key:String,tableName:String,columnFamily:String,columnName:String): Array[Byte]= {
    val get = new Get(key.getBytes)
    get.addColumn(columnFamily.getBytes, columnName.getBytes)
    conn.getTable(tableName).get(get).value()
  }

  def main(args: Array[String]) {
      println(DoesTableExist("isaxGql"))
      println(Get("root_node_00","isaxGql","node","store"))
  }
}
