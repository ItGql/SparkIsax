package cn.edu.fudan.mmdb.sparkIndex

import java.util
import cn.edu.fudan.mmdb.hbase.isax.index.{TerminalNodePersisted, InternalNodePersisted, ISAXIndex}
import cn.edu.fudan.mmdb.isax
import cn.edu.fudan.mmdb.sparkIndex.hbase.HBaseUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, HTable}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import collection.JavaConversions._
import cn.edu.fudan.mmdb.isax.Sequence
import cn.edu.fudan.mmdb.isax.index.{TimeseriesInstance, NodeType, IndexHashParams}
import cn.edu.fudan.mmdb.timeseries.Timeseries
import org.apache.spark.{SparkContext, SparkConf}
import java.util.{ArrayList => JavaList}
import scala.util.control._
import cn.edu.fudan.mmdb.isax.ISAXUtils
/**
  * Created by gql on 17/3/26.
  */
object SparkIndex {

  def main(args: Array[String]) {
    if(args.length <3){
      System.out.println("please give me a input  args[0] means tableName in hbase  args[1] means inputFile args[2] means Maximum depth you want" );
    }

    val sparkConf = new SparkConf().setAppName("SparkIndex")
    val sc = new SparkContext(sparkConf)
    //parameters settings

    val offset = 0
    //the length of timeseries which can be reset
    val tslen = 364  
    //segmentations on the x axis
    val word_length = 14
    //cardinality of the first level
    val base_card = 4
    val level = args(2).toInt
    //the split threshold 
    val k =50
    //caradinality sequence for every level in the isax tree
    val init_cardsPerlevel = initCard(word_length, base_card, level)
    //all preprocessed timeseries
    val timeseries = sc.textFile(args(1)).map(x => x.split("\\t").map(_.toDouble)).map(x => new Timeseries(x, offset, tslen))
    timeseries.cache()

    //get a global isax tree
    val InterNodekeysRDD = timeseries.flatMap(ts =>  calcPath(init_cardsPerlevel, level, ts)).map(x => (x, 1)).reduceByKey(_+_).filter(_._2 > k).keys
    //println("number of internal nodes ＝" + InterNodekeysRDD.count())
    var tree = InterNodekeysRDD.collect()
    val globalTreeSet = tree.toSet
    sc.broadcast(globalTreeSet)

    System.out.println("number of uninserted timeseries = "+timeseries.map(ts => (getLeafRepresentation(ts, level, globalTreeSet, init_cardsPerlevel), ts)).filter(_._1 == "limited").count())
    val TerminalNodeRDD = timeseries.map(ts => (getLeafRepresentation(ts, level, globalTreeSet, init_cardsPerlevel), ts)).filter(_._1 != "limited").groupByKey()
    /*choose another height
    TerminalNodeRDD.keys.filter(_ == "limited").count()
     */

    val InternalNodeRDD = InterNodekeysRDD.union(TerminalNodeRDD.keys).map(indexhash => (getParentIndexHash(indexhash, word_length, base_card), indexhash)).groupByKey()
    //transform the contents that need to be persisted into hbase ：InternalNode,root,TerminalNode
    val rootRDD = InternalNodeRDD.filter(_._1=="root_node_00")
    val InternalRDD = InternalNodeRDD.filter(_._1 != "root_node_00")
    val internalNodePersisted = InternalNodeRDD.map(x=>(x._1,persistInternalNode(word_length, tslen, k, x._1, x._2, NodeType.INTERNAL)))
    val terminalNodePersisted = TerminalNodeRDD.map(x=>(x._1,persistTerminalNode(word_length, tslen, k, x._1, x._2)))
    val rootNodePersisted = rootRDD.map(x=>(x._1,persistInternalNode(word_length,tslen,k,x._1,x._2,NodeType.ROOT)))
   // println("Terminal nums = " +TerminalNodeRDD.count())
    //println("root nums = "+ rootRDD.count())
   // println("internal nums = "+ InternalRDD.count())
    if(!HBaseUtils.DoesTableExist(args(0)))
      HBaseUtils.CreateNewTable(args(0))
    terminalNodePersisted.foreachPartition{
      x=>{
        val conf = HBaseConfiguration.create()
        conf.set(TableInputFormat.INPUT_TABLE, args(0))
        val mytable=new HTable(conf,args(0))
        mytable.setAutoFlush(false,false)
        mytable.setWriteBufferSize(3*1024*1024)
        x.foreach{
          y=>{
            val put=new Put(Bytes.toBytes(y._1))
            put.add(Bytes.toBytes(ISAXIndex.COLUMN_FAMILY_NAME),Bytes.toBytes(ISAXIndex.STORE_COL_NAME),y._2)
            mytable.put(put)
          }
        }
        mytable.flushCommits()
      }
    }
    rootNodePersisted.union(internalNodePersisted).foreachPartition{
      x=>{
        val conf = HBaseConfiguration.create()
        conf.set(TableInputFormat.INPUT_TABLE, args(0))
        val mytable=new HTable(conf,args(0))
        mytable.setAutoFlush(false,false)
        mytable.setWriteBufferSize(3*1024*1024)
        x.foreach{
          y=>{
            val put=new Put(Bytes.toBytes(y._1))
            put.add(Bytes.toBytes(ISAXIndex.COLUMN_FAMILY_NAME),Bytes.toBytes(ISAXIndex.STORE_COL_NAME),y._2)
            mytable.put(put)
          }
        }
        mytable.flushCommits()
      }
    }
    sc.stop();

  }

  //获得父节点的isax表达式
  def getParentIndexHash(indexHash: String, word_length:Int, base_card: Int) = {
    val w1_in = new isax.Sequence(word_length);
    w1_in.parseFromIndexHash(indexHash);
    val cards = w1_in.getCardinalities;
    var promotionPos = cards.size()-1;
    for (i <- (0 until cards.size()-1)){
      if(cards.get(i) > cards.get(i+1))
        promotionPos = i
    }
    if(cards.get(promotionPos) == base_card)
      "root_node_00"
    else{
    w1_in.getSymbols.get(promotionPos).cardinality>>=1;
    w1_in.getSymbols.get(promotionPos).saxCharacter>>=1;
    w1_in.getIndexHash
    }
  }

  def nextCard(Indexhash:String,wordlength:Int): util.ArrayList[Integer] ={
    val tmp=new Sequence(wordlength)
    tmp.parseFromIndexHash(Indexhash)
    IndexHashParams.generateChildCardinality(tmp.getCardinalities)
  }

  def getLeafRepresentation(timeseries: Timeseries, level: Int, globaltree:scala.collection.immutable.Set[String] , cardsPerlevel:JavaList[JavaList[Integer]] ) ={
    var i = 0
    var ans = new String
    val loop = new Breaks
    loop.breakable {
      while (i < level) {
        //如果是InternalNode，下一层
        val tmp = ISAXUtils.CreateiSAXSequenceBasedOnCardinality(timeseries, cardsPerlevel.get(i)).getIndexHash()
        if (globaltree.contains(tmp)) {
          i = i + 1
        }
        else {
          ans = tmp
          loop.break()
        }
      }
    }
    if(i == level)
      ans = "limited"

    ans
  }
  //4.13 1dian
  def initCard(word_length:Integer, base_card:Integer, level : Integer) ={
    var init_cards = new JavaList[JavaList[Integer]]()
    var arSequenceCards = new JavaList[Integer]()
    for( i <- 0 to word_length-1) {
      arSequenceCards.add(base_card)
    }
    init_cards.add(arSequenceCards)
    for( j <- (1 until level)){
       init_cards.add(IndexHashParams.generateChildCardinality(init_cards.get(j-1)))
    }
    init_cards
  }
  def calcPath(fullIsaxTree: JavaList[JavaList[Integer]], level: Int, timeseries: Timeseries) = {
    val path = new JavaList[String]();
    for (i <- (0 until level)){
      path.add(ISAXUtils.CreateiSAXSequenceBasedOnCardinality(timeseries, fullIsaxTree.get(i)).getIndexHash())
    }
    path
  }
  def persistInternalNode( word_length:Int,ts_len:Int,threshold:Int,indexhash:String,decendants:Iterable[String],nodeType:NodeType): Array[Byte]={
    val p = new IndexHashParams()
    p.base_card = 4
    p.d = 1
    p.isax_word_length =word_length
    p.orig_ts_len = ts_len
    p.threshold = threshold
    val s: Sequence = new Sequence (ts_len)
    // than a word len
    val n: InternalNodePersisted = new InternalNodePersisted (s, p, nodeType)
    val it=decendants.iterator

    while(it.hasNext){
      n.descendants.put(it.next(),null)
    }
    n.getBytes
  }
  def persistTerminalNode( word_length:Int,ts_len:Int,threshold:Int,indexhash:String,arInstances:Iterable[Timeseries]): Array[Byte]={
    val p = new IndexHashParams()
    p.base_card = 4
    p.d = 1
    p.isax_word_length =word_length
    p.orig_ts_len = ts_len
    p.threshold = threshold
    val s: Sequence = new Sequence (ts_len)
    s.parseFromIndexHash(indexhash)
    // than a word len
    val n: TerminalNodePersisted = new  TerminalNodePersisted(s,p)
    val it=arInstances.iterator

    while(it.hasNext){
      var tmp=it.next()
      //you can choose a element in input as a meterId/userId....
      n.arInstances.put(tmp.getMeter_id,new TimeseriesInstance(tmp))
    }

    n.getBytes
  }


}
