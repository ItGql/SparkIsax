package cn.edu.fudan.mmdb.hbase.isax.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.fudan.mmdb.isax.Sequence;
import cn.edu.fudan.mmdb.isax.index.NodeType;
import cn.edu.fudan.mmdb.isax.index.TimeseriesInstance;
import cn.edu.fudan.mmdb.hbase.isax.index.HBaseUtils;
import cn.edu.fudan.mmdb.hbase.isax.index.ISAXIndex;
import cn.edu.fudan.mmdb.hbase.isax.index.InternalNodePersisted;
import cn.edu.fudan.mmdb.hbase.isax.index.NodePersisted;

/**
 * 统计索引节点的具体情况，用于验证索引建立的正确性，以及判断索引建立的好坏
 * 
 * @author FeiWang
 */
public class NodeStatistics {
	private static final Logger logger = LoggerFactory.getLogger(NodeStatistics.class);
	public List<Integer> tsNum = null;

	/**
	 * 从HBase表顺序遍历统计索引节点
	 */
	public void NodeStatisticsInOrder() {
		try {
			// 连接HBase
			HBaseUtils.open();
			// 获取所有的索引节点，包括其内容，key为索引的iSAX表达(root的表达为"root_node_00"),value是对象序列化后的byte数组
			Map<byte[], byte[]> tableContent = HBaseUtils.getAll(ISAXIndex.TABLE_NAME, ISAXIndex.COLUMN_FAMILY_NAME,
					ISAXIndex.STORE_COL_NAME);
			HBaseUtils.close();
//			tsNum = new ArrayList<Integer>();
			// 索引节点数目
			logger.info("*******************************************");
			logger.info("顺序统计结果如下:");
			logger.info("一共有" + tableContent.size() + "个索引节点(直接从HBase表中读出的节点数目，一行是一个节点)");
			
			int i = 0;// 根节点
			int j = 0;// 中间节点
			int k = 0;// 终端节点
			int rCount = 0;// 根节点指向节点数目
			int iCount = 0;// 中间节点指向节点数目
			int tCount = 0;// 终端节点指向时间序列条目数
			for (Map.Entry<byte[], byte[]> entry : tableContent.entrySet()) {
				String isax_hash = new String(entry.getKey());// key为索引的iSAX表达(root的表达为"root_node_00")
				byte[] node_bytes = entry.getValue(); // value是对象序列化后的byte数组
				NodePersisted hbase_node = null;
				if (isax_hash.equals("root_node_00")) {// 节点为root
					InternalNodePersisted root = new InternalNodePersisted();
					root.key = new Sequence(0);
					root.deserialize(node_bytes);
					root.key.setOrigLength(root.params.orig_ts_len);
					Set<String> keys = root.descendants.keySet();// 根节点的指向，他的后继
					rCount += keys.size();
					i++;
				} else {
					hbase_node = NodePersisted.deserialize_unknown(isax_hash, node_bytes);
					if (hbase_node.getType() == NodeType.INTERNAL) {// 节点为InternalNode，中间节点
						InternalNodePersisted internal_node = new InternalNodePersisted(hbase_node.key,
								hbase_node.params, NodeType.INTERNAL);
						internal_node.deserialize(node_bytes);
						Set<String> keys = internal_node.descendants.keySet();
						iCount += keys.size();// 具体的中间节点存储的指向后继节点的条目数
						j++;
					} else if (hbase_node.getType() == NodeType.TERMINAL) {// 节点为TERMINALNode，终端节点
						Map<String, TimeseriesInstance> a = hbase_node.getNodeInstances();
//						tsNum.add(a.size());// 具体的终端节点存储的时间序列的条目数
						tCount += a.size();
						k++;
					}
				}
			}
			logger.info("根节点=" + i);
			logger.info("中间节点=" + j);
			logger.info("终端节点=" + k);
			logger.info("一共有" + (i + j + k) + "个索引节点(根节点、中间节点和终端节点相加得到的数目)");// 可以与读入的节点数目作比较
			logger.info("根节点指向" + rCount + "个节点");
			logger.info("中间节点指向" + iCount + "个节点");
			logger.info("终端节点指向" + tCount + "个节点(即:实际索引树指向的时间序列条目数)");
			logger.info("索引树的扇出=" + (rCount + iCount) + ",+1(根节点)=索引树的节点数目" + (i + j + k));
//			for (i = 1; i <= tsNum.size(); i++) {
//				logger.info("第" + i + "终端个节点存储的时间序列条目数=" + tsNum.get(i - 1));
//			}
			logger.info("*******************************************");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 从HBase表层次遍历统计索引节点
	 */
	public void NodeStatisticsInLevel() {
		try {
			// 连接HBase
			HBaseUtils.open();
			Queue<String> queue = new LinkedList<String>();
//			tsNum = new ArrayList<Integer>();
			// 索引节点数目
			logger.info("\n\n###########################################");
			logger.info("层次遍历统计结果如下:");
			int i = 0;// 根节点
			int j = 0;// 中间节点
			int levelJ=0;//每一层的中间节点数目
			int k = 0;// 终端节点
			int levelK=0;//每一层的终端节点数目
			int rCount = 0;// 根节点指向节点数目
			int iCount = 0;// 中间节点指向节点数目
			int tCount = 0;// 终端节点指向时间序列条目数
			int levelT = 0;// 每一层终端节点存储的时间序列条目数
			queue.offer("root_node_00");// 根节点入队
			int level = 0;// 节点层次
			while (!queue.isEmpty()) {
				level++;
				levelJ=0;//每一层的中间节点数目
				levelK=0;//每一层的终端节点数目
				logger.info("*********************");
				logger.info("索引树第" + level + "层");
				// 存放当前层的所有节点，便于统计树的层次
				List<String> list = new ArrayList<String>();
				while (!queue.isEmpty()) {
					list.add(queue.poll());// 出队
				}
				int m = 0;// 用于遍历当前层次的节点
				levelT = 0;//// 每一层终端节点存储的时间序列条目数初始化为0
				String output="";
				for (m = 0; m < list.size(); m++) {
					String isax_hash = list.get(m);
					byte[] node_bytes = HBaseUtils.Get(isax_hash, ISAXIndex.TABLE_NAME, ISAXIndex.COLUMN_FAMILY_NAME,
							ISAXIndex.STORE_COL_NAME);// node_bytes是对象序列化后的byte数组
					if (isax_hash.equals("root_node_00")) {// 节点为root
						InternalNodePersisted root = new InternalNodePersisted();
						root.key = new Sequence(0);
						root.deserialize(node_bytes);
						root.key.setOrigLength(root.params.orig_ts_len);
						Set<String> keys = root.descendants.keySet();// 根节点的指向，他的后继
						for (String key : keys)// 将根节点指向的节点的iSAX表达添加到队列中
							queue.offer(key);
						rCount += keys.size();
						i++;
					} else {
						NodePersisted hbase_node = NodePersisted.deserialize_unknown(isax_hash, node_bytes);
						if (hbase_node.getType() == NodeType.INTERNAL) {// 节点为InternalNode，中间节点
							InternalNodePersisted internal_node = new InternalNodePersisted(hbase_node.key,
									hbase_node.params, NodeType.INTERNAL);
							internal_node.deserialize(node_bytes);
							Set<String> keys = internal_node.descendants.keySet();
							for (String key : keys)// 将中间节点指向的节点的iSAX表达添加到队列中
								queue.offer(key);
							iCount += keys.size();// 具体的中间节点存储的指向后继节点的条目数
							levelJ++;
						} else if (hbase_node.getType() == NodeType.TERMINAL) {// 节点为TERMINALNode，终端节点
							Map<String, TimeseriesInstance> a = hbase_node.getNodeInstances();
//							tsNum.add(a.size());// 具体的终端节点存储的时间序列的条目数
							tCount += a.size();
							levelT += a.size();
//							output+=a.size() + " ";
							levelK++;
						}
					}
//					logger.info(output);
				}
				j+=levelJ;
				k+=levelK;
				logger.info("\n该层一共有" + levelJ + "个中间节点");
				logger.info("该层一共有" + levelK + "个终端节点");
				logger.info("该层一共存储" + levelT + "条时间序列");
				list.clear();
			}
			HBaseUtils.close();

			logger.info("索引树共" + level + "层");
			logger.info("根节点=" + i);
			logger.info("中间节点=" + j);
			logger.info("终端节点=" + k);
			logger.info("一共有" + (i + j + k) + "个索引节点(根节点、中间节点和终端节点相加得到的数目)");// 可以与读入的节点数目作比较
			logger.info("根节点指向" + rCount + "个节点");
			logger.info("中间节点指向" + iCount + "个节点");
			logger.info("终端节点指向" + tCount + "个节点(即:实际索引树指向的时间序列条目数)");
			logger.info("索引树的扇出=" + (rCount + iCount) + ",+1(根节点)=索引树的节点数目" + (i + j + k));
//			for (i = 1; i <= tsNum.size(); i++) {
//				logger.info("第" + i + "终端个节点存储的时间序列条目数=" + tsNum.get(i - 1));
//			}
			logger.info("###########################################");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		NodeStatistics ns = new NodeStatistics();
		//ns.NodeStatisticsInOrder();
		ns.NodeStatisticsInLevel();
	}
}
