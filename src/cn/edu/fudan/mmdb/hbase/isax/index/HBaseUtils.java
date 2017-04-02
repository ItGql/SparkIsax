/**
   Copyright [2011] [Josh Patterson]

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

 */

package cn.edu.fudan.mmdb.hbase.isax.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.fudan.mmdb.isax.index.TimeseriesInstance;
import cn.edu.fudan.mmdb.hbase.isax.index.shell.Shell;
import cn.edu.fudan.mmdb.hbase.isax.util.ReadTimeSeries;

/**
 * Set of Utils to make working with HBase specific operations easier.
 * 
 * - Speed improvements, allow for caching of config?
 * 
 * 
 * @author jpatterson
 *
 */
public class HBaseUtils {
	private static Configuration config = null;
	private static HConnection conn = null;
	private static HBaseAdmin admin = null;
	private static HTableInterface hbase_table = null;
	public byte[][] a = null;
	private static final Logger logger = LoggerFactory.getLogger(HBaseUtils.class);
	/**
	 * 连接HBase
	 */
	public static void open() {
		try {
			config = HBaseConfiguration.create();
			conn = HConnectionManager.createConnection(config);
			admin = new HBaseAdmin(conn);
			hbase_table = conn.getTable(ISAXIndex.TABLE_NAME);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建表
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static boolean CreateNewTable(String TableName) throws IOException {
		// HTable hbase_table = new HTable(config, TableName);
		// Get get_cell = new Get( Bytes.toBytes(key));
		// Result result = hbase_table.get(get_cell);
		HTableDescriptor table_desc = new HTableDescriptor(TableName);
		HColumnDescriptor col_family = new HColumnDescriptor("node");
		col_family.setMaxVersions(1); // 设置数据保存的最大版本数
		table_desc.addFamily(col_family);
		admin.createTable(table_desc);
		// hbase_table.close();
		// return result.value();
		return true;
	}

	/**
	 * 判断是否包含表
	 * 
	 * @throws IOException
	 */
	public static boolean DoesTableExist(String TableName) throws IOException {
		// HTable hbase_table = new HTable(config, TableName);
		// Get get_cell = new Get( Bytes.toBytes(key));
		// Result result = hbase_table.get(get_cell);
		// hbase_table.close();
		return admin.tableExists(TableName);
		// return result.value();
		// return false;
	}

	/**
	 * 删除表
	 * 
	 * @throws IOException
	 */
	public static boolean DropTable(String TableName) throws IOException {
		admin.disableTable(Bytes.toBytes(TableName));
		admin.deleteTable(TableName);
		return true;
	}

	/**
	 * 罗列HBase中的所有表
	 * 
	 * @throws IOException
	 */
	public static void ListTables() throws IOException {
		System.out.println("List HBase Tables:");
		if (null == admin) {
			System.out.println("Can't Access HBase, quitting...");
		}
		HTableDescriptor[] tables = admin.listTables();
		for (int x = 0; x < tables.length; x++) {
			System.out.println("\tTable: " + tables[x].getNameAsString());
		}
	}

	/**
	 * 关闭HBase
	 */
	public static void close() {
		try {
			config.clear();
			conn.close();
			admin.close();
			hbase_table.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 将单条记录添加到hbase_table表中
	 * 
	 * @throws IOException
	 */public static void Put(String key, String TableName, String ColumnFamily, String ColumnName, byte[] value)
			throws IOException {
		Put put1 = new Put(Bytes.toBytes(key));

		put1.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName), value);
		hbase_table = conn.getTable(ISAXIndex.TABLE_NAME);
		hbase_table.put(put1);
	}

	/**
	 * 获取单张表的单条记录
	 * 
	 * @throws IOException
	 */
	public static byte[] Get(String key, String TableName, String ColumnFamily, String ColumnName) throws IOException {
		Get get_cell = new Get(Bytes.toBytes(key));
		get_cell.addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName));
		get_cell.setMaxVersions(1);
		get_cell.setCacheBlocks(false);
		Result result = hbase_table.get(get_cell);
		return result.value();
	}

	/**
	 * 获取单张表的所有记录
	 * 
	 * @throws IOException
	 */
	public static Map<byte[], byte[]> getAll(String TableName, String ColumnFamily, String ColumnName)
			throws IOException {
		Map<byte[], byte[]> tableContent = new HashMap<byte[], byte[]>();
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName));
		s.setMaxVersions(1);
		s.setCacheBlocks(false);
		ResultScanner rs = hbase_table.getScanner(s);
		for (Result r : rs) {
			byte[] key = r.getRow();
			byte[] value = r.getValue(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName));
			tableContent.put(key, value);
		}
		rs.close();
		return tableContent;
	}
	
	/**
	 * 拷贝表
	 * 
	 * @throws IOException
	 */
	public static void copyTable(String oldTableName, String newTableName,String ColumnFamily, String ColumnName)throws IOException {
		if(CreateNewTable(newTableName))
			logger.info("创建表"+newTableName+"表成功");
		else{
			logger.info("创建表"+newTableName+"表失败");
		}
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName));
		s.setMaxVersions(1);
		s.setCacheBlocks(false);
		ResultScanner rs = hbase_table.getScanner(s);
		
		HTableInterface hbase_table_new = conn.getTable(newTableName);
		for (Result r : rs) {
			byte[] key = r.getRow();
			byte[] value = r.getValue(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName));
			Put put = new Put(key);
			put.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes(ColumnName), value);
			hbase_table_new.put(put);
		}
		rs.close();
		hbase_table_new.close();
	}

	/**
	 * 删除一条记录
	 * 
	 * @throws IOException
	 */
	public static void DeleteRow(String key, String TableName) throws IOException {
		Delete d = new Delete(Bytes.toBytes(key));
		hbase_table.delete(d);
	}

	/**
	 * 用于测试的主函数
	 */
	public static void main(String[] args) {
		try {
			open();
			DropTable(ISAXIndex.TABLE_NAME);
//			logger.info(" ----- 开始拷贝表-------");
//			long start = System.currentTimeMillis();
//			copyTable(ISAXIndex.TABLE_NAME, ISAXIndex.TABLE_NAME_NEW,ISAXIndex.COLUMN_FAMILY_NAME, ISAXIndex.STORE_COL_NAME);
//			long end = System.currentTimeMillis();
//			logger.info(" ----- 拷贝表结束-------");
//			logger.info("拷贝用时" + (end - start) + "ms," + (end - start) / 1000.0 + "s");
			close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}