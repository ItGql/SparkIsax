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

package cn.edu.fudan.mmdb.hbase.isax.index.shell;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.fudan.mmdb.isax.index.HashTreeException;
import cn.edu.fudan.mmdb.isax.index.TimeseriesInstance;
import cn.edu.fudan.mmdb.hbase.isax.index.HBaseUtils;
import cn.edu.fudan.mmdb.hbase.isax.index.ISAXIndex;
import cn.edu.fudan.mmdb.hbase.isax.index.TestISAXIndex;
import cn.edu.fudan.mmdb.hbase.isax.index.adapters.GenomePatternOccurrences;
import cn.edu.fudan.mmdb.hbase.isax.index.adapters.GenomicIndex;
import cn.edu.fudan.mmdb.hbase.isax.index.adapters.TestGenomicIndex;
import cn.edu.fudan.mmdb.hbase.isax.util.ReadTimeSeries;

public class Shell {
	private static final Logger logger = LoggerFactory.getLogger(Shell.class);
	public void printCreateIndexHelp() {
		System.out.println("usage: Shell CreateIndex [table_name] <options>");
		System.out.println("\tRequired Options:");
		System.out.println("\t\t-base_card <card>");
		System.out.println("\t\t-dim_split <dim_split>");
		System.out.println("\t\t-base_word_len <len>");
		System.out.println("\t\t-base_ts_sample_size <size>");
		System.out.println("\t\t-split_threshold <th>");
	}

	public void CreateNewIndex(String[] args) {
		// String table_name = argv[i++];
		String strBaseCard = "4";
		String strDimPerSplit = "1";
		String strBaseWordLen = "4"; // has to be specified?
		String strBaseTSSampleSize = "16"; // has to be specified
		String strSplitThreshold = "100";
		if (args.length != 12) {
			// no table name
			printCreateIndexHelp();
			return;
		}
		String table_name = args[1];
		for (int x = 0; x < 12; x++) {
			String param = args[x];
			if ("-base_card".equals(param)) {
				strBaseCard = args[x + 1];
			} else if ("-dim_split".equals(param)) {
				strDimPerSplit = args[x + 1];
			} else if ("-base_word_len".equals(param)) {
				strBaseWordLen = args[x + 1];
			} else if ("-base_ts_sample_size".equals(param)) {
				strBaseTSSampleSize = args[x + 1];
			} else if ("-split_threshold".equals(param)) {
				strSplitThreshold = args[x + 1];
			}
		}
		// need to map all of the parts to params
		ISAXIndex.CreateNewIndex(table_name, strBaseCard, strDimPerSplit, strBaseWordLen, strBaseTSSampleSize,
				strSplitThreshold);
//		System.out.println(table_name + ", " + strBaseCard + ", " + strDimPerSplit + ", " + strBaseWordLen + ", "
//				+ strBaseTSSampleSize + ", " + strSplitThreshold);
		logger.info(table_name + ", " + strBaseCard + ", " + strDimPerSplit + ", " + strBaseWordLen + ", "
				+ strBaseTSSampleSize + ", " + strSplitThreshold);
	}

	public void printMainHelp() {
		System.out.println("usage: Shell [MainCommand] <options>");
		System.out.println("\tCommands");
		System.out.println("\t\tCreateIndex // type 'Shell CreateIndex' for param list");
		System.out.println("\t\tDeleteIndex <index_name>");
		System.out.println("\t\tDebugIndex <index_name>");
		System.out.println("\t\tListTables");
		System.out.println("\t\tRunQuickTests <index_name>");
		System.out.println("\t\tIndexDNASample <index_name>");
		System.out.println("\t\tIndexGenomeFile <index_name> <genome_file>");
		System.out.println("\t\tSearchGenomeIndex <index_name> <dna_sequence>");
	}

	public void run(String[] args) {
		if (args.length < 1) {
			this.printMainHelp();
		} else {
			int i = 0;
			String cmd = args[0];
			if ("CreateIndex".equals(cmd)) {
				this.CreateNewIndex(args);
			} else if ("DeleteIndex".equals(cmd)) {
				if (args.length < 2) {
					this.printMainHelp();
					return;
				}
				String table_name = args[++i];
				ISAXIndex.DeleteIndex(table_name);
			} else if ("DebugIndex".equals(cmd)) {
				if (args.length < 2) {
					this.printMainHelp();
					return;
				}
				String table_name = args[++i];
				ISAXIndex.DebugIndex(table_name);
			} else if ("ListTables".equals(cmd)) {
				try {
					HBaseUtils.ListTables();
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else if ("IndexDNASample".equals(cmd)) {
				if (args.length < 2) {
					this.printMainHelp();
					return;
				}
				String table_name = args[++i];
				System.out.println("Quick DNA Index Test: " + table_name);
				TestGenomicIndex.testDNASegmentInsertAndQuery(table_name);
			} else if ("IndexGenomeFile".equals(cmd)) {
				if (args.length < 3) {
					this.printMainHelp();
					return;
				}
				String table_name = args[++i];
				String genome_file = args[++i];
				System.out.println("Indexing genome file" + genome_file + " into index " + table_name);
				// TestGenomicIndex.testDNASegmentInsertAndQuery( table_name );
				GenomicIndex.IndexGenomeFile(table_name, genome_file);
			} else if ("SearchGenomeIndex".equals(cmd)) {
				if (args.length < 3) {
					this.printMainHelp();
					return;
				}
				String table_name = args[++i];
				String dna_seq = args[++i];
				System.out.println("Searching for sequence '" + dna_seq + "' in index " + table_name);
				// TestGenomicIndex.testDNASegmentInsertAndQuery( table_name );
				// GenomicIndex.IndexGenomeFile(table_name, genome_file);
				GenomePatternOccurrences occur = GenomicIndex.ApproxSearchForDNASequence(table_name, dna_seq);
				if (null == occur) {
					System.out.println(
							"No DNA sequences were found that were similar to your query pattern of: " + dna_seq);
				} else {
					System.out.println("\n\nFound:");
					// occur.Debug();
					occur.Print();
				}
			} else if ("RunQuickTests".equals(cmd)) {
				if (args.length < 4) {
					this.printMainHelp();
					return;
				}
				String table_name = args[++i];
				String insertCountFlag = args[++i];
				if (insertCountFlag.equals("-n") == false) {
					this.printMainHelp();
					return;
				}
				// SAMPLE SIZE should be pulled from the index itself.
				String count = args[++i];
				int iCount = Integer.parseInt(count);
				System.out.println("Quick Test: " + table_name + " with " + iCount + " inserts...");
				TestISAXIndex.RandomInsertAndSearchTest(table_name, iCount);
			}
		}
	}

	/**
	 * 
	 * Usage:
	 * 
	 * createIndex -
	 * 
	 * deleteIndex
	 * 
	 * listTables
	 * 
	 * runIndexTest
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		HBaseUtils.open();
		Shell shell = new Shell();
		String[] args1 = { "CreateIndex", ISAXIndex.TABLE_NAME, "-base_card", "4", "-dim_split", "1", "-base_word_len",
				"14", "-base_ts_sample_size", "364", "-split_threshold", "100" };
		shell.run(args1);
		
		logger.info(" ----- 加载时间序列数据到内存 -------");
		long start = System.currentTimeMillis();
		ReadTimeSeries rts = new ReadTimeSeries();
		List<TimeseriesInstance> tsiList = rts.ReadTimeSeriesFromFile("./data/result-made_no-pap_r-365_new.txt");
		long load = System.currentTimeMillis();
		logger.info("加载用时" + (load - start) + "ms," + (load - start) / 1000 + "s");
		
		int i = 0;
		ISAXIndex index = new ISAXIndex();
		index.LoadIndex(ISAXIndex.TABLE_NAME);
		int tsLength = tsiList.size();
		tsLength = ISAXIndex.INDEX_NUMBER;
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置日期格式
		DecimalFormat decimalFormat = new DecimalFormat("#.00");           //构造方法的字符格式这里如果小数不足2位,会以0补足.

		for (i = 1; i <= tsLength; i++) {
			TimeseriesInstance tsi = tsiList.get(i - 1);
			try {
				logger.info("索引第" + i + "条时间序列");
				index.InsertSequence(tsi);
			} catch (HashTreeException e) {
				e.printStackTrace();
			}
			if (i % 1000 == 0) {
				logger.info("已索引" + i + "条时间序列，完成" + decimalFormat.format(i * 100.0 / tsLength) + "%,当前时间为："
						+ df.format(new Date()));
			}
		}
		long finish = System.currentTimeMillis() - load;
		logger.info("插入索引用时" + finish + "ms," + decimalFormat.format(finish / 1000.0) + "s，");
		HBaseUtils.close();
	}
}
