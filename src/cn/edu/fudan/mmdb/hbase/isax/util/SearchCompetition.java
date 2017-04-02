package cn.edu.fudan.mmdb.hbase.isax.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.fudan.mmdb.timeseries.TPoint;
import cn.edu.fudan.mmdb.timeseries.TSException;
import cn.edu.fudan.mmdb.timeseries.Timeseries;
import cn.edu.fudan.mmdb.isax.index.TimeseriesInstance;
import cn.edu.fudan.mmdb.distance.EuclideanDistance;
import cn.edu.fudan.mmdb.hbase.isax.index.HBaseUtils;
import cn.edu.fudan.mmdb.hbase.isax.index.ISAXIndex;

public class SearchCompetition {
	private static final Logger logger = LoggerFactory.getLogger(SearchCompetition.class);
	public static List<TimeseriesInstance> tsiList = null;

	/**
	 * 从指定的文件中读取时间序列
	 * 
	 * @author FeiWang
	 */
	public void LocalSearch() {
		try {
			logger.info("**************************************");
			long start = System.currentTimeMillis();
			int k = 0;
			int j = 0;
			int success=0;
			// 这里选取每一百条记录中的第46以及96条记录
			for (k = 5000; k < ISAXIndex.INDEX_NUMBER; k = k + 20000) {
				TimeseriesInstance tsi = tsiList.get(k);
				Timeseries ts = tsi.getTS();
				FileReader fr = new FileReader(new File("./data/result-made_no-pap_r-365_new.txt"));
				BufferedReader br = new BufferedReader(fr);
				String line = null;
				int i = 0;
				while ((line = br.readLine()) != null) {
					String[] valuesStr = line.split("\t");
					double[] values = new double[valuesStr.length];
					Timeseries currentTimeSeries = new Timeseries();
					for (i = 0; i < valuesStr.length; i++) {
						values[i] = Double.parseDouble(valuesStr[i]);
						if(i==300)
							values[i]+=0.1;//让数据变化一点儿
						currentTimeSeries.add(new TPoint(values[i], i));
					}
					double cur_dist = EuclideanDistance.seriesDistance(ts.values(), currentTimeSeries.values());
					//低于指定阈值停止搜索
					if (cur_dist < 1){
						success++;
						break;
					}
				}
				j++;
				br.close();
			}
			long load = System.currentTimeMillis();
			logger.info("在本地顺序搜索" + j + "次用时" + (load - start) + "ms," + (load - start) / 1000.0 + "s");
			logger.info("平均搜索时间" + j + "次用时" + 1.0 * (load - start) / j + "ms," + (load - start) / (1000.0 * j) + "s");
			logger.info("成功" + success + "次");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 从指定的文件中读取时间序列
	 * 
	 * @author FeiWang
	 */
	public void LocalSearch2() {
		ReadTimeSeries rts = new ReadTimeSeries();
		List<TimeseriesInstance> tsiLocalList = rts.ReadTimeSeriesFromFile("./data/result-made_no-pap_r-365_new.txt");
		try {
			logger.info("**************************************");
			long start = System.currentTimeMillis();
			int i=0;
			int k = 0;
			int j = 0;
			int success=0;
			int size=tsiLocalList.size();
			// 这里选取每一百条记录中的第46以及96条记录
			for (k = 5000; k < ISAXIndex.INDEX_NUMBER; k = k + 20000) {
				TimeseriesInstance tsi = tsiList.get(k);
				Timeseries ts = tsi.getTS();
				double[] tsDouble=ts.values();
				tsDouble[300]+=0.1;//让数据变化一点儿
				Timeseries tsNew=new Timeseries(tsDouble);;
				for(i=0;i<size;i++)
				{
					TimeseriesInstance tsiLocal = tsiLocalList.get(i);
					Timeseries tsLocal = tsiLocal.getTS();
					double cur_dist = EuclideanDistance.seriesDistance(tsNew.values(), tsLocal.values());
					//低于指定阈值停止搜索
					if (cur_dist < 1){
						success++;
						break;
					}
				}
				j++;
			}
			long load = System.currentTimeMillis();
			logger.info("在本地顺序搜索" + j + "次用时" + (load - start) + "ms," + (load - start) / 1000.0 + "s");
			logger.info("平均搜索时间" + j + "次用时" + 1.0 * (load - start) / j + "ms," + (load - start) / (1000.0 * j) + "s");
			logger.info("成功" + success + "次");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public void iSAXSearch() {
		// 连接HBase
		HBaseUtils.open();
		ISAXIndex index = new ISAXIndex();
		index.LoadIndex(ISAXIndex.TABLE_NAME);
		logger.info("**************************************");
		long start = System.currentTimeMillis();
		int i = 0;
		int j = 0;
		int success=0;
		// 这里选取每一百条记录中的第46以及96条记录
		for (i = 5000; i < ISAXIndex.INDEX_NUMBER; i = i + 10000) {
			TimeseriesInstance tsi = tsiList.get(i);
			Timeseries ts = tsi.getTS();
			double[] tsDouble=ts.values();
			tsDouble[300]+=0.1;//让数据变化一点儿
			Timeseries tsNew;
			try {
				tsNew = new Timeseries(tsDouble);
				TimeseriesInstance tsNew2 = index.ApproxSearch(tsNew);
				if(tsNew2!=null){
					double cur_dist = EuclideanDistance.seriesDistance(tsNew2.getTS().values(), tsNew.values());
					//低于指定阈值停止搜索
					if (cur_dist < 1){
						success++;
					}
				}
			} catch (TSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			j++;
		}
		long load = System.currentTimeMillis();
		logger.info("在表" + ISAXIndex.TABLE_NAME + "上");
		logger.info("成功" + success + "次");
		logger.info("搜索" + j + "次用时" + (load - start) + "ms," + (load - start) / 1000.0 + "s");
		logger.info("平均搜索时间" + j + "次用时" + 1.0 * (load - start) / j + "ms," + (load - start) / (1000.0 * j) + "s");
		HBaseUtils.close();
	}

	/**
	 * 主函数主要用于测试
	 */
	public static void main(String[] args) {
		logger.info(" ----- 加载时间序列数据到内存 -------");
		long start = System.currentTimeMillis();
		ReadTimeSeries rts = new ReadTimeSeries();
		tsiList = rts.ReadTimeSeriesFromFile("./data/result-made_no-pap_r-365_new.txt");
		long load = System.currentTimeMillis();
		logger.info("加载用时" + (load - start) + "ms," + (load - start) / 1000.0 + "s");
		// int i = 0;
		// int j = 0;
		// System.out.println(tsiList.size());
		// for (i = 0; i < tsiList.size(); i++) {
		// TimeseriesInstance tsi = tsiList.get(i);
		// Timeseries ts = tsi.getTS();
		// }
		// System.out.println(j);

		SearchCompetition sc = new SearchCompetition();
//		sc.iSAXSearch();
		sc.LocalSearch2();
	}
}
