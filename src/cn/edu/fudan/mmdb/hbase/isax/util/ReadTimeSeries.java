package cn.edu.fudan.mmdb.hbase.isax.util;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.fudan.mmdb.timeseries.TPoint;
import cn.edu.fudan.mmdb.timeseries.Timeseries;
import cn.edu.fudan.mmdb.hbase.isax.index.shell.Shell;
import cn.edu.fudan.mmdb.isax.index.TimeseriesInstance;

public class ReadTimeSeries {
	private static final Logger logger = LoggerFactory.getLogger(ReadTimeSeries.class);
	/**
	 * 从指定的文件中读取时间序列
	 * 
	 * @author FeiWang
	 */
	public void cleanData(String filePathAndName) {
		FileReader fr = null;
		BufferedReader br = null;
		BufferedWriter bw = null;
		File file = null;
		try {
			fr = new FileReader(new File(filePathAndName));
			br = new BufferedReader(fr);
			file = new File("./data/result-made_no-pap_r-365_new.txt");
			bw = new BufferedWriter(new FileWriter(file));
			if (!file.exists() != false) {
				try {
					file.createNewFile();
					System.out.println("创建新的文件data/result-made_no-pap_r-365_new.txt成功!");
				} catch (IOException e) {
					System.out.println("创建新的文件data/result-made_no-pap_r-365_new.txt失败!");
				}
			}
			String line = null;
			int i = 0;
			int j=0;
			int k=0;
			double diff=0;            //用户每天的用电量，即有一天减去前一天的
			boolean isNegative=false; //判断用户用电量是否为负
			double sum=0;             //用于判断用户用电量是否为全0，由于精度的问题（有些可能不是全0，有小数在里面）
			DecimalFormat df = new DecimalFormat("#####0.000");//保留三位小数
			while ((line = br.readLine()) != null) {
				String[] valuesStr = line.split("\t");
				String output="";
				isNegative=false;
				sum=0;
				for (i = 2; i < 366; i++) {//过滤掉第一行，第一行的行号不保存。
					diff = Double.parseDouble(valuesStr[i]) - Double.parseDouble(valuesStr[i-1]);
					if(diff<0)  //用户用电量为负，不符合要求，过滤
					{
						isNegative=true;
						break;
					}
					sum+=diff;//用于判断用户用电量是否为全0，由于精度的问题（有些可能不是全0，有小数在里面）
					output+=df.format(diff)+"\t";
				}
				if(isNegative==false&&sum>=85.98){//用电量不为负，且一年总的用电量要大于1（原先是统计全0大于364天，但是由于精度问题，改用加法，统计 ）
					bw.write(output);//写入数据              //85.98是大约按照用电量大小排序的50000条记录的一个结果
					bw.flush();
					bw.newLine();    //换行
					j++;             //统计正确写入的数据量
				}
				if(j==300000)
					break;
				k++;//统计总的数据量
			}
			System.out.println("*****无错"+j+"行");
			System.out.println("*****总共"+k+"行");
			br.close();
			bw.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 从指定的文件中读取时间序列，,该函数主要用于操作"result-made_no-pap_r-365.txt"这个文件。
	 * @author FeiWang
	 */
	public List<TimeseriesInstance> ReadTimeSeriesFromFile(String filePathAndName) {
		List<TimeseriesInstance> tsiList = null;
		FileReader fr = null;
		BufferedReader br = null;
		tsiList = new ArrayList<TimeseriesInstance>();
		try {
			fr = new FileReader(new File(filePathAndName));
			br = new BufferedReader(fr);
			tsiList = new ArrayList<TimeseriesInstance>();
			String line = null;
			int i = 0;
			while ((line = br.readLine()) != null) {
				String[] valuesStr = line.split("\t");
				// 一共有364列,每行代表一个电表一年364天的用电量
				double[] values = new double[valuesStr.length];
				Timeseries ts = new Timeseries();
				for (i = 0; i < valuesStr.length; i++) {
					values[i] = Double.parseDouble(valuesStr[i]);
					ts.add(new TPoint(values[i], i));
				}
				TimeseriesInstance tsi = new TimeseriesInstance(ts);
				tsiList.add(tsi);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tsiList;
	}
	/**
	 * 从指定的文件中读取时间序列,该函数主要用于操作"shiqu_pap_Completed.txt"这个文件。
	 * @author FeiWang
	 */
	public List<TimeseriesInstance> ReadTimeSeriesFromFile2(String filePathAndName) {
		List<TimeseriesInstance> tsiList = null;
		try {
			FileReader fr = new FileReader(new File(filePathAndName));
			BufferedReader br = new BufferedReader(fr);
			tsiList = new ArrayList<TimeseriesInstance>();
			String line = null;
			int i = 0;
			while ((line = br.readLine()) != null) {
				String[] valuesStr = line.split("\t");
				// 一共有367列,前两个值是电表编号之类，剩下365列为了建立ISA索引的方便，选取前360列
				// 也就是跳过前7个数
				double[] values = new double[valuesStr.length - 7];
				Timeseries ts = new Timeseries();
				for (i = 7; i < valuesStr.length; i++) {
					values[i - 7] = Double.parseDouble(valuesStr[i]);
					ts.add(new TPoint(values[i - 7], i - 7));
				}
				TimeseriesInstance tsi = new TimeseriesInstance(ts);
				tsiList.add(tsi);
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tsiList;
	}
	/**
	 * 主函数主要用于测试
	 */
	public static void main(String[] args) {
		ReadTimeSeries rts = new ReadTimeSeries();
		logger.info(" ----- 开始清洗数据-------");
		long start = System.currentTimeMillis();
		rts.cleanData("./data/result-made_no-pap_r-365.txt");
		long load = System.currentTimeMillis();
		logger.info("清洗数据用时" + (load - start) + "ms," + (load - start) / 1000 + "s");

		logger.info(" ----- 加载时间序列数据到内存 -------");
		long start2 = System.currentTimeMillis();
		List<TimeseriesInstance> tsiList = rts.ReadTimeSeriesFromFile("./data/result-made_no-pap_r-365_new.txt");

		long load2 = System.currentTimeMillis();
		logger.info("加载用时" + (load2 - start2) + "ms," + (load2 - start2) / 1000 + "s");
		//		int i = 0;
//		int j = 0;
//		System.out.println(tsiList.size());
//		for (i = 0; i < tsiList.size(); i++) {
//			TimeseriesInstance tsi = tsiList.get(i);
//			Timeseries ts = tsi.getTS();
//			System.out.println(ts.size());
//			double[] valuesDou = ts.values();
//			for (j = 0; j < valuesDou.length; j++) {
//				System.out.print(valuesDou[j] + " ");
//			}
//			if(valuesDou.length==364)
//				j++;
//		}
//		System.out.println(j);
	}
}