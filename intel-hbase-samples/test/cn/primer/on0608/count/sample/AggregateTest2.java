package cn.primer.on0608.count.sample;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionFactory;
import org.apache.hadoop.hbase.filter.ExpressionFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * -dir E:\Users\embracesource\workspace1\Intel_1\src -concurrent false
 * 
 * @author root
 * 
 */
public class AggregateTest2 {

	/**
	 * family:longStr2 中 0出现次数
	 * @throws Exception 
	 */
	@Test
	public void testCount1() throws Exception {
		
		Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "Test_Aggregate");
        String startKey = "";
        String endKey = "";
        

		// 1、选择出 列 family:longStr2 中 值为 0 的记录
		Expression exp = ExpressionFactory.eq(ExpressionFactory
				.toLong(ExpressionFactory.toString(ExpressionFactory
						.columnValue("family", "longStr2"))), ExpressionFactory
				.constant(Long.parseLong("0")));

		ExpressionFilter expressionFilter = new ExpressionFilter(exp);

		
		// 2、 根据时间段 为scan设置 startKey 和 endKey
		// Scan(byte[] startRow, Filter filter)

		// 3、对 这些记录 进行 count 操作

		Scan scan = new Scan();
		
		scan.setFilter(expressionFilter);
		
		
		int count = 0;
		ResultScanner scanner = table.getScanner(scan);
		Result r = scanner.next();
		System.out.println("====");
		while (r != null) {
			count++;
			System.out.println();
			System.out.println("第" + count + "条记录：" );
			System.out.println("r.getWritableSize()" + r.getWritableSize());
			System.out.println("r.size()" + r.size());
			System.out.println("r.r.getBytes()" + r.getBytes());
			System.out.println("r.getColumn(family.getBytes(), qualifier.getBytes()" + r.getColumn("family".getBytes(), "compositeLongStr".getBytes()));
			//Method for retrieving the row key that corresponds to the row from which this Result was created.
			System.out.println("r.getRow()" + r.getRow());
			//Returns the value of the first column in the Result.
			System.out.println("r.value()" + r.value());
//			System.out.println("r." + r);
			System.out.println(r);
			
			r = scanner.next();
		}
		System.out.println("符合要求的记录数量： " + count);
	}

	public final static String CONFIG_DIR_KEY = "dir";
	public final static String FILE_SUFFIX = "count1";

	public static void main(String[] args) {
		File currentDir = new File(".");
		System.out.println(currentDir.getAbsolutePath());
		Properties props = parseArgs(args);

		// 获得当前工程claspath 跟路径目录
		File dir = new File(props.getProperty(CONFIG_DIR_KEY));

		File[] fileList = null;
		if (dir.isDirectory()) {
			fileList = dir.listFiles(new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					if (pathname.isFile()
							&& pathname.toString().endsWith(FILE_SUFFIX))
						return true;
					return false;
				}
			});
		} else {
			System.out.println("The diretory " + dir
					+ " does NOT exist. Please have a check first.");
			return;
		}

		boolean isConcurrent = props.getProperty("concurrent")
				.equalsIgnoreCase("true");
		for (File f : fileList) {
			if (isConcurrent) {
				new AggregateOperationRunner(f).start();
			} else {
				new AggregateOperationRunner(f).run();
			}
		}
	}

	public static Properties parseArgs(String[] args) {
		Properties props = new Properties();
		// parse arguments
		int argindex = 0;

		if (args.length != 4) {
			System.out.println("Args' length should be 4.");
			usageMessage();
			System.exit(0);
		}

		if (args[argindex].equalsIgnoreCase("-dir")) {
			argindex++;
			props.setProperty("dir", args[argindex++]);
		}

		if (args[argindex].equalsIgnoreCase("-concurrent")) {
			argindex++;
			props.setProperty("concurrent", args[argindex++]);
		}

		if (argindex != args.length) {
			usageMessage();
			System.exit(0);
		}

		return props;
	}

	public static void usageMessage() {
		System.out.println("Usage: Engine -dir dir -concurrent true/false ");
	}

}
