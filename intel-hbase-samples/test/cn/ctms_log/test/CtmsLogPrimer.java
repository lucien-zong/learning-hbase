package cn.ctms_log.test;

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
public class CtmsLogPrimer {

	/**
	 * 统计目标中心 AD 的所有源中心的请求
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCountAD() throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "ctms_log");
		String startKey = "";
		String endKey = "";
		String startRow = "AD0";

		// String startRow =
		// "AD000AD0002013053107566766758344841720111AD000-1939372196";
		// String startRow =
		// "AD000AA0000000000000000000000000000000000AA000-0000000000";
		String stopRow = "AD9";
		// String stopRow = "AD9";

		// 1、选择出 列 family:longStr2 中 值为 0 的记录
		// Expression exp = ExpressionFactory.eq(ExpressionFactory
		// .toLong(ExpressionFactory.toString(ExpressionFactory
		// .columnValue("family", "longStr2"))), ExpressionFactory
		// .constant(Long.parseLong("0")));
		//
		// ExpressionFilter expressionFilter = new ExpressionFilter(exp);

		// 2、 根据时间段 为scan设置 startKey 和 endKey
		// Scan(byte[] startRow, Filter filter)

		// 3、对 这些记录 进行 count 操作

		Scan scan = new Scan();
		// scan.setStartRow(startRow.getBytes());
		// scan.setStopRow(stopRow.getBytes());

		// scan.setFilter(expressionFilter);

		int count = 0;
		ResultScanner scanner = table.getScanner(scan);
		Result r = scanner.next();
		System.out.println("====");
		while (r != null) {
			count++;
			// System.out.println();
			// System.out.println("第" + count + "条记录：" );
			// System.out.println("r.getWritableSize()" + r.getWritableSize());
			// System.out.println("r.size()\t" + r.size());//这一行的列的数量
			// System.out.println("r.r.getBytes()" + r.getBytes());
			// System.out.println("r.getColumn(family.getBytes(), qualifier.getBytes()"
			// + r.getColumn("family".getBytes(),
			// "compositeLongStr".getBytes()));
			// //Method for retrieving the row key that corresponds to the row
			// from which this Result was created.
			// System.out.println("r.getRow()" + r.getRow());
			// //Returns the value of the first column in the Result.
			// System.out.println("r.value()" + r.value());
			// System.out.println("r." + r);
			System.out.println(r);

			r = scanner.next();
		}
		System.out.println("符合要求的记录数量： " + count);
	}

	/**
	 * 查看表中有多少数据，count 'ctms_log' , 303000 条数据 数据中心为AD 有符合要求的记录数量： 1694
	 * 
	 * @throws Exception
	 */
	@Test
	public void testAD1() throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "ctms_log");
		String startKey = "";
		String endKey = "";
		String startRow = "AD0";
		// String startRow =
		// "AD000AD0002013053107566766758344841720111AD000-1939372196";
		// String startRow =
		// "AD000AA0000000000000000000000000000000000AA000-0000000000";
		String stopRow = "AD999";

		// 选出目标数据中心为 AD  记录
		Expression exp = ExpressionFactory.eq(ExpressionFactory
				.toString(ExpressionFactory.columnValue("f", "dest_center")),
				ExpressionFactory.toString(ExpressionFactory.constant("AD")));

		ExpressionFilter expressionFilter = new ExpressionFilter(exp);

		
		// 时间范围 value=2013/05/31 07:53:28.260
		// 2013/06/04 14:45:44.269

		// scan 'ctms_log',{COLUMNS => 'f:time_exec'}
		// Expression exp = ExpressionFactory.lt(ExpressionFactory
		// .toLong(ExpressionFactory.toString(ExpressionFactory
		// .columnValue("f", "time_exec"))), ExpressionFactory
		// .constant(Long.parseLong("0")));
		//
		// ExpressionFilter expressionFilter = new ExpressionFilter(exp);

		// 2、 根据时间段 为scan设置 startKey 和 endKey
		// Scan(byte[] startRow, Filter filter)

		// 3、对 这些记录 进行 count 操作

		Scan scan = new Scan();
		// scan.setStartRow(startRow.getBytes());
		// scan.setStopRow(stopRow.getBytes());

		scan.setFilter(expressionFilter);

		int count = 0;
		ResultScanner scanner = table.getScanner(scan);
		Result r = scanner.next();
		System.out.println("====");
		while (r != null) {
			count++;
			// System.out.println();
			System.out.println("第" + count + "条记录：");
			// System.out.println("r.getWritableSize()" + r.getWritableSize());
			// System.out.println("r.size()\t" + r.size());//这一行的列的数量
			// System.out.println("r.r.getBytes()" + r.getBytes());
			// System.out.println("r.getColumn(family.getBytes(), qualifier.getBytes()"
			// + r.getColumn("family".getBytes(),
			// "compositeLongStr".getBytes()));
			// //Method for retrieving the row key that corresponds to the row
			// from which this Result was created.
			// System.out.println("r.getRow()" + r.getRow());
			// //Returns the value of the first column in the Result.
			// System.out.println("r.value()" + r.value());
			// System.out.println("r." + r);
			System.out.println(r);

			r = scanner.next();
		}
		System.out.println("符合要求的记录数量： " + count);
	}

	/**
	 * 查看表中有多少数据，count 'ctms_log' , 303000 条数据
	 * 
	 * @throws Exception
	 */
	@Test
	public void testStartKey1() throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "Test_Aggregate");
		String startKey = "";
		String endKey = "";
		String startRow = "3,c0";

		// String startRow =
		// "AD000AD0002013053107566766758344841720111AD000-1939372196";
		// String startRow =
		// "AD000AA0000000000000000000000000000000000AA000-0000000000";
		String stopRow = "6";
		// String stopRow = "AD9";

		// 1、选择出 列 family:longStr2 中 值为 0 的记录
		// Expression exp = ExpressionFactory.eq(ExpressionFactory
		// .toLong(ExpressionFactory.toString(ExpressionFactory
		// .columnValue("family", "longStr2"))), ExpressionFactory
		// .constant(Long.parseLong("0")));
		//
		// ExpressionFilter expressionFilter = new ExpressionFilter(exp);

		// 时间范围 value=2013/05/31 07:53:28.260
		// 2013/06/04 14:45:44.269

		// scan 'ctms_log',{COLUMNS => 'f:time_exec'}
		// Expression exp = ExpressionFactory.lt(ExpressionFactory
		// .toLong(ExpressionFactory.toString(ExpressionFactory
		// .columnValue("f", "time_exec"))), ExpressionFactory
		// .constant(Long.parseLong("0")));
		//
		// ExpressionFilter expressionFilter = new ExpressionFilter(exp);

		// 2、 根据时间段 为scan设置 startKey 和 endKey
		// Scan(byte[] startRow, Filter filter)

		// 3、对 这些记录 进行 count 操作

		Scan scan = new Scan();
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());

		// scan.setFilter(expressionFilter);

		int count = 0;
		ResultScanner scanner = table.getScanner(scan);
		Result r = scanner.next();
		System.out.println("====");
		while (r != null) {
			count++;
			// System.out.println();
			System.out.println("第" + count + "条记录：");
			// System.out.println("r.getWritableSize()" + r.getWritableSize());
			// System.out.println("r.size()\t" + r.size());//这一行的列的数量
			// System.out.println("r.r.getBytes()" + r.getBytes());
			// System.out.println("r.getColumn(family.getBytes(), qualifier.getBytes()"
			// + r.getColumn("family".getBytes(),
			// "compositeLongStr".getBytes()));
			// //Method for retrieving the row key that corresponds to the row
			// from which this Result was created.
			// System.out.println("r.getRow()" + r.getRow());
			// //Returns the value of the first column in the Result.
			// System.out.println("r.value()" + r.value());
			// System.out.println("r." + r);
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
