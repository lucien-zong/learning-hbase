package cn.ctms_log.taska;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.CompositeLongStrColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.GroupByClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongStrColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionFactory;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ExpressionFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.util.bloom.Filter;

import com.intel.hbase.test.util.TimeCounter;

public class CtmsCountAdRunner extends Thread {

	public final static String HBASE_ADDRESS_KEY = "hbase_address";
	public final static String TABLE_KEY = "table";
	public final static String ACTION_KEY = "action";
	public final static String COLUMNS_KEY = "columns";
	public final static String COLUMN_INTERPRETER_KEY = "interpreter";
	public final static String DELIM_KEY = "delim";
	public final static String INDEX_KEY = "index";
	public final static String RPC_TIMEOUT = "hbase.rpc.timeout";

	private File configFileName;
	private Properties props;

	public CtmsCountAdRunner(File file) {
		this.configFileName = file;
	}

	@Override
	public void run() {
		props = new Properties();
		try {
			InputStream in = new BufferedInputStream(new FileInputStream(
					configFileName));
			props.load(in);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("File " + configFileName
					+ " is NOT valid. Please have a check first.");
			return;
		}

		String address = props.getProperty(HBASE_ADDRESS_KEY);
		byte[] table = Bytes.toBytes(props.getProperty(TABLE_KEY));
		String action = props.getProperty(ACTION_KEY);
		String timeoutValue = props.getProperty(RPC_TIMEOUT, "120000");

		Configuration conf = HBaseConfiguration.create();
		try {
			ZKUtil.applyClusterKeyToConf(conf, address);
			long timeout = Long.parseLong(timeoutValue);
			conf.setLong("hbase.rpc.timeout", timeout);

			AggregationClient aClient = new AggregationClient(conf);

			Scan scan = new Scan();

			if (!configScan(scan))
				return;

			final ColumnInterpreter<Long, Long> columnInterpreter = getColumnInterpreter();
			if (columnInterpreter == null) {
				System.out
						.println("The value of 'interpreter' set in configuration file "
								+ configFileName
								+ " seems wrong."
								+ "Please have a check .");
				return;
			}

			try {

				TimeCounter executeTimer = new TimeCounter();
				executeTimer.begin();
				executeTimer.enter();

				// 1、 选出目标数据中心为 AD 记录
				Expression exp = ExpressionFactory.eq(ExpressionFactory
						.toString(ExpressionFactory.columnValue("f",
								"dest_center")), ExpressionFactory
						.toString(ExpressionFactory.constant("AD")));
				ExpressionFilter expressionFilter = new ExpressionFilter(exp);

				String familyName = "f";
				String columnName = "time_ctms";
				String startTime = "2013/05/31 07:56:07.323";
				String endTime = "2013/06/11 07:56:07.323";

				SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
						Bytes.toBytes(familyName), Bytes.toBytes(columnName),
						CompareOp.GREATER, Bytes.toBytes(startTime));
				SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
						Bytes.toBytes(familyName), Bytes.toBytes(columnName),
						CompareOp.LESS, Bytes.toBytes(endTime));

				FilterList fl = new FilterList(expressionFilter, filter1,
						filter2);

				scan.setFilter(fl);

				// ============================================================================

				// ============================================================================
				GroupByClient groupByClient = new GroupByClient(conf);
				Scan[] scans = { scan };

				// 2、分组

				List<Expression> groupByExpresstions = new ArrayList<Expression>();
				List<Expression> selectExpresstions = new ArrayList<Expression>();

				// 按源中心分组
				Expression key1 = ExpressionFactory.columnValue("f",
						"source_center");
				groupByExpresstions.add(key1);

				Expression select1 = ExpressionFactory
						.groupByKey(ExpressionFactory.columnValue("f",
								"source_center"));
				Expression select2 = ExpressionFactory.count(ExpressionFactory
						.columnValue("f", "dest_center"));
				Expression select3 = ExpressionFactory.count(ExpressionFactory
						.columnValue("f", "time_ctms"));
				selectExpresstions.add(select1);

				selectExpresstions.add(select2);
				// selectExpresstions.add(select3);

				List<EvaluationResult[]> groupByResultList = groupByClient
						.groupBy(table, scans, groupByExpresstions,
								selectExpresstions, null);
				System.out.println("\t目标中心为AD的统计结果：\n");
				System.out.println("\t\t源中心\t\t数量");
				int returnCount = 0;
				for (EvaluationResult[] res : groupByResultList) {
					String resultString = "";
					for (int i = 0; i < res.length; i++) {
						EvaluationResult er = res[i];
						resultString += "\t\t" + er.toString();
						if (0 == ((i + 1) % selectExpresstions.size())) {
							System.out.println(resultString);
							returnCount += Integer.parseInt(er.toString());
							resultString = "";
						}
					}
				}

				System.out.println();

				executeTimer.leave();
				executeTimer.end();
				System.out.println("++  [config -> " + configFileName
						+ "] \n++  查询耗时：" + executeTimer.getTimeString()
						+ " ++");
				System.out.println("++  返回数据总量： " + returnCount);

			} catch (Throwable e) {
				e.printStackTrace();
			}
		} catch (NumberFormatException e) {
			System.out.println("Invaild argument!");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private boolean configScan(Scan scan) {
		String columns = props.getProperty(COLUMNS_KEY);// conf.co:columns=family:mod10Str
		StringTokenizer st = new StringTokenizer(columns, ", ");
		while (st.hasMoreTokens()) {
			String column[] = st.nextToken().split(":");
			switch (column.length) {
			case 2:
				scan.addColumn(Bytes.toBytes(column[0]),
						Bytes.toBytes(column[1]));
				break;
			case 1:
				scan.addFamily(Bytes.toBytes(column[0]));
				break;
			default:
				System.out
						.println("The value of 'columns' set in configuration file "
								+ configFileName
								+ " seems incorrect. Please have a check first.");
				return false;
			}
		}
		return true;
	}

	private ColumnInterpreter<Long, Long> getColumnInterpreter() {
		String interpreter = props.getProperty(COLUMN_INTERPRETER_KEY);
		if (interpreter.equalsIgnoreCase("LongStr")) {
			return new LongStrColumnInterpreter();// column value string '12'
													// will be regarded by this
													// interpreter as 12 while
													// by LongColumnInterpreter
													// as 12594 (0x3132)
		} else if (interpreter.equalsIgnoreCase("CompositeLongStr")) {
			String delim = props.getProperty(DELIM_KEY, ",");
			int index;
			index = Integer.parseInt(props.getProperty(INDEX_KEY, "0"));
			return new CompositeLongStrColumnInterpreter(delim, index);
		} else if (interpreter.equalsIgnoreCase("Long")) {
			return new LongColumnInterpreter();
		} else
			return null;

	}

}
