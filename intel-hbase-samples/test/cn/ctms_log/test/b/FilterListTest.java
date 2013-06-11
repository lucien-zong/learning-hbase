package cn.ctms_log.test.b;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionFactory;
import org.apache.hadoop.hbase.filter.ExpressionFilter;
import org.apache.hadoop.hbase.filter.FilterList;

import com.intel.hbase.test.util.TimeCounter;

public class FilterListTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "ctms_log");

        TimeCounter executeTimer = new TimeCounter();
        executeTimer.begin();
        executeTimer.enter();
        Scan scan = new Scan();
        
        
        
        
        
        
        
      //1、 选出目标数据中心为 AD 记录
		Expression exp1 = ExpressionFactory.eq(ExpressionFactory
				.toString(ExpressionFactory.columnValue("f",
						"dest_center")), ExpressionFactory
				.toString(ExpressionFactory.constant("AD")));
		Expression exp2 = ExpressionFactory.eq(ExpressionFactory
				.toString(ExpressionFactory.columnValue("f",
						"source_center")), ExpressionFactory
				.toString(ExpressionFactory.constant("AD")));
		Expression exp3 = ExpressionFactory.eq(ExpressionFactory
				.toString(ExpressionFactory.columnValue("f",
						"source_center")), ExpressionFactory
				.toString(ExpressionFactory.constant("H5")));
		ExpressionFilter expressionFilter1 = new ExpressionFilter(exp1);
		ExpressionFilter expressionFilter2 = new ExpressionFilter(exp2);
		ExpressionFilter expressionFilter3 = new ExpressionFilter(exp3);
		scan.setFilter(expressionFilter1);
		scan.setFilter(expressionFilter2);
		scan.setFilter(expressionFilter3);
		
		
		FilterList fl = new FilterList(expressionFilter1,expressionFilter2);
        
        scan.setFilter(fl);
        int count = 0;
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();//keyvalues={0,c01/family:compositeLongStr/1370592062825/Put/vlen=5/ts=0, 0,c01/family:long/1370592062825/Put/vlen=8/ts=0, 0,c01/family:longStr1/1370592062825/Put/vlen=1/ts=0, 0,c01/family:longStr2/1370592062825/Put/vlen=1/ts=0, 0,c01/family:mod10Str/1370592062825/Put/vlen=1/ts=0}
        while (r != null) {
            count++;
            r = scanner.next();
        }
        System.out
                .println("++ Scanning finished with count : " + count + " ++");
        scanner.close();

        executeTimer.leave();
        executeTimer.end();
        System.out.println("++ Time cost for scanning: "
                + executeTimer.getTimeString() + " ++");
    }

}
