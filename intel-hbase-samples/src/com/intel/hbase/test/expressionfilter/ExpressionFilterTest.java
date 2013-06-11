package com.intel.hbase.test.expressionfilter;

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

import com.intel.hbase.test.util.TimeCounter;

public class ExpressionFilterTest {

    public static void main(String[] args) throws Exception {
    	System.out.println("kkkk");
        if (args.length < 2) {
            throw new Exception("Table name not specified.");
        }
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, args[0]);
        String startKey = args[1];

        TimeCounter executeTimer = new TimeCounter();
        executeTimer.begin();
        executeTimer.enter();
        
        //exp含义： 过滤条件： family：longStr2 列 值为 2的 
        Expression exp = ExpressionFactory.eq(ExpressionFactory
                .toLong(ExpressionFactory.toString(ExpressionFactory
                        .columnValue("family", "longStr2"))), ExpressionFactory
//                .constant(Long.parseLong("99")));
        .constant(Long.parseLong("3")));
        ExpressionFilter expressionFilter = new ExpressionFilter(exp);
        //Scan(byte[] startRow, Filter filter) 设置一个row值，看看是不是只扫描它后面的数据
        Scan scan = new Scan(Bytes.toBytes(startKey), expressionFilter);
        int count = 0;
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();
        System.out.println("====");
        while (r != null) {
            count++;
            System.out.println(r);
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
