package com.intel.hbase.test.multirowrangefilter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowKeyRange;
import org.apache.hadoop.hbase.util.Bytes;

import com.intel.hbase.test.util.TimeCounter;

public class MultiRowRangeFilterTest {
    public static void main(String[] args) throws Exception {
    	args = new String[]{"Test_Aggregate"};
        if (args.length < 1) {
            throw new Exception("Table name not specified.");
        }
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, args[0]);

        TimeCounter executeTimer = new TimeCounter();
        executeTimer.begin();
        executeTimer.enter();
        Scan scan = new Scan();
        List<RowKeyRange> ranges = new ArrayList<RowKeyRange>();
//        ranges.add(new RowKeyRange(Bytes.toBytes("001"), Bytes.toBytes("002")));
//        ranges.add(new RowKeyRange(Bytes.toBytes("003"), Bytes.toBytes("004")));
        ranges.add(new RowKeyRange(Bytes.toBytes("0,c01"), Bytes.toBytes("3,c01")));
        ranges.add(new RowKeyRange(Bytes.toBytes("6,c01"), Bytes.toBytes("8,c01")));
        Filter filter = new MultiRowRangeFilter(ranges);
        scan.setFilter(filter);
        int count = 0;
        ResultScanner scanner = table.getScanner(scan);
        Result r = scanner.next();//keyvalues={0,c01/family:compositeLongStr/1370592062825/Put/vlen=5/ts=0, 0,c01/family:long/1370592062825/Put/vlen=8/ts=0, 0,c01/family:longStr1/1370592062825/Put/vlen=1/ts=0, 0,c01/family:longStr2/1370592062825/Put/vlen=1/ts=0, 0,c01/family:mod10Str/1370592062825/Put/vlen=1/ts=0}
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
