package com.intel.hbase.test.aggregate;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.CompositeLongStrColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongStrColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

import com.intel.hbase.test.util.TimeCounter;

public class AggregateOperationRunner extends Thread {

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

    public AggregateOperationRunner(File file) {
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
            // scan 添加一个或多个列信息,scan就是可以返回多行，不添加行条件
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

                if (action.equalsIgnoreCase("rowcount")) {
                	//有表，有列解释器，有查询条件(scan：按列查询)，就可以进行聚合操作了
                    Long rowCount = aClient.rowCount(table, columnInterpreter,
                            scan);
                    System.out.println("The result of the rowCount is "
                            + rowCount);
                } else if (action.equalsIgnoreCase("max")) {
                    Long max = aClient.max(table, columnInterpreter, scan);
                    System.out.println("The result of the max is " + max);
                } else if (action.equalsIgnoreCase("min")) {
                    Long min = aClient.min(table, columnInterpreter, scan);
                    System.out.println("The result of the min is " + min);
                } else if (action.equalsIgnoreCase("sum")) {
                    Long sum = aClient.sum(table, columnInterpreter, scan);
                    System.out.println("The result of the sum is " + sum);
                } else if (action.equalsIgnoreCase("std")) {
                    Double std = aClient.std(table, columnInterpreter, scan);
                    System.out.println("The result of the std is " + std);
                } else if (action.equalsIgnoreCase("median")) {
                    Long median = aClient
                            .median(table, columnInterpreter, scan);
                    System.out.println("The result of the median is " + median);
                } else if (action.equalsIgnoreCase("avg")) {
                    Double avg = aClient.avg(table, columnInterpreter, scan);
                    System.out.println("The result of the avg is " + avg);
                } else {
                    System.out.println("The action '" + action
                            + "' set in configuration file " + configFileName
                            + " doesn't exist.");
                    return;
                }

                executeTimer.leave();
                executeTimer.end();
                System.out.println("++ Time cost for Aggregation [config -> "
                        + configFileName + "]: " + executeTimer.getTimeString()
                        + " ++");

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
        String columns = props.getProperty(COLUMNS_KEY);//conf.co:columns=family:mod10Str
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
            return new LongStrColumnInterpreter();//column value string '12' will be regarded by this interpreter as 12 while by LongColumnInterpreter as 12594 (0x3132)
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
