package cn.ctms_log.test;

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
import org.apache.hadoop.hbase.client.coprocessor.GroupByClient;
import org.apache.hadoop.hbase.expression.Expression;
import org.apache.hadoop.hbase.expression.ExpressionFactory;
import org.apache.hadoop.hbase.expression.evaluation.EvaluationResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

import com.intel.hbase.test.util.TimeCounter;

public class GroupByOperationRunner extends Thread {

    public final static String HBASE_ADDRESS_KEY = "hbase_address";
    public final static String TABLE_KEY = "table";
    public final static String RPC_TIMEOUT = "hbase.rpc.timeout";
    public final static String GROUGBY_KEY_KEY = "KEY";
    public final static String GROUGBY_SELECT_KEY = "SELECT";

    private File configFileName;
    private Properties props;
    protected String selectString = "";

    public GroupByOperationRunner(File file) {
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
        String timeoutValue = props.getProperty(RPC_TIMEOUT, "120000");

        Configuration conf = HBaseConfiguration.create();
        try {
            ZKUtil.applyClusterKeyToConf(conf, address);
            //java.io.IOException: Cluster key invalid, the format should be:hbase.zookeeper.quorum:hbase.zookeeper.client.port:zookeeper.znode.parent
            long timeout = Long.parseLong(timeoutValue);
            conf.setLong("hbase.rpc.timeout", timeout);

            GroupByClient groupByClient = new GroupByClient(conf);
            Scan[] scans = { new Scan() };

            List<Expression> groupByExpresstions = new ArrayList<Expression>();
            List<Expression> selectExpresstions = new ArrayList<Expression>();
            
        	//按源中心分组
//			Expression key1 = ExpressionFactory.columnValue("f",
//					"source_center");
//			groupByExpresstions.add(key1);

			 configGroupByKey(groupByExpresstions, selectExpresstions);

//			Expression select1 = ExpressionFactory.groupByKey(ExpressionFactory
//					.columnValue("f", "source_center"));
//			Expression select2 = ExpressionFactory.count(ExpressionFactory
//					.columnValue("f", "dest_center"));
//			Expression select3 = ExpressionFactory.count(ExpressionFactory
//					.columnValue("f", "time_ctms"));
//			Expression select4 = ExpressionFactory.count(ExpressionFactory
//					.columnValue("f", "time_exec"));
//			selectExpresstions.add(select1);
//			selectExpresstions.add(select2);
//			selectExpresstions.add(select3);
//			selectExpresstions.add(select4);
			 
            configSelectExpresstions(selectExpresstions);

            try {
                TimeCounter executeTimer = new TimeCounter();
                executeTimer.begin();
                executeTimer.enter();

                List<EvaluationResult[]> groupByResultList = groupByClient
                        .groupBy(table, scans, groupByExpresstions,
                                selectExpresstions, null);

                System.out.println("-- Output groupby result START --");
                System.out.println(selectString);
                for (EvaluationResult[] res : groupByResultList) {
                    String resultString = "";
                    for (int i = 0; i < res.length; i++) {
                        EvaluationResult er = res[i];
                        resultString += "\t\t" + er.toString();
                        if (0 == ((i + 1) % selectExpresstions.size())) {
                            System.out.println(resultString);
                            resultString = "";
                        }
                    }
                }
                System.out.println("-- Output groupby result END --");

                executeTimer.leave();
                executeTimer.end();
                System.out.println("++ Time cost for GroupBy [config -> "
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

    private boolean configGroupByKey(List<Expression> groupByExpresstions,
            List<Expression> selectExpresstions) {
        String columns = props.getProperty(GROUGBY_KEY_KEY);
        StringTokenizer st = new StringTokenizer(columns, ", ");
        while (st.hasMoreTokens()) {
            String column[] = st.nextToken().split(":");
            switch (column.length) {
            case 2:
                groupByExpresstions.add(ExpressionFactory.columnValue(
                        column[0], column[1]));
                break;
            default:
                System.out
                        .println("The value of 'KEY' set in configuration file "
                                + configFileName
                                + " seems incorrect. Please have a check first.");
                return false;
            }
        }
        return true;
    }

    private boolean configSelectExpresstions(List<Expression> selectExpresstions) {
        String aggregations = props.getProperty(GROUGBY_SELECT_KEY);
        StringTokenizer st = new StringTokenizer(aggregations, ", ");
        while (st.hasMoreTokens()) {
            String aggregation[] = st.nextToken().split("#");
            if (1 == aggregation.length) {
                String tmpColumns = aggregation[0];
                String tmpColumn[] = tmpColumns.split(":");
                if (2 != tmpColumn.length) {
                    System.out
                            .println("The value of 'SELECT' set in configuration file "
                                    + configFileName
                                    + " seems incorrect. Please have a check first.");
                    return false;
                } else {
                    selectExpresstions.add(ExpressionFactory
                            .groupByKey(ExpressionFactory.columnValue(
                                    tmpColumn[0], tmpColumn[1])));
                    selectString += "\t" + tmpColumns;
                    continue;
                }

            } else {
                String action = aggregation[0];
                String columns = aggregation[1];
                String column[] = columns.split(":");
                if (2 != column.length) {
                    System.out
                            .println("The value of 'SELECT' set in configuration file "
                                    + configFileName
                                    + " seems incorrect. Please have a check first.");
                    return false;
                } else {
                    Expression columnValueExp = ExpressionFactory.columnValue(
                            column[0], column[1]);
                    if (action.equalsIgnoreCase("rowcount")) {
                        selectExpresstions.add(ExpressionFactory
                                .count(columnValueExp));
                    } else if (action.equalsIgnoreCase("max")) {
                        selectExpresstions.add(ExpressionFactory
                                .max(columnValueExp));
                    } else if (action.equalsIgnoreCase("min")) {
                        selectExpresstions.add(ExpressionFactory
                                .min(columnValueExp));
                    } else if (action.equalsIgnoreCase("sum")) {
                        selectExpresstions.add(ExpressionFactory
                                .sum(columnValueExp));
                    } else if (action.equalsIgnoreCase("std")) {
                        selectExpresstions.add(ExpressionFactory
                                .stdDev(columnValueExp));
                    } else if (action.equalsIgnoreCase("avg")) {
                        selectExpresstions.add(ExpressionFactory
                                .avg(columnValueExp));
                    } else {

                    }
                    selectString += "\t" + action + "(" + columns + ")";
                }
            }

        }
        return true;

    }

}
