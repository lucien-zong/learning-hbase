package com.intel.hbase.test.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeUtil {

    public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    public static final SimpleDateFormat DEFAULT_PRINT_DATE_FORMAT = new SimpleDateFormat(
            TIME_FORMAT);

    protected static long now() {
        return System.currentTimeMillis();
    }

    public static String timeMillisToString(Long time) {
        long ms = time % 1000;
        long s = time / 1000;
        long m = s / 60;
        s = s % 60;
        long h = m / 60;
        m = m % 60;
        return "[" + Long.toString(time) + " ms] --> " + Long.toString(h)
                + "(h):" + Long.toString(m) + "(m):" + Long.toString(s)
                + "(s)." + Long.toString(ms) + "(ms)";
    }

    public static String longToDateString(long time) {
        Date date = new Date(time);
        return DEFAULT_PRINT_DATE_FORMAT.format(date);
    }

    public static Date printStringToDate(String s) throws ParseException {
        return DEFAULT_PRINT_DATE_FORMAT.parse(s);
    }

}
