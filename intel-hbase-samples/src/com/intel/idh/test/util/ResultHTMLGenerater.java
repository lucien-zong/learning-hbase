package com.intel.idh.test.util;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

public class ResultHTMLGenerater {

    protected final static String CS_REGRESSION = "<td bgcolor=\"Red\">";
    protected final static String CS_RESOLVED = "<td bgcolor=\"Green\">";
    protected final static String CS_PASS = "<td>";
    protected final static String CS_FAIL = "<td bgcolor=\"Red\">";
    protected final static String CS_NORMAL = "<td>";
    protected final static String CS_NEW_CASE_PASS = "<td bgcolor=\"Green\">[+]";
    protected final static String CS_NEW_CASE_FAIL = "<td bgcolor=\"Yellow\">[+]";

    private String title;

    private ArrayList<ResultInfo> info = new ArrayList<ResultInfo>();

    public ResultHTMLGenerater() {

    }

    public void addCase(String name, String desciption, boolean pass) {
        info.add(new ResultInfo(name, desciption, pass));
    }

    public void generateHTMLFile(String file) {
        StringBuffer sb = new StringBuffer();
        sb.append("<html>");
        sb.append("<head>");
        sb.append("<title>" + title + "</title>");
        sb.append("</head>");
        sb.append("<body>");
        sb.append("<table border=1>");
        for (ResultInfo info : this.info) {
            printInfo(sb, info);
        }
        sb.append("</table>");
        sb.append("</body>");
        try {
            FileWriter fw = new FileWriter(file);
            fw.append(sb);
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void printInfo(StringBuffer sb, ResultInfo info) {
        sb.append("<tr>");
        sb.append("<td>" + info.name + "</td>");
        sb.append("<td>" + info.desciption + "</td>");
        if (info.pass) {
            printPassed(sb);
        } else {
            printFailed(sb);
        }
        sb.append("</tr>");
    }

    private void printPassed(StringBuffer sb) {
        sb.append(CS_PASS + "PASSED" + "</td>");
    }

    private void printFailed(StringBuffer sb) {
        sb.append(CS_FAIL + "FAILED" + "</td>");
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public class ResultInfo {

        String name;
        String desciption;
        boolean pass;

        public ResultInfo(String name, String desc, boolean result) {
            this.name = name;
            desciption = desc;
            pass = result;
        }
    }

    public static void main(String[] args) {
        ResultHTMLGenerater rg = new ResultHTMLGenerater();
        rg.setTitle("test");
        rg.addCase("case1", "it's kengdie ", true);
        rg.addCase("case2", "it's kengdie2 ", false);
        rg.generateHTMLFile("test.html");
    }
}
