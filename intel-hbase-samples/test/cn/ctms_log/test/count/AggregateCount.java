package cn.ctms_log.test.count;

import java.io.File;
import java.io.FileFilter;
import java.util.Properties;
/**
 *-dir E:\Users\embracesource\workspace1\Intel_1\src -concurrent false 
 * @author root
 *
 */
public class AggregateCount {

    public final static String CONFIG_DIR_KEY = "dir";
    public final static String FILE_SUFFIX = "AggregateCount";

    public static void main(String[] args) {
        File currentDir = new File(".");
//        System.out.println(currentDir.getAbsolutePath());
        Properties props = parseArgs(args);

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
                new AggregateCountRunner(f).start();
            } else {
                new AggregateCountRunner(f).run();
            }
        }
    }

    public static Properties parseArgs(String[] args) {
        Properties props = new Properties();
        // parse arguments
        int argindex = 0;
        
        args = new String[4];
		args[0] = "-dir";
		args[1] = System.getProperty("user.dir") + File.separator + "src";
		args[2] = "-concurrent";
		args[3] = "false";

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
