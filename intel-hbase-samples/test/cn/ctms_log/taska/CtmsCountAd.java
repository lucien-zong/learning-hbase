package cn.ctms_log.taska;

import java.io.File;
import java.io.FileFilter;
import java.util.Properties;

/**
 * @Desc 统计目标中心为 AD 的所有源中心的请求<br/>
 *       使用配置文件 conf.CtmsCountAd
 * @author root
 * @date 2013-6-9 下午4:49:24
 */
public class CtmsCountAd {

	public final static String CONFIG_DIR_KEY = "dir";
	public final static String FILE_SUFFIX = "CtmsCountAd";

	public static void main(String[] args) {

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
				new CtmsCountAdRunner(f).start();
			} else {
				new CtmsCountAdRunner(f).run();
			}
		}
	}

	public static Properties parseArgs(String[] args) {

		Properties props = new Properties();
		args = new String[4];
		args[0] = "-dir";
		args[1] = "E:\\Users\\embracesource\\workspace1\\Intel_1\\src";
		args[2] = "-concurrent";
		args[3] = "false";
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
