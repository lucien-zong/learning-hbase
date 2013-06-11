package cn.ctms_log.test.testtaskb;

import java.io.File;
import java.io.FileFilter;
import java.util.Properties;

import org.junit.Test;

/**
 * @Desc 测试_所有源中心 发送请求数，根据源中心分组. <br/>
 *       所用配置文件： conf.CtmsCountAd_test
 * @author root
 * @date 2013-6-9 下午2:07:48
 */
public class SourceCenterGBTest {

	public final static String CONFIG_DIR_KEY = "dir";
	public final static String FILE_SUFFIX = "CtmsCountAd_test";

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
				new SourceCenterGBRunnerTest(f).start();
			} else {
				new SourceCenterGBRunnerTest(f).run();
			}
		}
	}

	public static Properties parseArgs(String[] args) {

		Properties props = new Properties();

		args = new String[4];
		args[0] = "-dir";
		args[1] = System.getProperty("user.dir") + File.separator + "src";
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

	@Test
	public void testTmp() {
		System.out.println(System.getProperty("user.dir"));
	}

	public static void usageMessage() {
		System.out.println("Usage: Engine -dir dir -concurrent true/false ");
	}

}
