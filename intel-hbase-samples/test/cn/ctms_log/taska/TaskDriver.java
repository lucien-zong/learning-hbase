package cn.ctms_log.taska;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.reflect.Method;

import org.junit.Test;

public class TaskDriver {

	/**
	 * @Desc 统计目标中心为 AD 的所有源中心的请求<br/>
	 *       测试 CtmsCountAd
	 * @author root
	 * @date 2013-6-9 下午4:52:40
	 */
	@Test
	public void testCtmsCountAd() throws Exception {

		for (int i = 1; i < 51; i++) {

			System.out.println("\r\n=======================    TEST  " + i
					+ "    =======================");

			Method mainMethod = Class.forName("cn.ctms_log.taska.CtmsCountAd")
					.getMethod("main", String[].class);

			mainMethod.invoke(null, (Object) new String[] {});

			Method countMethod = Class.forName(
					"cn.ctms_log.test.count.AggregateCount").getMethod("main",
					String[].class);

			countMethod.invoke(null, (Object) new String[] {});

		}

	}

	/**
	 * /test/cn/ctms_log/taska/output 152488 50 平均时间： 3049 ms
	 * /test/cn/ctms_log/taskb/output_all 94216 50 平均时间： 1884 ms
	 * /test/cn/ctms_log/taskb/output_less 116482 50 平均时间： 2329 ms
	 * /test/cn/ctms_log/taskb/output_more 127863 50 平均时间： 2557 ms
	 */
	@Test
	public void calcTime() throws Exception {

		String output = "/test/cn/ctms_log/taska/output";
		String output_all = "/test/cn/ctms_log/taskb/output_all";
		String output_less = "/test/cn/ctms_log/taskb/output_less";
		String output_more = "/test/cn/ctms_log/taskb/output_more";

		show(output);
		show(output_all);
		show(output_less);
		show(output_more);

	}

	private void show(String filePath) throws Exception {

		System.out.println(filePath);

		File file = new File(System.getProperty("user.dir") + filePath);
		BufferedReader br = new BufferedReader(new FileReader(file));

		int count = 0;
		String line = null;
		int totalTime = 0;

		while (true) {
			if ((line = br.readLine()) != null) {
				if (line.contains("查询")) {
					String time = line.split("(\\[|\\])")[1].substring(0, 4);
					totalTime += Integer.parseInt(time);
					count++;
				}
			} else {
				break;
			}
		}
		System.out.println(totalTime);
		System.out.println(count);
		System.out.println("平均时间： " + totalTime / count + " ms");

		br.close();
	}

}
