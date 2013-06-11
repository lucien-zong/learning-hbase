package cn.ctms_log.taskb;

import java.lang.reflect.Method;

import org.junit.Test;

public class TaskDriver {

	/**
	 * @Desc 测试所有源中心发送请求数，根据源中心分组
	 * @author root
	 * @date 2013-6-9 下午4:39:13
	 */
	@Test
	public void testSourceCenterGB() throws Exception {

		for (int i = 1; i < 51; i++) {

			System.out.println("\r\n=======================    TEST  " + i
					+ "    =======================");

			Method mainMethod = Class.forName(
					"cn.ctms_log.taskb.SourceCenterGBTest").getMethod("main",
					String[].class);

			mainMethod.invoke(null, (Object) new String[] {});

			Method countMethod = Class.forName(
					"cn.ctms_log.test.count.AggregateCount").getMethod("main",
					String[].class);

			countMethod.invoke(null, (Object) new String[] {});

		}

	}

}
