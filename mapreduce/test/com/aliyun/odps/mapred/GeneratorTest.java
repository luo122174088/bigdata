package com.aliyun.odps.mapred;

import org.junit.Test;

public class GeneratorTest {

	public void test(String[] str) throws Exception {
		CSVGenerator.main(str);

		WordCount.main(new String[] { str[0], str[1] });
	}

	/**
	 * 数据文件，1个，4M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test4M() throws Exception {
		test(new String[] { "4M-1", "3", "4", "1" });
	}

	/**
	 * 数据文件，1个，8M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test8M() throws Exception {
		test(new String[] { "8M-1", "3", "8", "1" });
	}

	/**
	 * 数据文件，1个，16M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test16M() throws Exception {
		test(new String[] { "16M-1", "3", "16", "1" });
	}

	/**
	 * 数据文件，1个，32M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test32M() throws Exception {
		test(new String[] { "32M-1", "3", "32", "1" });
	}

	/**
	 * 数据文件，1个，64M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test64M() throws Exception {
		test(new String[] { "64M-1", "3", "64", "1" });
	}

	/**
	 * 数据文件，1个，128M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test128M() throws Exception {
		test(new String[] { "128M-1", "3", "128", "1" });
	}

	/**
	 * 数据文件，1个，256M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test256M() throws Exception {
		test(new String[] { "256M-1", "3", "256", "1" });
	}

	/**
	 * 数据文件，1个，512M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test512M() throws Exception {
		test(new String[] { "512M-1", "3", "512", "1" });
	}

	/**
	 * 数据文件，1个，1024M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test1024M() throws Exception {
		test(new String[] { "1024M-1", "3", "1024", "1" });
	}

	/**
	 * 数据文件，1个，2048M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test2048M() throws Exception {
		test(new String[] { "2048M-1", "3", "2048", "1" });
	}

	/**
	 * 数据文件，4个，每个512M,3列
	 * 
	 * @throws Exception
	 */
	@Test
	public void test4X512M() throws Exception {
		test(new String[] { "4X512M-1", "3", "512", "4" });
	}
}
