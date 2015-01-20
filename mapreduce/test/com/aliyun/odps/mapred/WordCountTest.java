package com.aliyun.odps.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Test;

public class WordCountTest {

	private void test(String input, String result) throws Exception {
		System.setProperty("input.path", input);
		WordCount.main(new String[] { "wc_in", "wc_out" });

		System.out.println("MapReduce job finishes");

		File jobDir = new File("job");
		File[] dirs = jobDir.listFiles();

		jobDir = dirs[dirs.length - 1];

		File outputFile = new File(jobDir, "reduce/R1_000001/part-1");

		System.out.println("Checking job output: " + outputFile.getPath() + " with count result: "
				+ result);

		BufferedReader reader1 = new BufferedReader(new FileReader(outputFile));
		BufferedReader reader2 = new BufferedReader(new FileReader(result));

		String line1 = null;
		String line2 = null;
		while (true) {
			line1 = reader1.readLine();
			line2 = reader2.readLine();
			if (line1 != null && line2 == null) {
				System.out.println("Error, " + result + " ends.");
				break;
			} else if (line1 == null && line2 != null) {
				System.out.println("Error, " + outputFile.getPath() + " ends.");
				break;
			} else if (line1 == null && line2 == null) {
				System.out.println("Correct.");
				break;
			} else {
				if (!line1.equals(line2)) {
					System.out.println("Error");
					System.out.println("Output: " + line1);
					System.out.println("Result: " + line2);
					break;
				}
			}
		}
		reader1.close();
		reader2.close();
	}

	@Test
	public void test1() throws Exception {
		int[] inputs = new int[] { 1, 4, 8, 16, 64, 256 };
		for (int i : inputs) {
			String input = "data/" + i + "M-4";
			String result = "result/" + i + "M-4.result";
			test(input, result);
		}
	}

	public void test2() throws Exception {
		System.setProperty("input.path", "data/64M-4");
		WordCount.main(new String[] { "wc_in", "wc_out" });
	}

	public void test3() throws Exception {
		int i = 1;
		String input = "data/" + i + "M-4";
		String result = "result/" + i + "M-4.result";
		test(input, result);
	}

	public void test4() throws Exception {
		System.setProperty("input.path", "data/test");
		WordCount.main(new String[] { "wc_in", "wc_out" });
	}
}
