package com.aliyun.odps.mapred;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class WordCounter {
	private static String path_src = "C:/Users/sky/Desktop/bigdata-master/mapreduce/4M-1/input-0.csv";
	private static String path_result = "C:/Users/sky/Desktop/bigdata-master/mapreduce/4M-1/output-0.csv";
	private static BufferedReader br = null;
	private static BufferedWriter bw = null;
	private static String line_current = null;
	private static String[] words = null;
	private static List<String> word_list = new ArrayList<String>();

	public static void main(String[] args) {
		File file = new File(path_src);
		if (!file.exists()) {
			System.out.println("file " + file + " is not existed, exit");
			return;
		}

		try {
			br = new BufferedReader(new FileReader(file.getPath()));
			line_current = br.readLine();
			while (line_current != null) {
				words = line_current.split(" |,|\\.");
				for (String s : words) {
					if (!s.equals(""))
						word_list.add(s);
				}

				line_current = br.readLine();
			}

			// for (String temp : word_list) {
			// System.out.println(temp);
			// }

			// HashSet
			Set<String> hashSet = new HashSet<String>(word_list);
			// for (String str : hashSet) {
			// System.out.println("word: " + str + ", occur times: "
			// + Collections.frequency(word_list, str));
			// }

			// HashMap
			Map<String, Integer> hashMap = new HashMap<String, Integer>();
			for (String temp : word_list) {
				Integer count = hashMap.get(temp);
				hashMap.put(temp, (count == null) ? 1 : count + 1);
			}

			// TreeMap
			TreeMap<String, Integer> treeMap = new TreeMap<String, Integer>(
					hashMap);

			// Record result to another file
			printMap(treeMap);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			closeInputStream(br);
			closeOutputStream(bw);
		}
	}

	public static void printMap(Map<String, Integer> map) throws IOException {

		bw = new BufferedWriter(new FileWriter(path_result));

		Set<String> keys = map.keySet();
		for (String s : keys) {
			// System.out.println("word: " + s + ", times: " + map.get(s));
			writeResult(s + "," + map.get(s));
		}
		// for (Map.Entry<String, Integer> entry : map.entrySet()) {
		// System.out.println("word: " + entry.getKey() + ", number : " +
		// entry.getValue());
		// writeResult(entry.getKey() + "," + entry.getValue());
		// }

	}

	public static void writeResult(String line) throws IOException {

		try {
			if (bw != null) {
				bw.write(line);
				bw.newLine();
				bw.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
			closeOutputStream(bw);
		}
	}

	public static void closeOutputStream(BufferedWriter writer) {
		try {
			if (writer != null) {
				writer.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void closeInputStream(BufferedReader reader) {
		try {
			if (reader != null) {
				reader.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
