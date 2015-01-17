package com.aliyun.odps.mapred;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import com.csvreader.CsvWriter;

/**
 * Generate Test Files
 * 
 * @author luochen
 * 
 */
public class CSVGenerator {

	private static Random random = new Random(System.currentTimeMillis());

	private static int Column_Len = 5;

	public static void main(String[] args) throws IOException {
		if (args.length < 4) {
			System.out.println("Usage: path columns size(M) num");
			return;
		}

		String outputDir = args[0];

		int columns = Integer.valueOf(args[1]);
		int size = Integer.valueOf(args[2]) * 1024 * 1024;
		int num = Integer.valueOf(args[3]);

		File dir = new File(outputDir);

		if (!dir.exists()) {
			dir.mkdirs();
		}

		for (int i = 0; i < num; i++) {
			generate(dir, i, columns, size);
		}
	}

	private static void generate(File dir, int seq, int columns, int size) throws IOException {
		File file = new File(dir, "input-" + seq + ".csv");

		CsvWriter writer = new CsvWriter(file.getAbsolutePath());

		int bytesWrite = 0;
		FileOutputStream out;

		while (bytesWrite < size) {
			for (int i = 0; i < columns; i++) {
				String str = random(Column_Len);
				bytesWrite += (str.length() + 1);
				writer.write(str);
			}
			writer.endRecord();
		}
		writer.close();
	}

	private static String random(int length) {
		StringBuilder builder = new StringBuilder(length);
		for (int i = 0; i < length; i++) {
			builder.append((char) (ThreadLocalRandom.current().nextInt('a', 'z')));
		}
		return builder.toString();
	}
}
