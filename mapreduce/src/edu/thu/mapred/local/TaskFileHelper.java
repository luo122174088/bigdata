package edu.thu.mapred.local;

import java.io.File;
import java.io.IOException;

public class TaskFileHelper {

	private BaseDriver driver;

	public TaskFileHelper(BaseDriver driver) {
		this.driver = driver;
	}

	public File getSpillFile(int numSpill) throws IOException {
		String dir = driver.getTaskDir();
		File file = new File(dir, "spill" + numSpill);
		return file;
	}

	public File getTempDir() {
		File dir = new File(driver.getTaskDir(), "tmp");
		if (!dir.exists()) {
			dir.mkdir();
		}
		return dir;
	}

	public File getOutputFile() {
		String dir = driver.getTaskDir();
		File file = new File(dir, "file.out");
		return file;
	}

}
