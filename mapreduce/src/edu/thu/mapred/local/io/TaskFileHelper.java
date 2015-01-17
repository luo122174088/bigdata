package edu.thu.mapred.local.io;

import java.io.File;
import java.io.IOException;

import edu.thu.mapred.local.BaseDriver;

public class TaskFileHelper {

	private BaseDriver driver;

	public TaskFileHelper(BaseDriver driver) {
		this.driver = driver;
	}

	public File getSpillFile(int numSpill) throws IOException {
		String dir = this.driver.getTaskDir();
		File file = new File(dir, "spill" + numSpill);
		return file;
	}

	public File getTempDir() {
		File dir = new File(this.driver.getTaskDir(), "tmp");
		if (!dir.exists()) {
			dir.mkdir();
		}
		return dir;
	}

	public File getOutputFile() {
		String dir = this.driver.getTaskDir();
		File file = new File(dir, "file.out");
		return file;
	}

}
