package com.aliyun.odps.mapred;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

public class LockTest {

	@Test
	public void test() {
		Lock lock = new ReentrantLock();

		long start = System.currentTimeMillis();
		for (int i = 0; i < 50000000; i++) {
			lock.lock();
			lock.unlock();
		}
		long end = System.currentTimeMillis();
		System.out.println(end - start);

		start = System.currentTimeMillis();
		for (int i = 0; i < 10000000; i++) {
		}
		end = System.currentTimeMillis();
		System.out.println(end - start);

	}
}
