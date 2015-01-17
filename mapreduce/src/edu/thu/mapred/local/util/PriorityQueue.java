package edu.thu.mapred.local.util;

import java.util.Comparator;

public class PriorityQueue<T> {
	private T[] heap;
	private int size;
	private int maxSize;
	private Comparator<T> comparator;

	public PriorityQueue(Comparator<T> comparator) {
		this.comparator = comparator;
	}

	private boolean lessThan(T t1, T t2) {
		return comparator.compare(t1, t2) < 0;
	}

	@SuppressWarnings("unchecked")
	public void initialize(int maxSize) {
		this.size = 0;
		int heapSize = maxSize + 1;
		this.heap = (T[]) new Object[heapSize];
		this.maxSize = maxSize;
	}

	public final void put(T element) {
		this.size++;
		this.heap[this.size] = element;
		upHeap();
	}

	public boolean insert(T element) {
		if (this.size < this.maxSize) {
			put(element);
			return true;
		} else if (this.size > 0 && !lessThan(element, top())) {
			this.heap[1] = element;
			adjustTop();
			return true;
		} else {
			return false;
		}
	}

	public final T top() {
		if (this.size > 0) {
			return this.heap[1];
		} else {
			return null;
		}
	}

	public final T pop() {
		if (this.size > 0) {
			T result = this.heap[1];
			this.heap[1] = this.heap[this.size];
			this.heap[this.size] = null;
			this.size--;
			downHeap();
			return result;
		} else {
			return null;
		}
	}

	public final void adjustTop() {
		downHeap();
	}

	public final int size() {
		return this.size;
	}

	public final void clear() {
		for (int i = 0; i <= this.size; i++) {
			this.heap[i] = null;
		}
		this.size = 0;
	}

	private final void upHeap() {
		int i = this.size;
		T node = this.heap[i];
		int j = i >>> 1;
		while (j > 0 && lessThan(node, this.heap[j])) {
			this.heap[i] = this.heap[j];
			i = j;
			j = j >>> 1;
		}
		this.heap[i] = node;
	}

	private final void downHeap() {
		int i = 1;
		T node = this.heap[i];
		int j = i << 1;
		int k = j + 1;
		if (k <= this.size && lessThan(this.heap[k], this.heap[j])) {
			j = k;
		}
		while (j <= this.size && lessThan(this.heap[j], node)) {
			this.heap[i] = this.heap[j];
			i = j;
			j = i << 1;
			k = j + 1;
			if (k <= this.size && lessThan(this.heap[k], this.heap[j])) {
				j = k;
			}
		}
		this.heap[i] = node;
	}
}
