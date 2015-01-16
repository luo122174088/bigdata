package edu.thu.mapred.local.util;

public abstract class PriorityQueue<T> {
	private T[] heap;
	private int size;
	private int maxSize;

	protected abstract boolean lessThan(Object a, Object b);

	@SuppressWarnings("unchecked")
	protected final void initialize(int maxSize) {
		size = 0;
		int heapSize = maxSize + 1;
		heap = (T[]) new Object[heapSize];
		this.maxSize = maxSize;
	}

	public final void put(T element) {
		size++;
		heap[size] = element;
		upHeap();
	}

	public boolean insert(T element) {
		if (size < maxSize) {
			put(element);
			return true;
		} else if (size > 0 && !lessThan(element, top())) {
			heap[1] = element;
			adjustTop();
			return true;
		} else
			return false;
	}

	public final T top() {
		if (size > 0)
			return heap[1];
		else
			return null;
	}

	public final T pop() {
		if (size > 0) {
			T result = heap[1]; // save first value
			heap[1] = heap[size]; // move last to first
			heap[size] = null; // permit GC of objects
			size--;
			downHeap(); // adjust heap
			return result;
		} else
			return null;
	}

	public final void adjustTop() {
		downHeap();
	}

	public final int size() {
		return size;
	}

	public final void clear() {
		for (int i = 0; i <= size; i++)
			heap[i] = null;
		size = 0;
	}

	private final void upHeap() {
		int i = size;
		T node = heap[i];
		int j = i >>> 1;
		while (j > 0 && lessThan(node, heap[j])) {
			heap[i] = heap[j];
			i = j;
			j = j >>> 1;
		}
		heap[i] = node;
	}

	private final void downHeap() {
		int i = 1;
		T node = heap[i];
		int j = i << 1;
		int k = j + 1;
		if (k <= size && lessThan(heap[k], heap[j])) {
			j = k;
		}
		while (j <= size && lessThan(heap[j], node)) {
			heap[i] = heap[j];
			i = j;
			j = i << 1;
			k = j + 1;
			if (k <= size && lessThan(heap[k], heap[j])) {
				j = k;
			}
		}
		heap[i] = node;
	}
}
