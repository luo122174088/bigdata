package edu.thu.mapred.local.util;

public class IndexSorter {

	public static interface IndexSortable {
		int compareIndex(int i, int j);

		void swapIndex(int i, int j);
	}

	private static void fix(IndexSortable s, int l, int r) {
		if (s.compareIndex(l, r) > 0) {
			s.swapIndex(l, r);
		}
	}

	public static void sort(final IndexSortable s, int l, int r) {
		sortImpl(s, l, r);
	}

	private static void sortImpl(final IndexSortable s, int l, int r) {
		while (true) {
			if (r - l < 13) {
				for (int i = l; i < r; ++i) {
					for (int j = i; j > l && s.compareIndex(j - 1, j) > 0; --j) {
						s.swapIndex(j, j - 1);
					}
				}
				return;
			}
			fix(s, (l + r) >>> 1, l);
			fix(s, (l + r) >>> 1, r - 1);
			fix(s, l, r - 1);
			int i = l;
			int j = r;
			int ll = l;
			int rr = r;
			int cr;
			while (true) {
				while (++i < j) {
					if ((cr = s.compareIndex(i, l)) > 0) {
						break;
					}
					if (0 == cr && ++ll != i) {
						s.swapIndex(ll, i);
					}
				}
				while (--j > i) {
					if ((cr = s.compareIndex(l, j)) > 0) {
						break;
					}
					if (0 == cr && --rr != j) {
						s.swapIndex(rr, j);
					}
				}
				if (i < j) {
					s.swapIndex(i, j);
				} else {
					break;
				}
			}
			j = i;
			while (ll >= l) {
				s.swapIndex(ll--, --i);
			}
			while (rr < r) {
				s.swapIndex(rr++, j++);
			}
			if (i - l < r - j) {
				sortImpl(s, l, i);
				l = j;
			} else {
				sortImpl(s, j, r);
				r = i;
			}
		}
	}

}
