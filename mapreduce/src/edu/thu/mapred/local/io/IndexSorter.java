package edu.thu.mapred.local.io;

public class IndexSorter {

	public static interface Sortable {
		int compare(int i, int j);

		void swap(int i, int j);
	}

	private static void fix(Sortable s, int l, int r) {
		if (s.compare(l, r) > 0) {
			s.swap(l, r);
		}
	}

	public void sort(final Sortable s, int l, int r) {
		sortImpl(s, l, r);
	}

	private static void sortImpl(final Sortable s, int l, int r) {
		while (true) {
			if (r - l < 13) {
				for (int i = l; i < r; ++i) {
					for (int j = i; j > l && s.compare(j - 1, j) > 0; --j) {
						s.swap(j, j - 1);
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
					if ((cr = s.compare(i, l)) > 0) {
						break;
					}
					if (0 == cr && ++ll != i) {
						s.swap(ll, i);
					}
				}
				while (--j > i) {
					if ((cr = s.compare(l, j)) > 0) {
						break;
					}
					if (0 == cr && --rr != j) {
						s.swap(rr, j);
					}
				}
				if (i < j) {
					s.swap(i, j);
				} else {
					break;
				}
			}
			j = i;
			while (ll >= l) {
				s.swap(ll--, --i);
			}
			while (rr < r) {
				s.swap(rr++, j++);
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
