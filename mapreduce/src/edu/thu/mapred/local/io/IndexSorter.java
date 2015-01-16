package edu.thu.mapred.local.io;

public class IndexSorter {

	private static void fix(Sortable s, int p, int r) {
		if (s.compare(p, r) > 0) {
			s.swap(p, r);
		}
	}

	public void sort(final Sortable s, int p, int r) {
		sortInternal(s, p, r);
	}

	private static void sortInternal(final Sortable s, int p, int r) {
		while (true) {
			if (r - p < 13) {
				for (int i = p; i < r; ++i) {
					for (int j = i; j > p && s.compare(j - 1, j) > 0; --j) {
						s.swap(j, j - 1);
					}
				}
				return;
			}

			fix(s, (p + r) >>> 1, p);
			fix(s, (p + r) >>> 1, r - 1);
			fix(s, p, r - 1);

			int i = p;
			int j = r;
			int ll = p;
			int rr = r;
			int cr;
			while (true) {
				while (++i < j) {
					if ((cr = s.compare(i, p)) > 0)
						break;
					if (0 == cr && ++ll != i) {
						s.swap(ll, i);
					}
				}
				while (--j > i) {
					if ((cr = s.compare(p, j)) > 0)
						break;
					if (0 == cr && --rr != j) {
						s.swap(rr, j);
					}
				}
				if (i < j)
					s.swap(i, j);
				else
					break;
			}
			j = i;
			while (ll >= p) {
				s.swap(ll--, --i);
			}
			while (rr < r) {
				s.swap(rr++, j++);
			}

			assert i != j;
			if (i - p < r - j) {
				sortInternal(s, p, i);
				p = j;
			} else {
				sortInternal(s, j, r);
				r = i;
			}
		}
	}

}
