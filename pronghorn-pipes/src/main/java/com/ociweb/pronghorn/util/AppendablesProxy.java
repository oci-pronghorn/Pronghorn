package com.ociweb.pronghorn.util;

import java.io.IOException;

public class AppendablesProxy extends AppendableProxy {

	private final Appendable[] a;
	
	public static AppendablesProxy wrap(Appendable[] a) {
		return new AppendablesProxy(a);
	}
	
	public AppendablesProxy(Appendable[] a) {
		super(null);
		this.a = a;
	}
	
	@Override
	public AppendablesProxy append(CharSequence csq) {
		try {
			int i = a.length;
			while (--i>=0) {		
				a[i].append(csq);
			}
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public AppendablesProxy append(CharSequence csq, int start, int end) {
		try {
			int i = a.length;
			while (--i>=0) {	
				a[i].append(csq, start, end);
			}
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public AppendablesProxy append(char c) {
		try {
			int i = a.length;
			while (--i>=0) {	
				a[i].append(c);
			}
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
