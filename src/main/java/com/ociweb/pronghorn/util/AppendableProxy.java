package com.ociweb.pronghorn.util;

import java.io.IOException;

public class AppendableProxy implements Appendable {

	private final Appendable a;
	
	public AppendableProxy(Appendable a) {
		this.a = a;
	}
	
	@Override
	public AppendableProxy append(CharSequence csq) {
		try {
			a.append(csq);
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public AppendableProxy append(CharSequence csq, int start, int end) {
		try {
			a.append(csq, start, end);
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public AppendableProxy append(char c) {
		try {
			a.append(c);
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
