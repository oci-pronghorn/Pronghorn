package com.ociweb.pronghorn.util;

import java.io.IOException;

public class AppendableProxy implements Appendable {

	private final Appendable a;
	
	public AppendableProxy(Appendable a) {
		this.a = a;
	}
	
	@Override
	public Appendable append(CharSequence csq) {
		try {
			return a.append(csq);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Appendable append(CharSequence csq, int start, int end) {
		try {
			return a.append(csq, start, end);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Appendable append(char c) {
		try {
			return a.append(c);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
