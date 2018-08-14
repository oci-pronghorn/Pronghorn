package com.ociweb.pronghorn.util;

import java.io.IOException;

public class AppendableReplicator implements Appendable {

	private final Appendable[] a;
	private final boolean[] enabled;
	
	public static AppendableReplicator wrap(Appendable ... a) {
		return new AppendableReplicator(a);
	}
	
	public AppendableReplicator(Appendable ... a) {
		this.a = a;
		this.enabled = new boolean[a.length];
		this.enableAll();
	}
	
	public void enableAll() {
		int i = this.enabled.length;
		while (--i>=0) {
			enabled[i] = true;
		}
	}
	
	public void enable(int i) {
		enabled[i] = true;
	}
	
	public void disable(int i) {
		enabled[i] = false;
	}
	
	
	@Override
	public AppendableReplicator append(CharSequence csq) {
		try {
			int i = a.length;
			while(--i>=0) {
				a[i].append(csq);
			}
			
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public AppendableReplicator append(CharSequence csq, int start, int end) {
		try {
			int i = a.length;
			while(--i>=0) {
				a[i].append(csq, start, end);
			}
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public AppendableReplicator append(char c) {
		try {
			int i = a.length;
			while(--i>=0) {
				a[i].append(c);
			}
			return this;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
