package com.ociweb.pronghorn.pipe;

public class PipeUTF8MutableCharSquence implements CharSequence {

	private StringBuilder target = new StringBuilder();
	private Pipe pipe; 
	private int meta;
	private int len;
	private boolean isInit;
	
	public PipeUTF8MutableCharSquence() {
	}
	
	public PipeUTF8MutableCharSquence setToField(Pipe pipe, int meta, int len) {
		this.target.setLength(0);
		this.pipe = pipe;
		this.meta = meta;
		this.len = len;
		this.isInit = false;
		return this;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CharSequence) {
			return Pipe.isEqual(pipe, (CharSequence)obj, meta, len);
		}
		return false;	
	}
	
	@Override
	public String toString() {
		if (!isInit) {
			Pipe.readUTF8(pipe, target, meta, len);	
			isInit = true;
		}
		return target.toString();
	}

	@Override
	public int length() {
		if (!isInit) {
			Pipe.readUTF8(pipe, target, meta, len);		
			isInit = true;
		}
		return target.length();
	}

	@Override
	public char charAt(int index) {
		if (!isInit) {
			Pipe.readUTF8(pipe, target, meta, len);	
			isInit = true;
		}
		return target.charAt(index);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		if (!isInit) {
			Pipe.readUTF8(pipe, target, meta, len);	
			isInit = true;
		}
		return target.subSequence(start, end);
	}

}
