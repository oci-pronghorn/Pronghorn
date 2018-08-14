package com.ociweb.pronghorn.pipe;

public class PipeReaderUTF8MutableCharSquence implements CharSequence {

	private StringBuilder target = new StringBuilder();
	private Pipe pipe; 
	private int loc;
	private boolean isInit;
	
	public PipeReaderUTF8MutableCharSquence() {
	}
	
	public PipeReaderUTF8MutableCharSquence setToField(Pipe pipe, int loc) {
		this.target.setLength(0);
		this.pipe = pipe;
		this.loc = loc;
		this.isInit = false;
		return this;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof CharSequence) {
			return PipeReader.eqUTF8(pipe, loc, (CharSequence)obj);
		}
		return false;	
	}
	
	@Override
	public String toString() {
		if (!isInit) {
			PipeReader.readUTF8(pipe, loc, target);	
			isInit = true;
		}
		return target.toString();
	}

	@Override
	public int length() {
		if (!isInit) {
			PipeReader.readUTF8(pipe, loc, target);	
			isInit = true;
		}
		return target.length();
	}

	@Override
	public char charAt(int index) {
		if (!isInit) {
			PipeReader.readUTF8(pipe, loc, target);
			isInit = true;
		}
		return target.charAt(index);
	}

	@Override
	public CharSequence subSequence(int start, int end) {
		if (!isInit) {
			PipeReader.readUTF8(pipe, loc, target);
			isInit = true;
		}
		return target.subSequence(start, end);
	}

}
