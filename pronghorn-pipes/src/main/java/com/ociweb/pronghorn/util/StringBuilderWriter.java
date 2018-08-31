package com.ociweb.pronghorn.util;

public class StringBuilderWriter implements AppendableByteWriter<StringBuilderWriter>, CharSequence {
    
	private StringBuilder builder = new StringBuilder();

    public String toString() {
        return builder.toString();
    }

	public void clear() {
		builder.setLength(0);
	}
	
    @Override
    public StringBuilderWriter append(CharSequence csq){
        builder.append(csq);
        return this;
    }

    @Override
    public StringBuilderWriter append(CharSequence csq,int start,int end){
        builder.append(csq,start,end);
        return this;
    }

    @Override
    public StringBuilderWriter append(char c){
        builder.append(c);
        return this;
    }

	@Override
	public void write(byte[] encodedBlock) {
		Appendables.appendUTF8(builder, encodedBlock, 0, encodedBlock.length, Integer.MAX_VALUE);
	}

	@Override
	public void write(byte[] encodedBlock, int pos, int len) {
		Appendables.appendUTF8(builder, encodedBlock, pos, len, Integer.MAX_VALUE);
	}

	@Override
	public void writeByte(int asciiChar) {
		builder.append((char)asciiChar);
	}

	@Override
	public int length() {
		return builder.length();
	}


	@Override
	public char charAt(int index) {
		return builder.charAt(index);
	}


	@Override
	public CharSequence subSequence(int start, int end) {
		return builder.subSequence(start, end);
	}
}
