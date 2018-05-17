package com.ociweb.pronghorn.util;

public class StringBuilderWriter implements AppendableByteWriter<StringBuilderWriter> {
    private final StringBuilder builder = new StringBuilder();

    public String toString() {
        return builder.toString();
    }

    public void setLength(int length) {
        builder.setLength(length);
    }
    
	@Override
	public void reset() {
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
}
