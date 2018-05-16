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
    public void write(byte b[], int pos, int len){    	
    	Appendables.appendUTF8(builder, b, pos, len, Integer.MAX_VALUE);
    }

    @Override
    public void write(byte[] b) {
        write(b, 0, b.length);
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
	public void writeByte(int b) {
		assert(b>=32) : "only simple ASCII char values are supported here";
		assert(b<127) : "only simple ASCII char values are supported here";
		builder.append((char)b);//be carefull these MUST be ASCII only
	}
}
