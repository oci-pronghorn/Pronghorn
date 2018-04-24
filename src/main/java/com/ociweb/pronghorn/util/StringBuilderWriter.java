package com.ociweb.pronghorn.util;

public class StringBuilderWriter implements AppendableByteWriter {
    private final StringBuilder builder = new StringBuilder();

    public String toString() {
        return builder.toString();
    }

    public void setLength(int length) {
        builder.setLength(length);
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
    public Appendable append(CharSequence csq){
        builder.append(csq);
        return this;
    }

    @Override
    public Appendable append(CharSequence csq,int start,int end){
        builder.append(csq,start,end);
        return this;
    }

    @Override
    public Appendable append(char c){
        builder.append(c);
        return this;
    }
}
