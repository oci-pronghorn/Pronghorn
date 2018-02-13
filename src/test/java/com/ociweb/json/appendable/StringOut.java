package com.ociweb.json.appendable;

import java.io.UnsupportedEncodingException;

public class StringOut implements AppendableByteWriter {
    private final StringBuilder builder = new StringBuilder();

    public String toString() {
        return builder.toString();
    }

    @Override
    public void write(byte b[], int pos, int len){
        CharSequence x = null;
        try {
            x = new String(b, pos, len, "UTF-8");
        } catch (UnsupportedEncodingException ignored) {
        }
        builder.append(x);
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
