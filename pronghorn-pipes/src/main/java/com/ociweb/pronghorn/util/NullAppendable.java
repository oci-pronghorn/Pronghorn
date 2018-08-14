package com.ociweb.pronghorn.util;

import java.io.IOException;

public class NullAppendable implements Appendable {

    @Override
    public Appendable append(CharSequence csq) throws IOException {
        //do nothing
        return this;
    }

    @Override
    public Appendable append(CharSequence csq, int start, int end) throws IOException {
        //do nothing
        return this;
    }

    @Override
    public Appendable append(char c) throws IOException {
        //do nothing 
        return this;
    }

}
