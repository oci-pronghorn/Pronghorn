package com.ociweb.pronghorn.util;

public interface ByteSquenceVisitor {

    void end(int value);

    boolean open(short[] data, int idx, int run);

    void close(int run);

}
