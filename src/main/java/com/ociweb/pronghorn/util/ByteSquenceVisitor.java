package com.ociweb.pronghorn.util;

public interface ByteSquenceVisitor {

    void end(int value);
    
    void safePoint(int value);

    boolean open(short[] data, int idx, int run);

    void close(int run);
    
    void addToResult(long l);

}
