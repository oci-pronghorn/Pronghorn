package com.ociweb.pronghorn.util;

import java.util.HashSet;
import java.util.Set;

public interface ByteSquenceVisitor {

    //void end(int value);
    
    //void safePoint(int value);

    //boolean open(short[] data, int idx, int run);

    //void close(int run);
	    
    void addToResult(long l);
	void clearResult();
}
