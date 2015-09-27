package com.ociweb.pronghorn.util;

public interface MemberHolderVisitor {

    void visit(long value);

    void finished();
    
}
