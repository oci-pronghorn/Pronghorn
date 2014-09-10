package com.ociweb.jfast.catalog.extraction;

import java.nio.MappedByteBuffer;

public interface ExtractionVisitor {

    void openFrame(); //called before first use on new frame
    
    void appendContent(MappedByteBuffer mappedBuffer, int contentPos, int position, boolean contentQuoted);

    void closeRecord(int startPos);

    void closeField();

    void closeFrame(); //must use any buffers it has been given because they are about to be changed

}
