package com.ociweb.jfast.catalog.extraction;

import java.nio.MappedByteBuffer;

public interface ExtractionVisitor {

    void appendContent(MappedByteBuffer mappedBuffer, int contentPos, int position, boolean contentQuoted);

    void closeRecord();

    void closeField();

    void contextSwitch(); //must use any buffers it has been given because they are about to be changed

}
