package com.ociweb.jfast.generator;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

public class FASTWriterSourceFileObject extends SimpleJavaFileObject {

    final byte[] catBytes;
    
    public FASTWriterSourceFileObject(byte[] catBytes) {
        super(new File(FASTClassLoader.SIMPLE_WRITER_NAME+".java").toURI(),Kind.SOURCE);
        this.catBytes = catBytes;
    }
    
    public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        //generate new source into the StringBuffer and return it.
        return new FASTWriterDispatchGenerator(catBytes).generateFullReaderSource(new StringBuilder());
    }
    
}
