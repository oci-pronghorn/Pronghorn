package com.ociweb.jfast.generator;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

public class FASTReaderSourceFileObject extends SimpleJavaFileObject {

    final byte[] catBytes;
    
    public FASTReaderSourceFileObject(byte[] catBytes) {
        super(new File(FASTClassLoader.SIMPLE_READER_NAME+".java").toURI(),Kind.SOURCE);
        this.catBytes = catBytes;
    }
    
    public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        //generate new source into the StringBuffer and return it.
        return new FASTReaderDispatchGenerator(catBytes).generateFullReaderSource(new StringBuilder());
    }
    
}
