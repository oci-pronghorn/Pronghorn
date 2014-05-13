package com.ociweb.jfast.generator;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

public class FASTReaderSourceFileObject extends SimpleJavaFileObject {

    final FASTReaderDispatchGenerator generator;
    
    public FASTReaderSourceFileObject(byte[] catBytes) {
        super(new File(FASTDispatchClassLoader.SIMPLE_READER_NAME+".java").toURI(),Kind.SOURCE);
        //new instance of code generator
        generator = new FASTReaderDispatchGenerator(catBytes);
    }
    
    public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        //generate new source into the StringBuffer and return it.
        return generator.generateFullReaderSource(new StringBuilder());
    }
    
}
