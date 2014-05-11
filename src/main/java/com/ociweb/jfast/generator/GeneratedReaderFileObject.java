package com.ociweb.jfast.generator;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import javax.tools.SimpleJavaFileObject;

public class GeneratedReaderFileObject extends SimpleJavaFileObject {

    private final byte[] catBytes;
    
    public GeneratedReaderFileObject(byte[] catBytes) {
        super(generatedSourceURI(),Kind.SOURCE);
        this.catBytes = catBytes;
    }
    
    private static URI generatedSourceURI() {
       
        return new File(FASTDispatchClassLoader.SIMPLE_READER_NAME+".java").toURI();
    }
    
    
    public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        //new instance of code generator
        FASTReaderDispatchGenerator generator = new FASTReaderDispatchGenerator(catBytes);
        //generate new source into the StringBuffer and return it.
        return generator.generateFullReaderSource(new StringBuilder());
    }
    
}
