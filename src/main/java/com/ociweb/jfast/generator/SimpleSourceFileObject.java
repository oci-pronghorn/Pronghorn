package com.ociweb.jfast.generator;

import java.io.File;
import java.io.IOException;

import javax.tools.SimpleJavaFileObject;

public class SimpleSourceFileObject extends SimpleJavaFileObject {

	final CharSequence body;
	final String className;
	
	public SimpleSourceFileObject(String className, CharSequence body) {
		super(new File(className+".java").toURI(),Kind.SOURCE);
        this.className = className;
		this.body = body; 
	}
	
    
    @Override
	public String getName() {
    	return className;
	}

	public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
        //generate new source into the StringBuffer and return it.
        return body;
	}

}
