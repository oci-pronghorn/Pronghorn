//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;


/**
 * Use this class to add FASTStreamable capabilities to POJO
 * 
 * This technique is only appropriate for static template situations. 
 * If the template will be changing field order/ compression type etc the
 * other class should be used????
 * 
 * @author Nathan Tippy
 *
 */
public interface FASTStreamable { 

	void write(FASTWriterDispatch writer);
	
	void read(FASTReaderDispatch reader);
	

}
