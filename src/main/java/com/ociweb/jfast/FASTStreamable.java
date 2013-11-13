package com.ociweb.jfast;

import com.ociweb.jfast.field.FieldReader;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.stream.FASTStreamer;

/**
 * Use this class to add FASTStreamable capabilities to POJO
 * @author Nathan Tippy
 *
 */
public interface FASTStreamable {

	void write(FASTStreamer writer);
	
	void read(FieldReader reader);
	

}
