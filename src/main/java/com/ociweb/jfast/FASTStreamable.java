package com.ociweb.jfast;

import com.ociweb.jfast.field.FieldReader;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.stream.FASTWriter;

/**
 * Use this class to add FASTStreamable capabilities to POJO
 * @author Nathan Tippy
 *
 */
public interface FASTStreamable {

	void write(FASTWriter writer);
	
	void read(FieldReader reader);
	

}
