package com.ociweb.jfast;

import com.ociweb.jfast.stream.FASTWriter;
import com.ociweb.jfast.stream.FASTReader;

/**
 * Use this class to add FASTStreamable capabilities to POJO
 * @author Nathan Tippy
 *
 */
public interface FASTStreamable {

	void write(FASTWriter writer);
	
	void read(FASTReader reader);
	

}
