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

	void write(FASTStaticWriter writer);
	
	void read(FASTStaticReader reader);
	

}
