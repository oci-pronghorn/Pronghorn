package com.ociweb.jfast.stream;

import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTDynamicWriter {

	private final FASTWriterDispatch writerDispatch;
	
	public FASTDynamicWriter(PrimitiveWriter primitiveWriter, TemplateCatalog catalog) {

		writerDispatch = new FASTWriterDispatch(primitiveWriter,
										catalog.dictionaryFactory(),
										catalog.templatesCount());
		
	}

	public void write(int flags, FASTRingBuffer buffer) {
				
		// TODO Auto-generated method stub
		
		//read flags to pick the right scripts
		//play down script to call write methods.
		
		
		
	}

}
