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

	public void write(FASTRingBuffer queue) {
				
		// TODO Auto-generated method stub
		
		//read flags to pick the right scripts
		//play down script to call write methods.
		
		//TODO: atomic stop must be set once per has more call.
		//TODO: only jump to next record instead of dumping all
		queue.dump(); //must dump values in buffer or we will hang when reading.
		
		
	}

}
