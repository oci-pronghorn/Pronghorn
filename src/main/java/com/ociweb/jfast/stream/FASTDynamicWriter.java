package com.ociweb.jfast.stream;

import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTDynamicWriter {

	private final FASTWriterDispatch writerDispatch;
	private final TemplateCatalog catalog;
	
	
	public FASTDynamicWriter(PrimitiveWriter primitiveWriter, TemplateCatalog catalog) {

		this.writerDispatch = new FASTWriterDispatch(primitiveWriter,
										catalog.dictionaryFactory(),
										catalog.templatesCount());
		this.catalog = catalog;
		
	}

	public void write(FASTRingBuffer queue) {
				
		
	//	int step = 1;//need to know the size of this record.
		
	//	queue.removeForward(step);
		
		int tokenId = queue.readInteger(0);
		
		//tokens - reading 
		int[] fullScript = catalog.fullScript();
		
		
		
		//queue.readInteger(catalog.fieldIdx(x));
		
		
		//read flags to pick the right scripts
		//play down script to call write methods.
		
		//TODO: atomic stop must be set once per has more call.
		//TODO: only jump to next record instead of dumping all
		queue.dump(); //must dump values in buffer or we will hang when reading.
		
		
	}

}
