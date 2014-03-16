package com.ociweb.jfast.stream;

import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTDynamicWriter {

	private final FASTWriterDispatch writerDispatch;
	private final TemplateCatalog catalog;
	private final int[] fullScript;
	
	private int activeScriptCursor;
	private int activeScriptLimit;
	
	public FASTDynamicWriter(PrimitiveWriter primitiveWriter, TemplateCatalog catalog) {

		this.writerDispatch = new FASTWriterDispatch(primitiveWriter,
										catalog.dictionaryFactory(),
										catalog.templatesCount(), 64, 64, 8, 8);
		this.catalog = catalog;
		this.fullScript = catalog.fullScript();
		
	}

	public void write(FASTRingBuffer queue) {
				
		
		//	random access to fields is supported in the ring buffer however the dynamic writer
	    //  requires the queue to have all the fields in the right order to speed encoding.
		//  Once a message/sequence is written the queue position is moved forward.
		
		
		if (queue.isBlocked(1)) {
			//TODO: what to do if can not read next?
			return;//try again later
		};
		
		//RingBuffer rules
		//Writer will not release the templateId unless all the fields are also released up to sequence or end.
		//Each sequence is released in full. 
		//As a result reader only needs to check for overrun in those two cases.
		
		
		
		int idx = 0;
		int templateId = queue.readInteger(idx); 				
		//tokens - reading 
		activeScriptCursor = catalog.getTemplateStartIdx(templateId);
		activeScriptLimit = catalog.getTemplateLimitIdx(templateId);
		
		int token = 0;
		writerDispatch.dispatchWriteByToken(token,queue,idx);
		
		
		
		////
		//Hack until the move forward is called.
		queue.dump(); //must dump values in buffer or we will hang when reading.
		
		
	}

}
