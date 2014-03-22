package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class FASTDynamicWriter {

	private final FASTWriterDispatch writerDispatch;
	private final TemplateCatalog catalog;
	private final int[] fullScript;
	private final FASTRingBuffer ringBuffer;
	
	private int activeScriptCursor;
	private int activeScriptLimit;
	boolean needTemplate = true;
	
	public FASTDynamicWriter(PrimitiveWriter primitiveWriter, TemplateCatalog catalog, FASTRingBuffer ringBuffer, FASTWriterDispatch writerDispatch) {

		this.writerDispatch = writerDispatch;

		this.catalog = catalog;
		this.fullScript = catalog.fullScript();
		this.ringBuffer = ringBuffer;
	}

	
	//non blocking write, returns if there is nothing to do.
	public void write() {
		//write from the the queue/ringBuffer 
		//queue will contain one full unit to be processed.
		//Unit: 1 Template/Message or 1 EntryInSequence
		
		//because writer does not move pointer up until full unit is ready to go
		//we only need to check if data is available, not the size.
		if (ringBuffer.hasContent()) {
			int idx = 0;			
			if (needTemplate) {
				//template processing (can these be nested?) 
				int templateId = ringBuffer.readInteger(idx); 	
				idx++;
				//tokens - reading 
				activeScriptCursor = catalog.getTemplateStartIdx(templateId);
				activeScriptLimit = catalog.getTemplateLimitIdx(templateId);
			}
						
			do{
				int token = fullScript[activeScriptCursor];
		
				if (writerDispatch.dispatchWriteByToken(token, idx)) {
					//starting a sequence or finished a sequence					
					if (++activeScriptCursor==activeScriptLimit) {
						needTemplate = true;
						idx += stepSizeInRingBuffer(token);
						return;
					}
					int seqScriptLength = TokenBuilder.MAX_INSTANCE&fullScript[++activeScriptCursor];
					if (writerDispatch.isSkippedSequence()) {
						//jump over sequence group in script
						activeScriptCursor += seqScriptLength;
						
					} else if (!writerDispatch.isFirstSequenceItem()) {						
				    	//jump back to top of this sequence in the script.
						System.err.println(TokenBuilder.tokenToString(fullScript[activeScriptCursor])+
								           " jump to "+TokenBuilder.tokenToString(fullScript[activeScriptCursor-seqScriptLength]));
						
						activeScriptCursor -= seqScriptLength;
						
					}
					
					needTemplate = false;
					idx += stepSizeInRingBuffer(token);
					return;
				}
				
				idx += stepSizeInRingBuffer(token);
				
			
			} while (++activeScriptCursor<activeScriptLimit);
			needTemplate = true;

				//TODO: play sequence group
				//if last sequence then needTemplateTrue
				//else loop to top of sequence cursor.
				
			
			
			
			
			//last thing TODO: step is the number of items removed from queue.
			//ringBuffer.removeForward(step);
		}
		
		
		
		//RingBuffer rules
		//Writer will not release the templateId unless all the fields are also released up to sequence or end.
		//Each sequence is released in full. 
		//As a result reader only needs to check for overrun in those two cases.
		
		
		
		////
		//Hack until the move forward is called.
		ringBuffer.dump(); //must dump values in buffer or we will hang when reading.
		
		
	}

	public void reset(boolean clearData) {
		needTemplate=true;
    	if (clearData) {
    		this.writerDispatch.reset();
    	}
	}
	
	
	private int stepSizeInRingBuffer(int token) {
		int stepSize = 0;
		if (0==(token&(16<<TokenBuilder.SHIFT_TYPE))) {
			//0????
			if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
				//00???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					//int
					stepSize = 1;
				} else {
					//long
					stepSize = 2;
				}
			} else {
				//01???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					//int for text (takes up 2 slots)
					stepSize = 2;			
				} else {
					//011??
					if (0==(token&(2<<TokenBuilder.SHIFT_TYPE))) {
						//0110? Decimal and DecimalOptional
						stepSize = 3;
					} else {
						//int for bytes
						stepSize = 0;//BYTES ARE NOT IMPLEMENTED YET BUT WILL BE 2;
					}
				}
			}
		} else {
			if (0==(token&(8<<TokenBuilder.SHIFT_TYPE))) {
				//10???
				if (0==(token&(4<<TokenBuilder.SHIFT_TYPE))) {
					//100??
					//Group Type, no others defined so no need to keep checking
					stepSize = 0;
				} else {
					//101??
					//Length Type, no others defined so no need to keep checking
					//Only happens once before a node sequence so push it on the count stack
					stepSize = 1;
				}
			} else {
				//11???
				//Dictionary Type, no others defined so no need to keep checking
				stepSize = 0;
			}
		}
		
		return stepSize;
	}

}
