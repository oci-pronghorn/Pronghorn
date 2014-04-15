package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.FieldReaderText;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FASTReaderDispatchGenExample extends FASTReaderDispatch {

	public FASTReaderDispatchGenExample(PrimitiveReader reader, DictionaryFactory dcr, int nonTemplatePMapSize,
			int[][] dictionaryMembers, int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
			int maxNestedGroupDepth, int ringBits, int ringTextBits) {
		super(reader, dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap, fullScript,
				maxNestedGroupDepth, ringBits, ringTextBits);
	}


	final int constIntAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
	final long constLongAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
	
    //TODO: B, this code generation must take place when loading the catalog binary file. So catalog file can be the same cross languages.
	//TODO: B, copy other dispatch and use it for code generation, if possible build generator that makes use of its own source as template.
	
	
	public boolean dispatchReadByToken() {
		switch(activeScriptCursor) {
			 case 0:		
  				    assert(gatherReadData(reader,0));
				 	return case0();
			 case 1:
  				    assert(gatherReadData(reader,1));
				 	return case1();
			 case 9:
				 	assert(gatherReadData(reader,9));
				 	return case9();
			 case 30:
				 	assert(gatherReadData(reader,30));
				 	return case30();
			 default:
   		    		assert(false) : "Unsupported Template";
			    	return false;				 
		}		
	}

	private boolean case1() {
		int length = case1a();
		if (length==0) {
			    //jumping over sequence (forward) it was skipped (rare case)
				activeScriptCursor = 31;//9+22;
				return false;
		} 		
		sequenceCountStack[++sequenceCountStackHead] = length;
		return case9();
	}

	private boolean case0() {
		//write a NonNull constant, no need to check more because this can not be null or dynamic.
		  queue.appendInt2(0x80000000,0x02); //ref and length,  ASCIIConstant 0xa02c0000
		  activeScriptCursor = 1;
		  return false;
	}

	private int case1a() {
		   case1reset();
		   
		   //textHeap ID and known fixed length
		   queue.appendInt6(0x80000001,0x03, //ASCIIConstant 0xa02c0001
		                    0x80000002,0x01, //ASCIIConstant 0xa02c0002
		                    0x80000003,0x0d); //ASCIIConstant 0xa02c0003
		   
		   //TODO: B, Code gen, we DO want to use this dictiionary and keep NONE opp operators IFF they are referenced by other fields.

		   queue.appendInt3(reader.readIntegerUnsigned(),
				            reader.readIntegerUnsigned(),
				            reader.readIntegerUnsigned());
				
		   //TODO: A, must return the sequence count this may work for RingBuffer?
		   return queue.appendInt1(reader.readIntegerUnsigned());  //readIntegerUnsigned(0xd00c0003)); 

	}

	private void case1reset() {
		
		   expDictionary[0] = expInit[0];
		   mantDictionary[0] = mantInit[0];
		   expDictionary[1] = expInit[1];
		   mantDictionary[1] = mantInit[1];
		   readerText.heap.reset(4);
		   rIntDictionary[4] = rIntInit[4];
		   rIntDictionary[8] = rIntInit[8];
		   rIntDictionary[9] = rIntInit[9];
		   rIntDictionary[10] = rIntInit[10];
		   rIntDictionary[11] = rIntInit[11];
		   rIntDictionary[12] = rIntInit[12];
	}

	private boolean case9() {
		
		reader.openPMap(nonTemplatePMapSize);
		
		case9a1();
		case9a2();	
		case9a3();	
		case9a4();

		closeGroup(0xc0dc0014);
		activeScriptCursor = 29;
		assert(gatherReadData(reader,29));
		return checkSequence != 0 && completeSequence(0x014); 
	}

	private void case9a2() {
		long xl1;
		queue.appendInt6(reader.readIntegerSignedDefault(0 /* default value */),
				         (int) ((xl1 = reader.readLongSignedDelta(0x00, 0x00, mantDictionary)) >>> 32),
				         (int) (xl1 & 0xFFFFFFFF),
				         reader.readIntegerUnsignedCopy(0x0a, 0x0a, rIntDictionary),
				         reader.readIntegerSignedDeltaOptional(0x0b, 0x0b, rIntDictionary,constIntAbsent),
				         reader.readIntegerUnsignedDeltaOptional(0x0c, 0x0c, rIntDictionary,constIntAbsent));
	}

	private void case9a1() {
		int xi1;
		int xi2;
		queue.appendInt8(reader.readIntegerUnsignedCopy(0x04, 0x04, rIntDictionary),
				         reader.readIntegerUnsignedDefaultOptional(1 /*default or absent value */, constIntAbsent),
		                 textIdRef(xi1 = readerText.readASCIICopy(0x04), xi2 = readerText.textHeap().length2(xi1)),
		                 xi2,
		                 (xi1 = reader.readIntegerUnsigned()) == 0 ? constIntAbsent : xi1 - 1,
		                 9, //dictionary1[0x07],//constant?
		                 reader.readIntegerUnsignedCopy(0x08, 0x08, rIntDictionary),
		                 reader.readIntegerUnsignedIncrement(0x09, 0x09, rIntDictionary)
		                );//not used if null 
	}

	private void case9a4() {
		int xi1;
		int xi2; //TODO: X, Read next 4 bits in one byte and mask them off as needed here to minimize calls.
		queue.appendInt7(//not used if null
		                 textIdRef(xi1 = reader.popPMapBit()==0 ? (FieldReaderText.INIT_VALUE_MASK|0x07) : readerText.readASCIIToHeap(0x07), xi2 = readerText.textHeap().length2(xi1)),xi2,
		                 textIdRef(xi1 = reader.popPMapBit()==0 ? (FieldReaderText.INIT_VALUE_MASK|0x08) : readerText.readASCIIToHeap(0x08), xi2 = readerText.textHeap().length2(xi1)),xi2,
		                 reader.readIntegerUnsignedDefaultOptional(2147483647 ,constIntAbsent),
		                 textIdRef(xi1 = reader.popPMapBit()==0 ? (FieldReaderText.INIT_VALUE_MASK|0x09) : readerText.readASCIIToHeap(0x09), xi2 = readerText.textHeap().length2(xi1)),xi2);//not used if null
	}

	private void case9a3() {
		int xi1;
		int xi2;
		long xl1;
		
		queue.appendInt8(textIdRef(xi1 = reader.popPMapBit()==0 ? (FieldReaderText.INIT_VALUE_MASK|0x05) : readerText.readASCIIToHeap(0x05), xi2 = readerText.textHeap().length2(xi1)),xi2,
						reader.readIntegerSignedDefaultOptional(2147483647 /* default or absent value */,constIntAbsent),
						(int) ((xl1 = reader.readLongSignedDeltaOptional(0x01, 0x01, mantDictionary, constLongAbsent)) >>> 32),
						(int) (xl1 & 0xFFFFFFFF),
						reader.readIntegerUnsignedDefaultOptional(2147483647, constIntAbsent),
						textIdRef(xi1 = reader.popPMapBit()==0 ? (FieldReaderText.INIT_VALUE_MASK|0x06) : readerText.readASCIIToHeap(0x06), xi2 = readerText.textHeap().length2(xi1)),xi2
						);
	}



	private boolean case30() {		
 
		case30a();
				
		int length2= queue.appendInt1(reader.readIntegerUnsigned());// readIntegerUnsigned(0xd00c0011));
		if (length2==0) {
		    //jumping over sequence (forward) it was skipped (rare case)
			activeScriptCursor = 46;//36+10;
			return false;
		} else {			
			sequenceCountStack[++sequenceCountStackHead] = length2;
		}		

		reader.openPMap(nonTemplatePMapSize);
		
		queue.appendInt2(0x8000000e,0x0); //ASCIIConstant(0xa02c000e
				
		case30b();

		closeGroup(0xc0dc0008);
		activeScriptCursor = 45;
		return checkSequence!=0 && completeSequence(0x008);

	}

	private void case30a() {
		//write a NonNull constant, no need to check more because this can not be null or dynamic.
		queue.appendInt8(0x8000000a,0x5,//ASCIIConstant 0xa02c000a
					     0x8000000b,0x0,//ASCIIConstant(0xa02c000b
		                 0x8000000c,0x0, ///ASCIIConstant(0xa02c000c
		                 reader.readIntegerUnsigned(),
				         reader.readIntegerUnsigned());

		int xi1;
		int xi2;
		queue.appendInt2(textIdRef(xi1 = readerText.readASCIIToHeap(0x0d), xi2 = readerText.textHeap().length2(xi1)),xi2);//not used if null//normal read without constant, may need copy
	}

	private void case30b() {
		long xl1;
		queue.appendInt8((int)((xl1 = reader.readLongUnsignedOptional(constLongAbsent))>>>32),
				         (int)(xl1&0xFFFFFFFF),
				         reader.readIntegerUnsignedDefaultOptional(1/*default or optional */, constIntAbsent),
                         (int)((xl1 = reader.readLongUnsigned(0x01, rLongDictionary))>>>32),
				         (int)(xl1&0xFFFFFFFF),
				         reader.readIntegerUnsignedDefault(1/*default value*/),
				         reader.readIntegerUnsigned(),
				         rIntDictionary[0x15]);
	}
	



	private int textIdRef(int heapId, int length) {
		return heapId<0 ? heapId : queue.writeTextToRingBuffer(heapId, length);
	}
	
}
