package com.ociweb.pronghorn.components.ingestion.csv;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.readBytesAndreleaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.TypeExtractor;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Split CSV lines into meta messages
 * @author Nathan Tippy
 *
 */
public class FieldSplitterStage extends PronghornStage {

	private final RingBuffer inputRing;
	private final RingBuffer outputRing;	
	private final TypeExtractor typeExtractor;   
	private final Logger log = LoggerFactory.getLogger(FieldSplitterStage.class);
	
	 
	private static final int step =  FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
	
    private static final byte[] quoter;
    static {
    	quoter = new byte[256]; //these are all zeros
    	quoter['"'] = 1; //except for the value of quote.
    }
     
	public FieldSplitterStage(GraphManager graphManager, RingBuffer inputRing, RingBuffer outputRing) {
		super(graphManager,inputRing,outputRing);
		this.inputRing = inputRing;
		this.outputRing = outputRing;
		
		if (RingBuffer.from(inputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages for input.");
		}
		
		if (RingBuffer.from(outputRing) != MetaMessageDefs.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the MetaFieldFROM catalog of messages for output.");
		}
		
		typeExtractor = new TypeExtractor(true /* force ASCII */);	
		
	}
	
	public void run() {
		
			//read from the byte stream which is already chunked by lines
     		readData(this, inputRing, outputRing);

	}
	
	@Override
	public void shutdown() {
		
		endOfData(outputRing);
	}
	
	public void readData(FieldSplitterStage stage, RingBuffer inputRing, RingBuffer outputRing) {
	
		//return if there is no data found
		while (RingBuffer.contentToLowLevelRead(inputRing, step)) {

	    	int msgIdx = RingBuffer.takeMsgIdx(inputRing);
	    				
	    	if (msgIdx<0) { //exit logic
	    		new Exception("warning old exit used").printStackTrace(); //DELETE this code
	    	} else {   
	    		RingBuffer.confirmLowLevelRead(inputRing,  step);
	    		
	    		
	    		int meta = takeRingByteMetaData(inputRing);
	        	int len = takeRingByteLen(inputRing);
	        	int mask = byteMask(inputRing);	
	        	int pos = bytePosition(meta, inputRing, len)&mask;     		
				byte[] data = byteBackingArray(meta, inputRing);
				   						    		
	    		beginningOfLine(outputRing);
					    		
	    		int len1 = 1+mask - pos;
				if (len1>=len) {
					//simple add bytes					
					consumeBytes(stage.typeExtractor, outputRing, data, pos, len); 
				} else {			
					//rolled over the end of the buffer
					consumeBytes(stage.typeExtractor, outputRing, data, pos, len1, 0, len - len1);	
				}
				
				endOfLine(outputRing);
				//done reading bytes input can have that section of the array again.
	
	    	}
	    	readBytesAndreleaseReadLock(inputRing);
		}
		
	}


	private static void consumeBytes(TypeExtractor typeExtractor, RingBuffer output, byte[] data, int offset1, int length1, int offset2, int length2) {
				
		assert(length1>=0) : "bad length "+length1;
		assert(length2>=0) : "bad length "+length2;
		
		byte prevB = 0;		
		int quoteCount = 0;
		int i = offset1;
		int fieldStart = offset1;
		int lineStop = length1+offset1;
		int fieldIdx = 0;
		
		while (i<lineStop) {			
			if (',' == data[i] && '\\'!=prevB) {
				//send the new field as a message up to this point.
				consumeField(fieldIdx++, typeExtractor, output, data, fieldStart, i-fieldStart);
				fieldStart = i+1;
			}
			prevB = data[i++];
			quoteCount += quoter[0xFF&prevB];
		}
		int lastStart = fieldStart;//keep these two values in order to span the gap to the second set of data
		int lastStop  = lineStop;
		
        //middle span
		i = offset2;
		fieldStart = offset2;
		lineStop = length2 + offset2;
		while (i<lineStop) {			
			if (',' == data[i] && '\\'!=prevB && (quoteCount&1)==0) {
				//send the new field as a message up to this point.				
				if (lastStart==lastStop) {
					consumeField(fieldIdx++, typeExtractor, output, data, fieldStart, i-fieldStart);
				} else {
					consumeField(fieldIdx++, typeExtractor, output, data, lastStart, lastStop-lastStart, fieldStart, i-fieldStart);
				}
				fieldStart = i+1;
				prevB = data[i++];
				break;//go to tail bytes that do not need to deal with both blocks
			}
			prevB = data[i++];
			quoteCount += quoter[0xFF&prevB];
		}
		if (i==lineStop) {
			//send the new field as a message up to this point. also this is the last field
			if (lastStart==lastStop) {
				consumeField(fieldIdx++, typeExtractor, output, data, fieldStart, lineStop-fieldStart);	
			} else {
				consumeField(fieldIdx++, typeExtractor, output, data, lastStart, lastStop-lastStart, fieldStart, lineStop-fieldStart);			
			}
		} else {
		
			//tail bytes
			while (i<lineStop) {			
				if (',' == data[i] && '\\'!=prevB && (quoteCount&1)==0) {
					//send the new field as a message up to this point.
					consumeField(fieldIdx++, typeExtractor, output, data, fieldStart, i-fieldStart);
					fieldStart = i+1;
				}
				prevB = data[i++];
				quoteCount += quoter[0xFF&prevB];
			}			
			//last field at the end of length
			consumeField(fieldIdx++, typeExtractor, output, data, fieldStart, lineStop-fieldStart);
		}
		
		
	}
	
	
	private static void consumeBytes(TypeExtractor typeExtractor, RingBuffer output, byte[] data, int offset, int length) {
				
		
		int fieldIdx = 0;
		byte prevB = 0;
		int quoteCount = 0;
		int i = offset;
		int fieldStart = offset;
		int lineStop = length+offset;
		while (i<lineStop) {			
			if (',' == data[i] && '\\'!=prevB && (quoteCount&1)==0) {
				//send the new field as a message up to this point.				
				consumeField(fieldIdx++, typeExtractor, output, data, fieldStart, i-fieldStart);
				fieldStart = i+1;
			}
			prevB = data[i++];
			quoteCount += quoter[0xFF&prevB];
		}
		//last field at the end of length
		consumeField(fieldIdx++, typeExtractor, output, data, fieldStart, lineStop-fieldStart);
				
	}
		
	private static void consumeField(int fieldIdx, TypeExtractor typeExtractor, RingBuffer output, byte[] data, int offset, int length) {
		TypeExtractor.resetFieldSum(typeExtractor);
		TypeExtractor.appendContent(typeExtractor, data, offset, offset+length);
						
		//NOTE: in this case fieldIdx is not used however it holds the column number starting with zero
		

	//	System.err.println("xxxxx "+new String(data,offset,length));
		
		
		writeMetaMessage(typeExtractor, data, offset, length, output);
		
		
		//TODO: As an alternate implementation we can 
		//            * Open a message of type X in beginningOfLine() ByteBuffer.addMessageIDx(x)
		//            * For every call including zero lookup the type and use they ByteBuffer.add XXX
		//            * change endOfData to use RingBuffer.publishEOF(ring);
		//            * publish in the endOfLine() method.
		//TODO: The best approach would be to have these code generated from the template file  - see JavaPoet See YF
		//        using the high level API to write would allow us to write the fields in any order that they arrive.
		//      WAIT: may not need code generation. If we pass in array of strings that represent the fields in order
		//         Then on startup convert those strings to an array of LOCs
		//         then on parse use fieldIdx to look up the LOC and type to do the "right thing" 
		//             the switch would be a conditional that could be removed by code generation.... but only if needed.
		
		
	}
	
	
	private static void consumeField(int fieldIdx, TypeExtractor typeExtractor, RingBuffer output, byte[] data, int offset1, int length1, int offset2, int length2) {
		TypeExtractor.resetFieldSum(typeExtractor);
		assert(length1>=0) : "bad length "+length1;
		TypeExtractor.appendContent(typeExtractor, data, offset1, offset1+length1);
		assert(length2>=0) : "bad length "+length2;
		TypeExtractor.appendContent(typeExtractor, data, offset2, offset2+length2);
				
		//NOTE: in this case fieldIdx is not used however it holds the column number starting with zero
				
		writeMetaMessage(typeExtractor, data, offset1, length1, offset2, length2, output);
	}

	
	///////////////////////////////////////////////////////////////////////////////
	//All the code after this point is for converting these fields into meta-messages
	///////////////////////////////////////////////////////////////////////////////
	
	
	private static void writeMetaMessage(TypeExtractor typeExtractor,
			byte[] data, int offset, int length, RingBuffer output) {
		
		//RingBuffer.spinBlockOnTailTillMatchesHead(output.tailPos.get(), output);
		//spinBlockOnTail(output.tailPos.get(), output.workingHeadPos.value-(output.maxSize-FieldReferenceOffsetManager.maxFragmentSize(RingBuffer.from(output))), output);
		
		switch (TypeExtractor.extractType(typeExtractor)) {
		
			case TypeExtractor.TYPE_UINT:
					writeUInt(typeExtractor, output);
				break;
			case TypeExtractor.TYPE_SINT:
				    writeInt(typeExtractor, output);					
				break;
    		case TypeExtractor.TYPE_ULONG:
    				writeULong(typeExtractor, output);	    			
    			break;
	    	case TypeExtractor.TYPE_SLONG:
	    			writeLong(typeExtractor, output); 				
				break;
		    case TypeExtractor.TYPE_ASCII:
		    	   writeASCII(data, offset, length, output);
				break; 
		    case TypeExtractor.TYPE_BYTES:
				   writeBytes(data, offset, length, output);		    	
		    	break; 
		    case TypeExtractor.TYPE_DECIMAL:
				   writeDecimal(typeExtractor, output);		    			    	
		    	break;
			case TypeExtractor.TYPE_NULL:
				  writeNull(output);				
				break;
		}
		RingBuffer.setPublishBatchSize(output,  0);
		RingBuffer.publishWrites(output);
	}


	private static void writeMetaMessage(TypeExtractor typeExtractor,
			byte[] data, int offset1, int length1, int offset2, int length2,
			RingBuffer output) {
		
		//RingBuffer.spinBlockOnTailTillMatchesHead(output.tailPos.get(), output);
		//spinBlockOnTail(output.tailPos.get(), output.workingHeadPos.value-(output.maxSize-FieldReferenceOffsetManager.maxFragmentSize(RingBuffer.from(output))), output);
		
		switch (TypeExtractor.extractType(typeExtractor)) {
		
			case TypeExtractor.TYPE_UINT:
				writeUInt(typeExtractor, output);
				break;
			case TypeExtractor.TYPE_SINT:
				writeInt(typeExtractor, output);
				break;
			case TypeExtractor.TYPE_ULONG:
				writeULong(typeExtractor, output);
				break;
	    	case TypeExtractor.TYPE_SLONG:
	    		writeLong(typeExtractor, output); 	
				break;
		    case TypeExtractor.TYPE_ASCII:
			    writeASCIISplit(data, offset1, length1, offset2, length2, output);			    			    	
				break; 
		    case TypeExtractor.TYPE_BYTES:
				writeBytesSplit(data, offset1, length1, offset2, length2, output);		    	
		    	break; 
		    case TypeExtractor.TYPE_DECIMAL:
				writeDecimal(typeExtractor, output);			    			    	
		    	break;
			case TypeExtractor.TYPE_NULL:
				writeNull(output);				
				break;	
		}
		RingBuffer.setPublishBatchSize(output,  0);
		RingBuffer.publishWrites(output);
	}

	
	
	
	public static void writeBytesSplit(byte[] data, int offset1, int length1,
			int offset2, int length2, RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_BYTEARRAY_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_BYTEARRAY_LOC);
			
		int	bytePosition = RingBuffer.bytesWorkingHeadPosition(output);		    	
		RingBuffer.copyBytesFromToRing(data, offset1, Integer.MAX_VALUE, output.unstructuredLayoutRingBuffer, bytePosition, output.byteMask, length1);
		RingBuffer.copyBytesFromToRing(data, offset2, Integer.MAX_VALUE, output.unstructuredLayoutRingBuffer, bytePosition+length1, output.byteMask, length2);
		int length3 = length1+length2;
			
		RingBuffer.validateVarLength(output, length3);
		RingBuffer.addBytePosAndLen(output, bytePosition, length3);
		RingBuffer.setBytesWorkingHead(output, bytePosition + length3);
	}

	public static void writeASCIISplit(byte[] data, int offset1, int length1,
			int offset2, int length2, RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_ASCII_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_ASCII_LOC);
			
		int bytePosition = RingBuffer.bytesWorkingHeadPosition(output);
		RingBuffer.copyBytesFromToRing(data, offset1, Integer.MAX_VALUE, output.unstructuredLayoutRingBuffer, bytePosition, output.byteMask, length1);
		RingBuffer.copyBytesFromToRing(data, offset2, Integer.MAX_VALUE, output.unstructuredLayoutRingBuffer, bytePosition+length1, output.byteMask, length2);
		int length = length1+length2;

		RingBuffer.validateVarLength(output, length);
		RingBuffer.addBytePosAndLen(output,bytePosition, length);
		RingBuffer.setBytesWorkingHead(output, bytePosition + length);
	}

	public static void writeNull(RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_NULL_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_NULL_LOC);
	}

	public static void writeDecimal(TypeExtractor typeExtractor,
			RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_DECIMAL_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_DECIMAL_LOC);	
		RingBuffer.addDecimal(TypeExtractor.decimalPlaces(typeExtractor), typeExtractor.activeFieldLong*TypeExtractor.signMult(typeExtractor), output);
	}

	public static void writeBytes(byte[] data, int offset, int length,
			RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_BYTEARRAY_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_BYTEARRAY_LOC);
		RingBuffer.addByteArray(data, offset, length, output);
	}

	public static void writeASCII(byte[] data, int offset, int length, RingBuffer output) {

		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_ASCII_LOC]), output);
		
		RingBuffer.addMsgIdx(output,MetaMessageDefs.MSG_ASCII_LOC);
		
		
		RingBuffer.addByteArray(data, offset, length, output);
	}

	public static void writeLong(TypeExtractor typeExtractor, RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_INT64_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_INT64_LOC);
		RingBuffer.addLongValue(output.structuredLayoutRingBuffer, output.mask, RingBuffer.getWorkingHeadPositionObject(output), typeExtractor.activeFieldLong*(long)TypeExtractor.signMult(typeExtractor));
	}

	public static void writeULong(TypeExtractor typeExtractor, RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_UINT64_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_UINT64_LOC);
		RingBuffer.addLongValue(output.structuredLayoutRingBuffer, output.mask, RingBuffer.getWorkingHeadPositionObject(output), typeExtractor.activeFieldLong);
	}

	public static void writeInt(TypeExtractor typeExtractor, RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_INT32_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_INT32_LOC);
		RingBuffer.setValue(output.structuredLayoutRingBuffer,output.mask,RingBuffer.getWorkingHeadPositionObject(output).value++,((int)typeExtractor.activeFieldLong)*TypeExtractor.signMult(typeExtractor));		
			
			TypeExtractor.signMult(typeExtractor);
	}

	public static void writeUInt(TypeExtractor typeExtractor,
			RingBuffer output) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(output), RingBuffer.workingHeadPosition(output)-(output.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(output).fragDataSize[MetaMessageDefs.MSG_UINT32_LOC]), output);
		
		RingBuffer.addMsgIdx(output, MetaMessageDefs.MSG_UINT32_LOC);
		RingBuffer.setValue(output.structuredLayoutRingBuffer,output.mask,RingBuffer.getWorkingHeadPositionObject(output).value++,(int)typeExtractor.activeFieldLong);
	}
	
	
	private static void endOfData(RingBuffer ring) {
		
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(ring), RingBuffer.workingHeadPosition(ring)-(ring.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(ring).fragDataSize[MetaMessageDefs.MSG_FLUSH]), ring);
		
		RingBuffer.addMsgIdx(ring, MetaMessageDefs.MSG_FLUSH);
		RingBuffer.publishWrites(ring);
		RingBuffer.publishAllBatchedWrites(ring);
	}

	private static void beginningOfLine(RingBuffer ring) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(ring), RingBuffer.workingHeadPosition(ring)-(ring.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(ring).fragDataSize[MetaMessageDefs.MSG_MESSAGE_BEGIN_LOC]), ring);
		
		RingBuffer.addMsgIdx(ring, MetaMessageDefs.MSG_MESSAGE_BEGIN_LOC);
		RingBuffer.publishWrites(ring);
	}
	
	private static void endOfLine(RingBuffer ring) {
		//before write make sure the tail is moved ahead so we have room to write
		spinBlockOnTail(RingBuffer.tailPosition(ring), RingBuffer.workingHeadPosition(ring)-(ring.sizeOfStructuredLayoutRingBuffer-RingBuffer.from(ring).fragDataSize[MetaMessageDefs.MSG_MESSAGE_END_LOC]), ring);
		
		RingBuffer.addMsgIdx(ring, MetaMessageDefs.MSG_MESSAGE_END_LOC);
		RingBuffer.publishWrites(ring);
	}

}
