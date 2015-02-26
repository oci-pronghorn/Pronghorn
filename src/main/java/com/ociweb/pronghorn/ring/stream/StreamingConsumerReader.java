package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class StreamingConsumerReader {

	private StreamingConsumer visitor;
	private RingBuffer inputRing;
	private long nextTargetHead;
	private long headPosCache;
	private FieldReferenceOffsetManager from;
	
	private int nestedFragmentDepth;
	private int[] cursorStack;
	private int[] sequenceCounters;
	
	
	public StreamingConsumerReader(RingBuffer inputRing, StreamingConsumer visitor) {
		this.visitor = visitor;
		this.inputRing = inputRing;
		
		this.from = RingBuffer.from(inputRing);		
		this.cursorStack = new int[this.from.maximumFragmentStackDepth];
		this.sequenceCounters = new int[this.from.maximumFragmentStackDepth];
		
		//publish only happens on fragment boundary therefore we can assume that if 
		//we can read 1 then we can read the full fragment
		
		this.nextTargetHead = 1 + tailPosition(inputRing);
		this.headPosCache = headPosition(inputRing);	
		this.nestedFragmentDepth = 0;		
		
		int j = 0; ///debug code to be removed
		while (j<from.tokens.length) {
			System.err.println(j+" "+TokenBuilder.tokenToString(from.tokens[j]));
			j++;
		}
	}
	
//TODO: need test with nested sequence
//TODO: addUTF8 support
//TODO: add the visitor interface.
	
	public void run() {
		
		do {
			
			    if (visitor.paused()) {
			    	return;
			    }
			   //return to try again later if we can not read a fragment
		        if (headPosCache < nextTargetHead) {
					headPosCache = inputRing.headPos.longValue();
					if (headPosCache < nextTargetHead) {
						return; //come back later when we find more content
					}
				}
		        		        
		        int startPos;
		        int cursor;
		        if (0==nestedFragmentDepth) {
		        	//start new message
		        	
		        	//block until one more byteVector is ready.
		        	cursor = RingBuffer.takeMsgIdx(inputRing);
		        	if (cursor<0) {
		        		int zero = RingBuffer.takeValue(inputRing);
		        		assert(0==zero);
		        		return;
		        	}
		        	startPos = 1;//new message so skip over this messageId field
		        	
		        	visitor.visitTemplateOpen(from.fieldNameScript[cursor],from.fieldIdScript[cursor]);
		        	
			        //must the next read position forward by the size of this fragment so next time we confirm that there is a fragment to read.
			        nextTargetHead += from.fragDataSize[cursor];
			        		        		        
			        //visit all the fields in this fragment
			        processFragment(startPos, cursor);
			        
			        inputRing.workingTailPos.value += (from.fragDataSize[cursor]-startPos);
			        
					releaseReadLock(inputRing);
		        	
		        } else {
		        	
		        	cursor = cursorStack[nestedFragmentDepth];
		        	startPos = 0;//this is not a new message so there is no id to jump over.
		        	
		        	
			        //must the next read position forward by the size of this fragment so next time we confirm that there is a fragment to read.
			        nextTargetHead += from.fragDataSize[cursor];
			        		        		        
			        //visit all the fields in this fragment
			        processFragment(startPos, cursor);
			        
			        inputRing.workingTailPos.value += (from.fragDataSize[cursor]-startPos);
			        
					releaseReadLock(inputRing);
		        	
		        	
		        	//decrement one count because we are now doing one pass over the fragment
		        	if(--sequenceCounters[nestedFragmentDepth]<=0) {
		        		visitor.visitSequenceClose(from.fieldNameScript[cursor],from.fieldIdScript[cursor]);
		        		nestedFragmentDepth--; //will become zero so we start a new message
		        	}
		        			        	
		        }

	            	
	        	
		} while(true); //keep running until we run out of content
		
	}

	private void processFragment(int startPos, int cursor) {
		int fieldsInScript = from.fragScriptSize[cursor];
		int i = startPos;
		int idx = 0;
		while (i<fieldsInScript) {
			int j = cursor+i++;
						
			switch (TokenBuilder.extractType(from.tokens[j])) {
				case TypeMask.Group:
					int operator = TokenBuilder.extractOper(from.tokens[j]);
					if (0 == (OperatorMask.Group_Bit_Close&operator)) {
						visitor.visitFragmentOpen(from.fieldNameScript[j],from.fieldIdScript[j]);
					} else {
						visitor.visitFragmentClose(from.fieldNameScript[j],from.fieldIdScript[j]);
					}					
					break;
				case TypeMask.GroupLength:
					assert(i==fieldsInScript) :" this should be the last field";
					int seqLen = RingBuffer.readValue(idx, inputRing);
					idx++;
					nestedFragmentDepth++;
					sequenceCounters[nestedFragmentDepth]= seqLen;
					cursorStack[nestedFragmentDepth] = cursor+fieldsInScript;
					visitor.visitSequenceOpen(from.fieldNameScript[j+1],from.fieldIdScript[j+1],seqLen);
					
					break;
				case TypeMask.IntegerSigned:
					visitor.visitSignedInteger(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readValue(idx++, inputRing));
					break;
				case TypeMask.IntegerUnsigned: //Java does not support unsigned int so we pass it as a long being careful not to get it signed.
					visitor.visitUnsignedInteger(from.fieldNameScript[j],from.fieldIdScript[j],  0xFFFFFFFFl&(long)RingBuffer.readValue(idx++, inputRing));
					break;
				case TypeMask.IntegerSignedOptional:
					{
						int value = RingBuffer.readValue(idx++, inputRing);
						if (FieldReferenceOffsetManager.getAbsent32Value(from)!=value) {
							visitor.visitOptionalSignedInteger(from.fieldNameScript[j],from.fieldIdScript[j],value);
						}
					}
					break;
				case TypeMask.IntegerUnsignedOptional:
					{
						int value = RingBuffer.readValue(idx++, inputRing);
						if (FieldReferenceOffsetManager.getAbsent32Value(from)!=value) {
							visitor.visitOptionalUnsignedInteger(from.fieldNameScript[j],0xFFFFFFFFl&(long)from.fieldIdScript[j],value);
						}
					}
					break;
				case TypeMask.LongSigned:
					{
						visitor.visitSignedLong(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readLong(idx, inputRing));
						idx+=2;
					}	
					break;	
				case TypeMask.LongUnsigned:
					{
						visitor.visitUnsignedLong(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readLong(idx, inputRing));
						idx+=2;
					}	
					break;	
				case TypeMask.LongSignedOptional:
					{
						long value = RingBuffer.readLong(idx, inputRing);
						idx+=2;
						if (FieldReferenceOffsetManager.getAbsent64Value(from)!=value) {
							visitor.visitSignedLong(from.fieldNameScript[j],from.fieldIdScript[j],value);
						}
					}	
					break;		
				case TypeMask.LongUnsignedOptional:
					{
						long value = RingBuffer.readLong(idx, inputRing);
						idx+=2;
						if (FieldReferenceOffsetManager.getAbsent64Value(from)!=value) {
							visitor.visitUnsignedLong(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readLong(idx, inputRing));
						}
					}	
					break;
				case TypeMask.Decimal:
					{
						int exp = RingBuffer.readValue(idx++, inputRing);
						long mant = RingBuffer.readLong(idx, inputRing);
						idx+=2;
						visitor.visitDecimal(from.fieldNameScript[j],from.fieldIdScript[j],exp,mant);
						i++;//add 1 extra because decimal takes up 2 slots in the script
					}
					break;	
				case TypeMask.DecimalOptional:
					{
						int exp = RingBuffer.readValue(idx++, inputRing);
						long mant = RingBuffer.readLong(idx, inputRing);
						idx+=2;
						if (FieldReferenceOffsetManager.getAbsent32Value(from)!=exp) {
							visitor.visitOptionalDecimal(from.fieldNameScript[j],from.fieldIdScript[j],exp,mant);
						}
						i++;//add 1 extra because decimal takes up 2 slots in the script
					}
					break;	
				case TypeMask.TextASCII:
					{						
						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
						int len =  RingBuffer.readRingByteLen(idx, inputRing);
						idx+=2;
						assert(len>=0) : "Optional strings are NOT supported for this type";
						int pos = bytePosition(meta,inputRing,len);    		
						String name = from.fieldNameScript[j];
						long id = from.fieldIdScript[j];
						visitor.visitASCII(name, id, RingBuffer.readASCII(inputRing, visitor.targetASCII(name, id), pos, len));
					}
					break;
				case TypeMask.TextASCIIOptional:
					{						
						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
						int len =  RingBuffer.readRingByteLen(idx, inputRing);
						idx+=2;
						if (len>0) { //a negative length is a null and zero there is no work to do
							int pos = bytePosition(meta,inputRing,len);    		
							String name = from.fieldNameScript[j];
							long id = from.fieldIdScript[j];
							visitor.visitOptionalASCII(name, id, RingBuffer.readASCII(inputRing, visitor.targetOptionalASCII(name, id), pos, len));
						}
					}
					break;
				case TypeMask.TextUTF8:
					{						
						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
						int len =  RingBuffer.readRingByteLen(idx, inputRing);
						idx+=2;
						assert(len>=0) : "Optional strings are NOT supported for this type";
						int pos = bytePosition(meta,inputRing,len);    		
						String name = from.fieldNameScript[j];
						long id = from.fieldIdScript[j];
						visitor.visitUTF8(name, id, RingBuffer.readUTF8(inputRing, visitor.targetUTF8(name, id), pos, len));
					}
					break;						
				case TypeMask.TextUTF8Optional:
					{						
						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
						int len =  RingBuffer.readRingByteLen(idx, inputRing);
						idx+=2;
						if (len>0) { //a negative length is a null and zero there is no work to do
							int pos = bytePosition(meta,inputRing,len);    		
							String name = from.fieldNameScript[j];
							long id = from.fieldIdScript[j];
							visitor.visitOptionalUTF8(name, id, RingBuffer.readUTF8(inputRing, visitor.targetOptionalUTF8(name, id), pos, len));
						}
					}
					break;
				case TypeMask.ByteArray:
					{						
						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
						int len =  RingBuffer.readRingByteLen(idx, inputRing);
						idx+=2;
						assert(len>=0) : "Optional strings are NOT supported for this type";
						int pos = bytePosition(meta,inputRing,len);    		
						String name = from.fieldNameScript[j];
						long id = from.fieldIdScript[j];
						visitor.visitBytes(name, id, RingBuffer.readBytes(inputRing, visitor.targetBytes(name, id), pos, len));
					}
					break;	
				case TypeMask.ByteArrayOptional:
					{						
						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
						int len =  RingBuffer.readRingByteLen(idx, inputRing);
						idx+=2;
						if (len>0) { //a negative length is a null and zero there is no work to do
							int pos = bytePosition(meta,inputRing,len);    		
							String name = from.fieldNameScript[j];
							long id = from.fieldIdScript[j];
							visitor.visitOptionalBytes(name, id, RingBuffer.readBytes(inputRing, visitor.targetOptionalBytes(name, id), pos, len));
						}
					}
					break;
		    	default: System.err.println("unknown "+TokenBuilder.tokenToString(from.tokens[j]));
			}
		}
		//inputRing.workingTailPos.value+=idx;
	}
	
}
