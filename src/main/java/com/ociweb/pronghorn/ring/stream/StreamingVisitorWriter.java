package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.releaseReadLock;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class StreamingVisitorWriter {

	private StreamingReadVisitor visitor;
	private RingBuffer outputRing;
//	private long nextTargetHead;
//	private long headPosCache;
	private FieldReferenceOffsetManager from;
	
	private int nestedFragmentDepth;
	private int[] cursorStack;
	private int[] sequenceCounters;
	
	//TODO: Just starting to rough in this class based on the reader.
	
	public StreamingVisitorWriter(RingBuffer outputRing, StreamingReadVisitor visitor) {
		this.visitor = visitor;
		this.outputRing = outputRing;
		
		this.from = RingBuffer.from(outputRing);	
		
		this.cursorStack = new int[this.from.maximumFragmentStackDepth];
		this.sequenceCounters = new int[this.from.maximumFragmentStackDepth];
		
		//publish only happens on fragment boundary therefore we can assume that if 
		//we can read 1 then we can read the full fragment
		
//		this.nextTargetHead = 1 + tailPosition(outputRing);
//		this.headPosCache = headPosition(outputRing);	
		this.nestedFragmentDepth = -1;		

		//debugFROM(from);
		
	}

	public void run() {
		
		while (!visitor.paused()) {	
			    
//			    //return to try again later if we can not read a fragment
//		        if (headPosCache < nextTargetHead) {
//					headPosCache = inputRing.headPos.longValue();
//					if (headPosCache < nextTargetHead) {
//						return; //come back later when we find more content
//					}
//				}
		        		        
		        int startPos;
		        int cursor;

		        if (nestedFragmentDepth<0) {	
		        	//start new message
		        	
//		        	//block until one more byteVector is ready.
//		        	cursor = RingBuffer.takeMsgIdx(inputRing);
//		        	if (cursor<0) {
//		        		int zero = RingBuffer.takeValue(inputRing);
//		        		assert(0==zero);
//		        		RingBuffer.releaseAll(inputRing);
//		        		return;
//		        	}
//		        	startPos = 1;//new message so skip over this messageId field
//		        	
//		        	visitor.visitTemplateOpen(from.fieldNameScript[cursor],from.fieldIdScript[cursor]);
//		        	
//			        //must the next read position forward by the size of this fragment so next time we confirm that there is a fragment to read.
//			        nextTargetHead += from.fragDataSize[cursor];
//			        		        		        
//			        //visit all the fields in this fragment
//			        processFragment(startPos, cursor);
//			        
//			        inputRing.workingTailPos.value += (from.fragDataSize[cursor]-startPos);
			        
		        	
		        } else {
		        	
//		        	cursor = cursorStack[nestedFragmentDepth];
//		        	startPos = 0;//this is not a new message so there is no id to jump over.
//		        			        	
//			        //must the next read position forward by the size of this fragment so next time we confirm that there is a fragment to read.
//			        nextTargetHead += from.fragDataSize[cursor];
//			        			        
//			        //visit all the fields in this fragment
//			        processFragment(startPos, cursor);
//			        
//			        inputRing.workingTailPos.value += (from.fragDataSize[cursor]-startPos);
			        
		        	
		        }
//		        releaseReadLock(inputRing);
		}
		
	}

	private void processFragment(int startPos, int cursor) {
		int fieldsInScript = from.fragScriptSize[cursor];
		int i = startPos;
		int idx = 0;
		while (i<fieldsInScript) {
			int j = cursor+i++;
			
			switch (TokenBuilder.extractType(from.tokens[j])) {
				case TypeMask.Group:
//					if (FieldReferenceOffsetManager.isGroupOpen(from, j)) {
//						visitor.visitFragmentOpen(from.fieldNameScript[j],from.fieldIdScript[j]);
//					} else {				
//						do {//close this member of the sequence or template
//							String name = from.fieldNameScript[j];
//							long id = from.fieldIdScript[j];
//							
//							//if this was a close of sequence count down so we now when to close it.
//							if (FieldReferenceOffsetManager.isGroupOpenSequence(from, j)) {
//								visitor.visitFragmentClose(name,id);
//								//close of one sequence member
//								if (--sequenceCounters[nestedFragmentDepth]<=0) {
//									//close of the sequence
//									visitor.visitSequenceClose(name,id);
//									nestedFragmentDepth--; //will become zero so we start a new message
//								} else {
//									break;
//								}
//							} else {
//								visitor.visitTemplateClose(name,id);
//								//this close was not a sequence so it must be the end of the message
//								nestedFragmentDepth = -1;
//								return;//must exit so we do not pick up any more fields
//							}
//						} while (++j<from.tokens.length && FieldReferenceOffsetManager.isGroupClosed(from, j) );
//						//if the stack is empty set the continuation for fields that appear after the sequence
//						if (j<from.tokens.length && !FieldReferenceOffsetManager.isGroup(from, j)) {
//							cursorStack[++nestedFragmentDepth] = j;
//						}
//						return;//this is always the end of a fragment
//					}					
					break;
				case TypeMask.GroupLength:
//					assert(i==fieldsInScript) :" this should be the last field";
//					int seqLen = RingBuffer.readValue(idx, inputRing);
//					idx++;
//					nestedFragmentDepth++;
//					sequenceCounters[nestedFragmentDepth]= seqLen;
//					cursorStack[nestedFragmentDepth] = cursor+fieldsInScript;
//										
//					visitor.visitSequenceOpen(from.fieldNameScript[j+1],from.fieldIdScript[j+1],seqLen);
//					//do not pick up the nestedFragmentDepth adjustment, exit now because we know 
//					//group length is always the end of a fragment
					return; 					
				case TypeMask.IntegerSigned:
//					visitor.visitSignedInteger(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readValue(idx++, inputRing));
					break;
				case TypeMask.IntegerUnsigned: //Java does not support unsigned int so we pass it as a long being careful not to get it signed.
//					visitor.visitUnsignedInteger(from.fieldNameScript[j],from.fieldIdScript[j],  0xFFFFFFFFl&(long)RingBuffer.readValue(idx++, inputRing));
					break;
				case TypeMask.IntegerSignedOptional:
//					{
//						int value = RingBuffer.readValue(idx++, inputRing);
//						if (FieldReferenceOffsetManager.getAbsent32Value(from)!=value) {
//							visitor.visitSignedInteger(from.fieldNameScript[j],from.fieldIdScript[j],value);
//						}
//					}
					break;
				case TypeMask.IntegerUnsignedOptional:
//					{
//						int value = RingBuffer.readValue(idx++, inputRing);
//						if (FieldReferenceOffsetManager.getAbsent32Value(from)!=value) {
//							visitor.visitUnsignedInteger(from.fieldNameScript[j],0xFFFFFFFFl&(long)from.fieldIdScript[j],value);
//						}
//					}
					break;
				case TypeMask.LongSigned:
//					{
//						visitor.visitSignedLong(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readLong(idx, inputRing));
//						idx+=2;
//					}	
					break;	
				case TypeMask.LongUnsigned:
//					{
//						visitor.visitUnsignedLong(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readLong(idx, inputRing));
//						idx+=2;
//					}	
					break;	
				case TypeMask.LongSignedOptional:
//					{
//						long value = RingBuffer.readLong(idx, inputRing);
//						idx+=2;
//						if (FieldReferenceOffsetManager.getAbsent64Value(from)!=value) {
//							visitor.visitSignedLong(from.fieldNameScript[j],from.fieldIdScript[j],value);
//						}
//					}	
					break;		
				case TypeMask.LongUnsignedOptional:
//					{
//						long value = RingBuffer.readLong(idx, inputRing);
//						idx+=2;
//						if (FieldReferenceOffsetManager.getAbsent64Value(from)!=value) {
//							visitor.visitUnsignedLong(from.fieldNameScript[j],from.fieldIdScript[j],RingBuffer.readLong(idx, inputRing));
//						}
//					}	
					break;
				case TypeMask.Decimal:
//					{
//						int exp = RingBuffer.readValue(idx++, inputRing);
//						long mant = RingBuffer.readLong(idx, inputRing);
//						idx+=2;
//						visitor.visitDecimal(from.fieldNameScript[j],from.fieldIdScript[j],exp,mant);
//						i++;//add 1 extra because decimal takes up 2 slots in the script
//					}
					break;	
				case TypeMask.DecimalOptional:
//					{
//						int exp = RingBuffer.readValue(idx++, inputRing);
//						long mant = RingBuffer.readLong(idx, inputRing);
//						idx+=2;
//						if (FieldReferenceOffsetManager.getAbsent32Value(from)!=exp) {
//							visitor.visitDecimal(from.fieldNameScript[j],from.fieldIdScript[j],exp,mant);
//						}
//						i++;//add 1 extra because decimal takes up 2 slots in the script
//					}
					break;	
				case TypeMask.TextASCII:
//					{						
//						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
//						int len =  RingBuffer.readRingByteLen(idx, inputRing);
//						idx+=2;
//						assert(len>=0) : "Optional strings are NOT supported for this type";
//						int pos = bytePosition(meta,inputRing,len);    		
//						String name = from.fieldNameScript[j];
//						long id = from.fieldIdScript[j];
//						visitor.visitASCII(name, id, RingBuffer.readASCII(inputRing, visitor.targetASCII(name, id), pos, len));
//					}
					break;
				case TypeMask.TextASCIIOptional:
//					{						
//						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
//						int len =  RingBuffer.readRingByteLen(idx, inputRing);
//						idx+=2;
//						if (len>0) { //a negative length is a null and zero there is no work to do
//							int pos = bytePosition(meta,inputRing,len);    		
//							String name = from.fieldNameScript[j];
//							long id = from.fieldIdScript[j];
//							visitor.visitASCII(name, id, RingBuffer.readASCII(inputRing, visitor.targetASCII(name, id), pos, len));
//						}
//					}
					break;
				case TypeMask.TextUTF8:
//					{						
//						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
//						int len =  RingBuffer.readRingByteLen(idx, inputRing);
//						idx+=2;
//						assert(len>=0) : "Optional strings are NOT supported for this type";
//						int pos = bytePosition(meta,inputRing,len);    		
//						String name = from.fieldNameScript[j];
//						long id = from.fieldIdScript[j];
//						visitor.visitUTF8(name, id, RingBuffer.readUTF8(inputRing, visitor.targetUTF8(name, id), pos, len));
//					}
					break;						
				case TypeMask.TextUTF8Optional:
//					{						
//						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
//						int len =  RingBuffer.readRingByteLen(idx, inputRing);
//						idx+=2;
//						if (len>0) { //a negative length is a null and zero there is no work to do
//							int pos = bytePosition(meta,inputRing,len);    		
//							String name = from.fieldNameScript[j];
//							long id = from.fieldIdScript[j];
//							visitor.visitUTF8(name, id, RingBuffer.readUTF8(inputRing, visitor.targetUTF8(name, id), pos, len));
//						}
//					}
					break;
				case TypeMask.ByteArray:
//					{						
//						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
//						int len =  RingBuffer.readRingByteLen(idx, inputRing);
//						idx+=2;
//						assert(len>=0) : "Optional strings are NOT supported for this type";
//						int pos = bytePosition(meta,inputRing,len);    		
//						String name = from.fieldNameScript[j];
//						long id = from.fieldIdScript[j];
//						visitor.visitBytes(name, id, RingBuffer.readBytes(inputRing, visitor.targetBytes(name, id, len), pos, len));
//					}
					break;	
				case TypeMask.ByteArrayOptional:
//					{						
//						int meta = RingBuffer.readRingByteMetaData(idx, inputRing);
//						int len =  RingBuffer.readRingByteLen(idx, inputRing);
//						idx+=2;
//						if (len>0) { //a negative length is a null and zero there is no work to do
//							int pos = bytePosition(meta,inputRing,len);    		
//							String name = from.fieldNameScript[j];
//							long id = from.fieldIdScript[j];
//							visitor.visitBytes(name, id, RingBuffer.readBytes(inputRing, visitor.targetBytes(name, id, len), pos, len));
//						}
//					}
					break;
		    	default: System.err.println("unknown "+TokenBuilder.tokenToString(from.tokens[j]));
			}
		}
		
		//we are here because it did not exit early with close group or group length therefore this
		//fragment is one of those that is not wrapped by a group open/close and we should do the close logic.
		nestedFragmentDepth--; 
		
	}
	
}
