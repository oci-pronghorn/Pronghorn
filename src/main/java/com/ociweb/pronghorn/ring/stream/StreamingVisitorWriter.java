package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.publishAllBatchedWrites;
import static com.ociweb.pronghorn.ring.RingBuffer.publishWrites;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class StreamingVisitorWriter {

	private StreamingWriteVisitor visitor;
	private RingBuffer outputRing;
	private FieldReferenceOffsetManager from;
	private int maxFragmentSize;
	
	private int nestedFragmentDepth;
	private int[] cursorStack;
	private int[] sequenceCounters;
	
	
	public StreamingVisitorWriter(RingBuffer outputRing, StreamingWriteVisitor visitor) {
		this.visitor = visitor;
		this.outputRing = outputRing;
		
		this.from = RingBuffer.from(outputRing);	
		
		this.maxFragmentSize = FieldReferenceOffsetManager.maxFragmentSize(this.from);
	
		
		this.cursorStack = new int[this.from.maximumFragmentStackDepth];
		this.sequenceCounters = new int[this.from.maximumFragmentStackDepth];
		
		this.nestedFragmentDepth = -1;
	}
	
	public boolean isAtBreakPoint() {
	    return nestedFragmentDepth<0;
	}

	public void run() {
		
		//write as long as its not posed and we have room to write any possible known fragment
	    
		while (!visitor.paused() && RingBuffer.roomToLowLevelWrite(outputRing, maxFragmentSize) ) {	
			    	        
		        int startPos;
		        int cursor;

		        if (nestedFragmentDepth<0) {	
		        	
		        	//start new message, visitor returns this new id to be written.
		        	cursor = visitor.pullMessageIdx();
		        	assert(isValidMessageStart(cursor, from));
		        	if (cursor<0) {
		        		RingBuffer.publishWrites(outputRing);
		        		RingBuffer.publishAllBatchedWrites(outputRing);
		        		return;
		        	}
		        	
		        	//System.err.println("new message Idx "+cursor+" writen to "+(outputRing.workingHeadPos.value));
		        	
		        	RingBuffer.addMsgIdx(outputRing,  cursor);
		        	
		        	startPos = 1;//new message so skip over this messageId field
		        	
		        	//Beginning of template
		        	
		        	//These name the message template but no need for them at this time
		        	//String messageName = from.fieldNameScript[cursor];
		        	//long messageId = from.fieldIdScript[cursor];
		        	

		        } else {
        	
		            
		        	cursor = cursorStack[nestedFragmentDepth];
		        	startPos = 0;//this is not a new message so there is no id to jump over.
			    
		        }
		        
		        //visit all the fields in this fragment
		        processFragment(startPos, cursor);
		        		        
		        RingBuffer.confirmLowLevelWrite(outputRing, from.fragDataSize[cursor]);
		        
		        publishWrites(outputRing);
		}
		publishAllBatchedWrites(outputRing);
		
	}

	private boolean isValidMessageStart(int cursor, FieldReferenceOffsetManager from) {
	       int i = from.messageStarts.length;
	       while (--i>=0) {
	           if (cursor == from.messageStarts[i]) {
	               return true;
	           }
	       }
	       return false;
    }

    public void startup() {
	        this.visitor.startup();
	    }
	    
	    public void shutdown() {
	        this.visitor.shutdown();
	    }
	    
	private void processFragment(int startPos, int cursor) {
		int fieldsInFragment = from.fragScriptSize[cursor];
		int i = startPos;
		
		//System.err.println("begin write of fragment "+from.fieldNameScript[cursor]+" "+cursor);
		while (i<fieldsInFragment) {
			int j = cursor+i++;
			
			switch (TokenBuilder.extractType(from.tokens[j])) {
				case TypeMask.Group:
					if (FieldReferenceOffsetManager.isGroupOpen(from, j)) {
						visitor.fragmentOpen(from.fieldNameScript[j],from.fieldIdScript[j]);
					} else {				
						do {//close this member of the sequence or template
							String name = from.fieldNameScript[j];
							long id = from.fieldIdScript[j];
							
							//if this was a close of sequence count down so we now when to close it.
							if (FieldReferenceOffsetManager.isGroupSequence(from, j)) {
								visitor.fragmentClose(name,id);
								
								//close of one sequence member
								if (--sequenceCounters[nestedFragmentDepth]<=0) {
									//close of the sequence
									visitor.sequenceClose(name,id);
									nestedFragmentDepth--; //will become zero so we start a new message
								
								} else {
									break;
								}
							} else {
							    visitor.templateClose(name,id);
								
							    assert(nestedFragmentDepth<=0) : "bad "+nestedFragmentDepth;
							    
								//this close was not a sequence so it must be the end of the message
								nestedFragmentDepth = -1;
								return;//must exit so we do not pick up any more fields
							}
						} while (++j<from.tokens.length && FieldReferenceOffsetManager.isGroupClosed(from, j) );
						//if the stack is empty set the continuation for fields that appear after the sequence
						if (j<from.tokens.length && !FieldReferenceOffsetManager.isGroup(from, j)) {
							cursorStack[++nestedFragmentDepth] = j;
						}
					//	 System.err.println("close nested fragments, next starts at "+(1+outputRing.workingHeadPos.value));
						return;//this is always the end of a fragment
					}					
					break;
				case TypeMask.GroupLength:				    
    				{
    				    
    				    int seqLen = visitor.pullSequenceLength(from.fieldNameScript[j],from.fieldIdScript[j]);
                        RingBuffer.addIntValue(seqLen, outputRing);    

                        assert(i==fieldsInFragment) :" this should be the last field";
                        sequenceCounters[++nestedFragmentDepth] = seqLen;
                        cursorStack[nestedFragmentDepth] = cursor+fieldsInFragment;
  
    				}
					return; 					
				case TypeMask.IntegerSigned:
				    RingBuffer.addIntValue(visitor.pullSignedInt(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);    

					break;
				case TypeMask.IntegerUnsigned: //Java does not support unsigned int so we pass it as a long being careful not to get it signed.
                    RingBuffer.addIntValue(visitor.pullUnsignedInt(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);    

					break;
				case TypeMask.IntegerSignedOptional:
					{
                       if (visitor.isAbsent(from.fieldNameScript[j],from.fieldIdScript[j])) {
                            RingBuffer.addIntValue(FieldReferenceOffsetManager.getAbsent32Value(from), outputRing);
                        } else {
                            RingBuffer.addIntValue(visitor.pullSignedInt(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing); 
                        }

					}
					break;
				case TypeMask.IntegerUnsignedOptional:
					{
                        if (visitor.isAbsent(from.fieldNameScript[j],from.fieldIdScript[j])) {
                            RingBuffer.addIntValue(FieldReferenceOffsetManager.getAbsent32Value(from), outputRing);
                        } else {
                            RingBuffer.addIntValue(visitor.pullUnsignedInt(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing); 
                        }

					}
					break;
				case TypeMask.LongSigned:
					{
						RingBuffer.addLongValue(visitor.pullSignedLong(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);						

					}	
					break;	
				case TypeMask.LongUnsigned:
					{
						RingBuffer.addLongValue(visitor.pullUnsignedLong(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);

					}	
					break;	
				case TypeMask.LongSignedOptional:
					{
						if (visitor.isAbsent(from.fieldNameScript[j],from.fieldIdScript[j])) {
							RingBuffer.addLongValue(FieldReferenceOffsetManager.getAbsent64Value(from), outputRing);
						} else {
							RingBuffer.addLongValue(visitor.pullSignedLong(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);
						}

					}	
					break;		
				case TypeMask.LongUnsignedOptional:
					{
		                if (visitor.isAbsent(from.fieldNameScript[j],from.fieldIdScript[j])) {
                            RingBuffer.addLongValue(FieldReferenceOffsetManager.getAbsent64Value(from), outputRing);
                        } else {
                            RingBuffer.addLongValue(visitor.pullUnsignedLong(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);
                        }

					}	
					break;
				case TypeMask.Decimal:
					{					    
					    int pullDecimalExponent = visitor.pullDecimalExponent(from.fieldNameScript[j],from.fieldIdScript[j]);
					    
                        RingBuffer.addIntValue(pullDecimalExponent, outputRing);
					    RingBuffer.addLongValue(visitor.pullDecimalMantissa(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);

						i++;//add 1 extra because decimal takes up 2 slots in the script
					}
					break;	
				case TypeMask.DecimalOptional:
					{
					    
					    if (visitor.isAbsent(from.fieldNameScript[j],from.fieldIdScript[j])) {
	                       RingBuffer.addIntValue(FieldReferenceOffsetManager.getAbsent32Value(from), outputRing);
	                       RingBuffer.addLongValue(FieldReferenceOffsetManager.getAbsent64Value(from), outputRing); 
					    } else {
					       RingBuffer.addIntValue(visitor.pullDecimalExponent(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);
                           RingBuffer.addLongValue(visitor.pullDecimalMantissa(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);
					    }

                       i++;//add 1 extra because decimal takes up 2 slots in the script
					   
					}
					break;	
				case TypeMask.TextASCII:
					{		
					    RingBuffer.addASCII(visitor.pullASCII(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);

					}
					break;
				case TypeMask.TextASCIIOptional:
					{				
					    //a null char sequence can be returned by vistASCII
					    RingBuffer.addASCII(visitor.pullASCII(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);

					}
					break;
				case TypeMask.TextUTF8:
					{			
                        RingBuffer.addUTF8(visitor.pullUTF8(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);

					}
					break;						
				case TypeMask.TextUTF8Optional:
					{						
                        RingBuffer.addUTF8(visitor.pullUTF8(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);

					}
					break;
				case TypeMask.ByteArray:
					{					
					    RingBuffer.addByteBuffer(visitor.pullByteBuffer(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);
				    
					}
					break;	
				case TypeMask.ByteArrayOptional:
					{						
                        RingBuffer.addByteBuffer(visitor.pullByteBuffer(from.fieldNameScript[j],from.fieldIdScript[j]), outputRing);
                        
					}
					break;
		    	default: System.err.println("unknown "+TokenBuilder.tokenToString(from.tokens[j]));
			}
		}
		
		
		//we are here because it did not exit early with close group or group length therefore this
		//fragment is one of those that is not wrapped by a group open/close and we should do the close logic.
		nestedFragmentDepth--; 
		
	}
	
}
