package com.ociweb.pronghorn.pipe.stream;

import static com.ociweb.pronghorn.pipe.Pipe.publishAllBatchedWrites;
import static com.ociweb.pronghorn.pipe.Pipe.publishWrites;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public class StreamingVisitorWriter {

	private StreamingWriteVisitor visitor;
	private Pipe outputRing;
	private FieldReferenceOffsetManager from;
	private int maxFragmentSize;
	
	private final LowLevelStateManager navState;
		
	
	public StreamingVisitorWriter(Pipe outputRing, StreamingWriteVisitor visitor) {
		this.visitor = visitor;
		this.outputRing = outputRing;
		
		this.from = Pipe.from(outputRing);	
		
		this.maxFragmentSize = FieldReferenceOffsetManager.maxFragmentSize(this.from);
	
		this.navState = new LowLevelStateManager(outputRing);
	}
	
	public boolean isAtBreakPoint() {
	    return LowLevelStateManager.isStartNewMessage(navState);
	}

	public void run() {
		
		//write as long as its not posed and we have room to write any possible known fragment
	    
		while (!visitor.paused() && Pipe.hasRoomForWrite(outputRing, maxFragmentSize) ) {	
			    	        
		        int startPos;
		        int cursor;

		        if (LowLevelStateManager.isStartNewMessage(navState)) {	
		        	
		        	//start new message, visitor returns this new id to be written.
		        	cursor = visitor.pullMessageIdx();
		        	assert(isValidMessageStart(cursor, from));
		        	if (cursor<0) {
		        		Pipe.publishWrites(outputRing);
		        		Pipe.publishAllBatchedWrites(outputRing);
		        		return;
		        	}
		        	
		        	//System.err.println("new message Idx "+cursor+" writen to "+(outputRing.workingHeadPos.value));
		        	
		        	Pipe.addMsgIdx(outputRing,  cursor);
		        	
		        	startPos = 1;//new message so skip over this messageId field
		        	
		        	//Beginning of template
		        	
		        	//These name the message template but no need for them at this time
		        	//String messageName = from.fieldNameScript[cursor];
		        	//long messageId = from.fieldIdScript[cursor];
		        	

		        } else {
        	
		            cursor = LowLevelStateManager.activeCursor(navState);
		        	startPos = 0;//this is not a new message so there is no id to jump over.
			    
		        }
		        
		        //visit all the fields in this fragment
		        processFragment(startPos, cursor);
		        		        
		        Pipe.confirmLowLevelWrite(outputRing, from.fragDataSize[cursor]);
		        
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
	    
	private void processFragment(int startPos, int fragmentCursor) {
		int fieldsInFragment = from.fragScriptSize[fragmentCursor];
		int i = startPos;
		
	    final String[] fieldNameScript = from.fieldNameScript;
	    final long[] fieldIdScript = from.fieldIdScript;
	    final int[] depth = from.fragDepth;
		
		//System.err.println("begin write of fragment "+from.fieldNameScript[cursor]+" "+cursor);
		while (i<fieldsInFragment) {
			int fieldCursor = fragmentCursor+i++;
			
			switch (TokenBuilder.extractType(from.tokens[fieldCursor])) {
				case TypeMask.Group:
					if (FieldReferenceOffsetManager.isGroupOpen(from, fieldCursor)) {
					    processFragmentOpen(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					} else {	
					    //process group close
					    final int fieldLimit = fieldCursor+(fieldsInFragment-i); 
					    final int len = from.tokens.length;
					    
						do {//close this member of the sequence or template
							String name = fieldNameScript[fieldCursor];
							long id = fieldIdScript[fieldCursor];
							
							//if this was a close of sequence count down so we now when to close it.
							if (FieldReferenceOffsetManager.isGroupSequence(from, fieldCursor)) {							    
							    if (processSequenceInstanceClose(name, id)) {
							        //jump out if we need to process one more fragment
								   return;
								}
							  //else this is the end of the sequence and close the nested groups as needed.
							} else {
							    //group close that is not a sequence.
							    if (fieldCursor<=fieldLimit) {
							        processMessageClose(name, id, depth[fieldCursor]>0);
							    }
							}
						} while (++fieldCursor<len && FieldReferenceOffsetManager.isGroupClosed(from, fieldCursor) );
						
						//if the stack is empty set the continuation for fields that appear after the sequence
						if (fieldCursor<len && !FieldReferenceOffsetManager.isGroup(from, fieldCursor)) {
							postProcessSequence(fieldCursor);
						}
						return;//this is always the end of a fragment
					}					
					break;
				case TypeMask.GroupLength:			 
    				    assert(i==fieldsInFragment) :" this should be the last field";
                        processSequenceOpen(fragmentCursor, fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);  
					return; 					
				case TypeMask.IntegerSigned:
                    processIntegerSigned(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);   
					break;
				case TypeMask.IntegerUnsigned: //Java does not support unsigned int so we pass it as a long being careful not to get it signed.
                    processIntegerUnsigned(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);    
					break;
				case TypeMask.IntegerSignedOptional:
	                      processIntegerSignedOptional(fieldNameScript, fieldIdScript, fieldCursor, fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;
				case TypeMask.IntegerUnsignedOptional:
	                      processIntegerUnsignedOptional(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;
				case TypeMask.LongSigned:
	                      processLongSigned(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);	
					break;	
				case TypeMask.LongUnsigned:
	                      processLongUnsigned(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;	
				case TypeMask.LongSignedOptional:
	                       processLongSignedOptional(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;		
				case TypeMask.LongUnsignedOptional:
	                       processLongUnsignedOptional(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);	
					break;
				case TypeMask.Decimal:
	                    processDecimal(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
						i++;//add 1 extra because decimal takes up 2 slots in the script
					break;	
				case TypeMask.DecimalOptional:
					    processDecimalOptional(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					i++;//add 1 extra because decimal takes up 2 slots in the script
					break;	
				case TypeMask.TextASCII:		
					    processTextASCII(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;
				case TypeMask.TextASCIIOptional:					
					    //a null char sequence can be returned by vistASCII
					    processTextASCIIOptional(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;
				case TypeMask.TextUTF8:				
                        processTextUTF8(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;						
				case TypeMask.TextUTF8Optional:					
                        processTextUTF8Optional(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;
				case TypeMask.ByteArray:				
					    processByteArray(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;	
				case TypeMask.ByteArrayOptional:
                    processByteArrayOptional(fieldNameScript[fieldCursor], fieldIdScript[fieldCursor]);
					break;
		    	default: System.err.println("unknown "+TokenBuilder.tokenToString(from.tokens[fieldCursor]));
			}
		}
		
		//we are here because it did not exit early with close group or group length therefore this
		//fragment is one of those that is not wrapped by a group open/close and we should do the close logic.
		processFragmentClose(); 
		
	}

    private void processFragmentClose() {
        LowLevelStateManager.closeFragment(navState);
    }

    private void postProcessSequence(int fieldCursor) {
        LowLevelStateManager.continueAtThisCursor(navState, fieldCursor);
    }

    private void processMessageClose(String name, long id, boolean needsToCloseFragment) {
        visitor.templateClose(name,id);
           
        if (needsToCloseFragment) {
            LowLevelStateManager.closeFragment(navState);
        }
    }

    private void processFragmentOpen(String name, long id) {
        visitor.fragmentOpen(name,id);
    }

    private boolean processSequenceInstanceClose(String name, long id) {

        visitor.fragmentClose(name,id);
        
        //close of one sequence member
        if (LowLevelStateManager.closeSequenceIteration(navState)) {
        	//close of the sequence
        	visitor.sequenceClose(name,id);
        	LowLevelStateManager.closeFragment(navState);  //will become zero so we start a new message
        	return false;
        }
        return true;
    }

    private void processSequenceOpen(int fragmentCursor, String name, long id) {
        int seqLen = visitor.pullSequenceLength(name,id);
        Pipe.addIntValue(seqLen, outputRing);
        LowLevelStateManager.processGroupLength(navState, fragmentCursor, seqLen);
    }

    private void processIntegerSigned(String name, long id) {
        Pipe.addIntValue(visitor.pullSignedInt(name,id), outputRing);
    }

    private void processIntegerUnsigned(String name, long id) {
        Pipe.addIntValue(visitor.pullUnsignedInt(name,id), outputRing);
    }

    private void processIntegerSignedOptional(final String[] fieldNameScript, final long[] fieldIdScript,
            int fieldCursor, String name, long id) {
        if (visitor.isAbsent(fieldNameScript[fieldCursor],fieldIdScript[fieldCursor])) {
                Pipe.addIntValue(FieldReferenceOffsetManager.getAbsent32Value(from), outputRing);
            } else {
                Pipe.addIntValue(visitor.pullSignedInt(name,id), outputRing); 
            }
    }

    private void processIntegerUnsignedOptional(String name, long id) {
        if (visitor.isAbsent(name,id)) {
            Pipe.addIntValue(FieldReferenceOffsetManager.getAbsent32Value(from), outputRing);
        } else {
            Pipe.addIntValue(visitor.pullUnsignedInt(name,id), outputRing); 
        }
    }

    private void processLongSigned(String name, long id) {
        Pipe.addLongValue(visitor.pullSignedLong(name,id), outputRing);
    }

    private void processLongUnsigned(String name, long id) {
        Pipe.addLongValue(visitor.pullUnsignedLong(name,id), outputRing);
    }

    private void processLongSignedOptional(String name, long id) {
        if (visitor.isAbsent(name,id)) {
        	Pipe.addLongValue(FieldReferenceOffsetManager.getAbsent64Value(from), outputRing);
        } else {
        	processLongSigned(name, id);
        }
    }

    private void processLongUnsignedOptional(String name, long id) {
        if (visitor.isAbsent(name,id)) {
            Pipe.addLongValue(FieldReferenceOffsetManager.getAbsent64Value(from), outputRing);
        } else {
            processLongUnsigned(name, id);
        }
    }

    private void processDecimal(String name, long id) {
        int pullDecimalExponent = visitor.pullDecimalExponent(name,id);
        
        Pipe.addIntValue(pullDecimalExponent, outputRing);
        Pipe.addLongValue(visitor.pullDecimalMantissa(name,id), outputRing);
    }

    private void processDecimalOptional(String name, long id) {
        if (visitor.isAbsent(name,id)) {
           Pipe.addIntValue(FieldReferenceOffsetManager.getAbsent32Value(from), outputRing);
           Pipe.addLongValue(FieldReferenceOffsetManager.getAbsent64Value(from), outputRing); 
        } else {
           Pipe.addIntValue(visitor.pullDecimalExponent(name,id), outputRing);
           Pipe.addLongValue(visitor.pullDecimalMantissa(name,id), outputRing);
        }
    }

    private void processTextASCII(String name, long id) {
        Pipe.addASCII(visitor.pullASCII(name,id), outputRing);
    }

    private void processTextASCIIOptional(String name, long id) {
        Pipe.addASCII(visitor.pullASCII(name,id), outputRing);
    }

    private void processTextUTF8(String name, long id) {
        Pipe.addUTF8(visitor.pullUTF8(name,id), outputRing);
    }
	
    private void processTextUTF8Optional(String name, long id) {
        Pipe.addUTF8(visitor.pullUTF8(name,id), outputRing);
    }

    private void processByteArray(String name, long id) {
        Pipe.addByteBuffer(visitor.pullByteBuffer(name,id), outputRing);
    }

    private void processByteArrayOptional(String name, long id) {
        processByteArray(name, id);
    }
	
}
