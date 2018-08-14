package com.ociweb.pronghorn.pipe.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.token.TokenBuilder;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public class StreamingVisitorReader {

    private static final Logger log = LoggerFactory.getLogger(StreamingVisitorReader.class);

	private final StreamingReadVisitor visitor;
	private final Pipe<?> inputRing;
	final FieldReferenceOffsetManager from;

	private final LowLevelStateManager navState;
    private final boolean processUTF8;


    //TODO: B, this does not work with preamble of any size. is preamble a feature we really want to continue supporting in all case?

	public StreamingVisitorReader(Pipe<?> inputRing, StreamingReadVisitor visitor, boolean processUTF8) {
		this.inputRing = inputRing;
        this.visitor = visitor;
        this.processUTF8 = processUTF8;
        this.from = Pipe.from(inputRing);
		this.navState = new LowLevelStateManager(from);
	}

	public StreamingVisitorReader(Pipe inputRing, StreamingReadVisitor visitor) {
        this(inputRing, visitor, true);
	}

	//TODO: these 3 need to be turned into static.

	public void startup() {
		this.visitor.startup();
	}

	public void shutdown() {
		this.visitor.shutdown();
	}

	public void run() {

		while (!visitor.paused() && Pipe.hasContentToRead(inputRing)) {

		        int startPos;
		        int cursor;

		        if (LowLevelStateManager.isStartNewMessage(navState)) {
		        	//start new message

		        	cursor = Pipe.takeMsgIdx(inputRing);
		        	if (cursor<0) {
		        		
						visitor.shutdown();
						
						Pipe.confirmLowLevelRead(inputRing, Pipe.EOF_SIZE);
						Pipe.releaseReadLock(inputRing);
												
						return;
		        	}
		        	startPos = 1;//new message so skip over this messageId field

		        	visitor.visitTemplateOpen(from.fieldNameScript[cursor], from.fieldIdScript[cursor]);

		        } else {
		        	cursor = LowLevelStateManager.activeCursor(navState);
		        	startPos = 0;//this is not a new message so there is no id to jump over.

		        }
		        int dataSize = Pipe.sizeOf(inputRing, cursor);

		        //must the next read position forward by the size of this fragment so next time we confirm that there is a fragment to read.
				Pipe.confirmLowLevelRead(inputRing, dataSize);

		        //visit all the fields in this fragment
		        processFragment(startPos, cursor);

		        //move the position up but exclude the one byte that we already added on
		        Pipe.setWorkingTailPosition(inputRing, Pipe.getWorkingTailPosition(inputRing)+ (dataSize-startPos) - 1 );//Subtract one so release reads can get the byte count

		        Pipe.releaseReadLock(inputRing);
		}

	}

    private void processFragment(int startPos, final int fragmentCursor) {

		final int fieldsInFragment = from.fragScriptSize[fragmentCursor];
		final String[] fieldNameScript = from.fieldNameScript;
        final long[] fieldIdScript = from.fieldIdScript;
        final int[] depth = from.fragDepth;

		int i = startPos;
		int idx = 0; //TODO: remove this and use the standard low level take methods.
		while (i<fieldsInFragment) {
			int fieldCursor = fragmentCursor+i++;

			switch (TokenBuilder.extractType(from.tokens[fieldCursor])) {
				case TypeMask.Group:
					if (FieldReferenceOffsetManager.isGroupOpen(from, fieldCursor)) {
						processFragmentOpen(fieldNameScript[fieldCursor], fieldCursor, fieldIdScript[fieldCursor]);
					} else {
					    //process group close
					    final int fieldLimit = fieldCursor+(fieldsInFragment-i);
					    final int len = from.tokens.length;

						do {//close this member of the sequence or template
							final String name = fieldNameScript[fieldCursor];
							final long id = fieldIdScript[fieldCursor];

							//if this was a close of sequence count down so we now when to close it.
							if (FieldReferenceOffsetManager.isGroupSequence(from, fieldCursor)) {
							    if (processSequenceInstanceClose(name, id)) {
							       //jump out if we need to process one more fragment
							        return;
								}
							  //else this is the end of the sequence and close the nested groups as needed.
							} else {
							    //group close that is not a sequence.
							    if (fieldCursor<=fieldLimit) { //fixed count of closes.
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

                    processSequenceOpen(fragmentCursor, fieldNameScript[fieldCursor+1], idx, fieldCursor, fieldIdScript[fieldCursor+1]);
					return;
				case TypeMask.IntegerSigned:
				    pronghornIntegerSigned(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
					break;
				case TypeMask.IntegerUnsigned: //Java does not support unsigned int so we pass it as a long being careful not to get it signed.
				    processIntegerUnsigned(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
					break;
				case TypeMask.IntegerSignedOptional:
				    processIntegerSignedOptional(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
					break;
				case TypeMask.IntegerUnsignedOptional:
				    processIntegerUnsignedOptional(fieldNameScript[fieldCursor], idx++, fieldCursor, fieldIdScript[fieldCursor]);
					break;
				case TypeMask.LongSigned:
				    processLongSigned(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
				    idx+=2;
					break;
				case TypeMask.LongUnsigned:
				    processLongUnsigned(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
				    idx+=2;
					break;
				case TypeMask.LongSignedOptional:
				    processLongSignedOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
				    idx+=2;
					break;
				case TypeMask.LongUnsignedOptional:
				    processLongUnsignedOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
					idx+=2;
				    break;
				case TypeMask.Decimal:
					processDecimal(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
					idx+=3;
					i++;//add 1 extra because decimal takes up 2 slots in the script
					break;
				case TypeMask.DecimalOptional:
					processDecimalOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
					idx+=3;
					i++;//add 1 extra because decimal takes up 2 slots in the script
					break;
				case TypeMask.TextASCII:
                    processTextASCII(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
					break;
				case TypeMask.TextASCIIOptional:
                    processTextASCIIOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
					break;
				case TypeMask.TextUTF8:
                    if (processUTF8) {
                        processTextUTF8(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    } else {
                        processByteVector(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    }
                    idx += 2;
                    break;
				case TypeMask.TextUTF8Optional:
                    if (processUTF8) {
                        processTextUTF8Optional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    } else {
                        processByteVectorOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    }
				    idx+=2;
				    break;
                case TypeMask.ByteVector:
                    processByteVector(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
                    idx+=2;
                    break;
                case TypeMask.ByteVectorOptional:
				    processByteVectorOptional(fieldNameScript[fieldCursor], idx, fieldCursor, fieldIdScript[fieldCursor]);
				    idx+=2;
				    break;
				case TypeMask.Dictionary:
				    processDictionary();
				    idx++;
				    break;
		    	default: log.error("unknown token type:"+TokenBuilder.tokenToString(from.tokens[fieldCursor]));
			}
		}

		//we are here because it did not exit early with close group or group length therefore this
		//fragment is one of those that is not wrapped by a group open/close and we should do the close logic.
		processFragmentClose();

	}

    private void processFragmentClose() {
        LowLevelStateManager.closeFragment(navState);
    }

    private boolean processSequenceInstanceClose(final String name, final long id) {
        visitor.visitFragmentClose(name,id);  //close of one sequence member
        if (LowLevelStateManager.closeSequenceIteration(navState)) {
            visitor.visitSequenceClose(name,id);//close of the sequence
            LowLevelStateManager.closeFragment(navState); //will become zero so we start a new message
            return false;
        }
        return true;
    }

    private void postProcessSequence(int fieldCursor) {
        LowLevelStateManager.continueAtThisCursor(navState, fieldCursor);
    }

    private void processSequenceOpen(final int fragmentCursor, String name, int idx, int fieldCursor, long id) {
        int seqLength = Pipe.readIntValue(idx,inputRing);
        visitor.visitSequenceOpen(name, id, seqLength);
        LowLevelStateManager.processGroupLength(navState, fragmentCursor, seqLength);
    }

    private void processMessageClose(final String name, final long id, boolean needsToCloseFragment) {
        visitor.visitTemplateClose(name, id);
        //this close was not a sequence so it must be the end of the message
        if (needsToCloseFragment) {
            LowLevelStateManager.closeFragment(navState);
        }
    }

    private void processFragmentOpen(String name, int fieldCursor, long id) {
        visitor.visitFragmentOpen(name, id, fieldCursor);
    }

    private void processDecimalOptional(String name, int idx, int fieldCursor, long id) {
        int exp = Pipe.readIntValue(idx,inputRing);
        long mant = Pipe.readLong(idx+1, inputRing);
        if (FieldReferenceOffsetManager.getAbsent32Value(from)!=exp) {
        	visitor.visitDecimal(name,id,exp,mant);
        }
    }

    private void processDecimal(String name, int idx, int fieldCursor, long id) {
        int exp = Pipe.readIntValue(idx,inputRing);
        long mant = Pipe.readLong(idx+1, inputRing);
        visitor.visitDecimal(name,id,exp,mant);
    }

    private void pronghornIntegerSigned(String name, final int idx, int fieldCursor, long id) {
        visitor.visitSignedInteger(name,id, Pipe.readIntValue(idx,inputRing));
    }

    private void processIntegerUnsigned(String name, final int idx, int fieldCursor, long id) {
        visitor.visitUnsignedInteger(name,id,  0xFFFFFFFFL&(long)Pipe.readIntValue(idx,inputRing));
    }

    private void processIntegerSignedOptional(String name, final int idx, int fieldCursor, long id) {
    	int value = Pipe.readIntValue(idx,inputRing);
    	if (FieldReferenceOffsetManager.getAbsent32Value(from)!=value) {
    		visitor.visitSignedInteger(name,id,value);
    	}
    }

    private void processIntegerUnsignedOptional(String name, final int idx, int fieldCursor, long id) {
    	int value = Pipe.readIntValue(idx,inputRing);
    	if (FieldReferenceOffsetManager.getAbsent32Value(from)!=value) {
    		visitor.visitUnsignedInteger(name, id, 0xFFFFFFFFL&(long)value);
    	}
    }

    private void processLongSigned(String name, final int idx, int fieldCursor, long id) {
       	visitor.visitSignedLong(name,id,Pipe.readLong(idx, inputRing));
    }

    private void processLongUnsigned(String name, final int idx, int fieldCursor, long id) {
       	visitor.visitUnsignedLong(name,from.fieldIdScript[fieldCursor],Pipe.readLong(idx, inputRing));
    }

    private void processLongSignedOptional(String name, final int idx, int fieldCursor, long id) {
       	long value = Pipe.readLong(idx, inputRing);
       	if (FieldReferenceOffsetManager.getAbsent64Value(from)!=value) {
        		visitor.visitSignedLong(name,id,value);
       	}
    }

    private void processLongUnsignedOptional(String name, final int idx, int fieldCursor, long id) {
        	long value = Pipe.readLong(idx, inputRing);
        	if (FieldReferenceOffsetManager.getAbsent64Value(from)!=value) {
        		visitor.visitUnsignedLong(name,id,Pipe.readLong(idx, inputRing));
        	}
    }

    private void processTextASCII(String name, final int idx, int fieldCursor, long id) {
        	int meta = Pipe.readByteArraMetaData(idx, inputRing);
        	int len =  Pipe.readByteArrayLength(idx, inputRing);
        	assert(len>=0) : "Optional strings are NOT supported for this type";

        	visitor.visitASCII(name, id, (CharSequence) Pipe.readASCII(inputRing, visitor.targetASCII(name, id), meta, len));

    }

    private void processTextASCIIOptional(String name, final int idx, int fieldCursor, long id) {

        	int meta = Pipe.readByteArraMetaData(idx, inputRing);
        	int len =  Pipe.readByteArrayLength(idx, inputRing);

        	if (len>0) { //a negative length is a null and zero there is no work to do
        		visitor.visitASCII(name, id, (CharSequence) Pipe.readASCII(inputRing, visitor.targetASCII(name, id), meta, len));
        	}

    }

    private void processTextUTF8(String name, final int idx, int fieldCursor, long id) {

        	int meta = Pipe.readByteArraMetaData(idx, inputRing);
        	int len =  Pipe.readByteArrayLength(idx, inputRing);

        	assert(len>=0) : "Optional strings are NOT supported for this type";
        	visitor.visitUTF8(name, id, (CharSequence) Pipe.readUTF8(inputRing, visitor.targetUTF8(name, id), meta, len));

    }

    private void processTextUTF8Optional(String name, final int idx, int fieldCursor, long id) {

        	int meta = Pipe.readByteArraMetaData(idx, inputRing);
        	int len =  Pipe.readByteArrayLength(idx, inputRing);

        	if (len>0) { //a negative length is a null and zero there is no work to do
        		visitor.visitUTF8(name, id, (CharSequence) Pipe.readUTF8(inputRing, visitor.targetUTF8(name, id), meta, len));
        	}

    }

    private void processByteVector(String name, final int idx, int fieldCursor, long id) {

        	int meta = Pipe.readByteArraMetaData(idx, inputRing);
        	int len =  Pipe.readByteArrayLength(idx, inputRing);

        	if (len>=0) {
        		visitor.visitBytes(name, id, Pipe.readBytes(inputRing, visitor.targetBytes(name, id, len), meta, len));
        	} else {
        		//len of -1 is a null not a zero length vector, 
        		//NOTE: visitor does not yet support null;
        	}
        	

    }

    private void processByteVectorOptional(String name, int idx, int fieldCursor, long id) {
        	int meta = Pipe.readByteArraMetaData(idx, inputRing);
        	int len =  Pipe.readByteArrayLength(idx, inputRing);

        	if (len>0) { //a negative length is a null and zero there is no work to do
        		visitor.visitBytes(name, id, Pipe.readBytes(inputRing, visitor.targetBytes(name, id, len), meta, len));
        	}
    }

    private void processDictionary() {
        {
            log.debug("dictionary operation discovered, TODO: add this to the vistor to be supported");
        }
    }

}
