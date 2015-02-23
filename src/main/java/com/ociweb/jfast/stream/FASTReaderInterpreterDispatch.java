//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.generator.FASTReaderDispatchTemplates;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public class FASTReaderInterpreterDispatch extends FASTReaderDispatchTemplates implements GeneratorDriving  {

    public int readFromIdx = -1;
    public final int[] rIntInit;
    public final long[] rLongInit;

    public final int MAX_INT_INSTANCE_MASK; 
    public final int MAX_BYTE_INSTANCE_MASK; 
    public final int MAX_LONG_INSTANCE_MASK;
    
    public final int nonTemplatePMapSize;
    public final int maxTemplatePMapSize;
    
    public final int[][] dictionaryMembers;
    
    //required for code generation and other documentation/debugging.
    protected final long[] fieldIdScript;
    protected final String[] fieldNameScript;
    
    protected final int[] fullScript;
    protected final int prembleBytes;

        
    public FASTReaderInterpreterDispatch(byte[] catBytes, RingBuffers ringBuffers) {
        this(new TemplateCatalogConfig(catBytes), ringBuffers);
    }    
    
    public FASTReaderInterpreterDispatch(TemplateCatalogConfig catalog, RingBuffers ringBuffers) {
        super(catalog, ringBuffers);
        
        this.fieldIdScript = catalog.fieldIdScript();
        this.fieldNameScript = catalog.fieldNameScript();
        
        this.rIntInit = catalog.dictionaryFactory().integerDictionary();
        this.rLongInit = catalog.dictionaryFactory().longDictionary();
        
        this.nonTemplatePMapSize = catalog.maxNonTemplatePMapSize();
        this.maxTemplatePMapSize = catalog.maxTemplatePMapSize();
        this.prembleBytes = catalog.clientConfig().getPreableBytes();;
        
        this.dictionaryMembers = catalog.dictionaryResetMembers();
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));
        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));
        this.MAX_BYTE_INSTANCE_MASK = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,     LocalHeap.itemCount(byteHeap) - 1);
        
        this.fullScript = catalog.fullScript();        
        
    }

    public void callBeginMessage(PrimitiveReader reader) {
        beginMessage(reader);
    }
    
    private void beginMessage(PrimitiveReader reader) {
        
        // get next token id then immediately start processing the script
        // /read prefix bytes if any (only used by some implementations)
        //ring buffer is build on int32s so the implementation limits preamble to units of 4
  //      assert ((this.preambleDataLength&0x3)==0) : "Preable may only be in units of 4 bytes";
  //      assert (this.preambleDataLength<=8) : "Preable may only be 8 or fewer bytes";
        //Hold the preamble value here until we know the template and therefore the needed ring buffer.
        
        
        
        //break out into series of gen calls to save int somewhere. units of 4 only.
        int p = this.prembleBytes;
        if (p>0) {
     //   	System.err.println("warnin preamble length "+this.preambleDataLength);
        	
            genReadPreambleA(reader, this);
             if (p>4) {
                genReadPreambleB(reader, this);
                assert(p==8) : "Unsupported large preamble";
            }
        }                                        
        
        // /////////////////
        // open message (special type of group)
        int preambleInts = (prembleBytes+3)>>2;
        genReadTemplateId(preambleInts, maxTemplatePMapSize, reader, this);        
        
        //TODO: X, add mode for reading the preamble above but NOT writing to ring buffer because it is not needed.
        //break out into second half of gen.
        p = this.prembleBytes;
        if (p>0) {
            genWritePreambleA(this); //No need to spin lock because it was done by genReadTemplateId
            if (p>4) {
                genWritePreambleB(this);
            }
        }
        genWriteTemplateId(this);
        //set this again because the code generation path may not have set it if it were skipped.
        RingBuffer rb = RingBuffers.get(ringBuffers,activeScriptCursor);
        rb.writeTrailingCountOfBytesConsumed = msgIdx>=0 && (1==rb.ringWalker.from.fragNeedsAppendedCountOfBytesConsumed[msgIdx]);
    }
    
    //TODO: MUST only call when we know there is room for the biggest known fragment, must avoid additional checks.
    // -1 end of file, 0 no data, 1 loaded
    public int decode(PrimitiveReader reader) {

    	final RingBuffer rbRingBuffer;
        if (activeScriptCursor<0) {
            if (PrimitiveReader.isEOF(reader)) { 
               // System.err.println("EOF");
                return -1; //no more data stop
            }  
            beginMessage(reader); 
            rbRingBuffer = RingBuffers.get(ringBuffers, activeScriptCursor); 
        } else {
        	rbRingBuffer = RingBuffers.get(ringBuffers, activeScriptCursor); 
        	//this is not the beginning of a fragment but we still need to mark the need to add the trailing bytes.
        	rbRingBuffer.writeTrailingCountOfBytesConsumed = (1==rbRingBuffer.ringWalker.from.fragNeedsAppendedCountOfBytesConsumed[activeScriptCursor]);        	
        }
              
           
        FieldReferenceOffsetManager from = RingBuffer.from(rbRingBuffer);		
        int fragDataSize = from.fragDataSize[activeScriptCursor];   
        
        //Waiting for tail position to change! can cache the value, must make same change in compiled code.
        long neededTailStop = rbRingBuffer.workingHeadPos.value   - rbRingBuffer.maxSize + fragDataSize;
        if (rbRingBuffer.ringWalker.tailCache < neededTailStop && ((rbRingBuffer.ringWalker.tailCache=rbRingBuffer.tailPos.longValue()) < neededTailStop) ) {
              return 0; //no space to read data and start new message so read nothing
        }
        
        assert(rbRingBuffer.workingHeadPos.value>=rbRingBuffer.headPos.get());
        
        int token;
        do {
            token = fullScript[activeScriptCursor];
  
        //    System.err.println("reading:"+TokenBuilder.tokenToString(token)+" from "+(null==reader ? "N/A" : String.valueOf(reader.position)));
    
            // The trick here is to keep all the conditionals in this method and
            // do the work elsewhere.
            if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
                // 0????
                if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                    // 00???
                    if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                        dispatchReadByTokenForInteger(token, reader, rbRingBuffer);
                    } else {
                        dispatchReadByTokenForLong(token, reader, rbRingBuffer);
                    }
                    readFromIdx = -1; //reset for next field where it might be used.
                } else {
                    // 01???
                    if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                        if (readFromIdx>=0) {
                            int source = token & MAX_BYTE_INSTANCE_MASK;
                            int target = readFromIdx & MAX_BYTE_INSTANCE_MASK;
                            genReadCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
                            readFromIdx = -1; //reset for next field where it might be used.
                        }
                        dispatchReadByTokenForText(token, reader);
                    } else {
                        // 011??
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            //Exponent token comes first and is of type Decimal, the Mantissa is second and is of type Long
                            dispatchReadByTokenForDecimal(reader, token, fullScript[++activeScriptCursor],rbRingBuffer); //pull second token);
                        } else {
                            if (readFromIdx>=0) {
                                int source = token & MAX_BYTE_INSTANCE_MASK;
                                int target = readFromIdx & MAX_BYTE_INSTANCE_MASK;
                                genReadCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
                                readFromIdx = -1; //reset for next field where it might be used.
                            }
                            dispatchReadByTokenForBytes(token, reader);
                        }
                    }
                }
            } else {
                // 1????
                if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                    // 10???
                    if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    	// 100??
                    	
                    	if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    		// 1000??                    	
                    	
                            // Group Type, no others defined so no need to keep
                            // checking
                            if (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) {
                                //Open group
                                
                                if (0==(token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) {
                                    
                                    // this is NOT a message/template so the
                                    // non-template pmapSize is used.
                                    if (nonTemplatePMapSize > 0) {
                                        genReadGroupPMapOpen(nonTemplatePMapSize, reader);
                                    }
                                } else {
                                    //this IS a message requireing template
                                    //System.err.println("xxx");
                                    //Do nothing because this is done by Reactor at this time.
                                    
                                }                                
                                
                            } else {
                                //Close group
                                int idx = TokenBuilder.MAX_INSTANCE & token;
                                closeGroup(token,idx, reader);                               
                                break;
                            }                            
                            
                    	} else {
                    		// 1001??  
                    		
                    		//template ref
                    		if (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) {
                    			//TODO: A, read open template ref
                    			
                    		} else {
                    			//TODO: A, read close template ref
                    			
                    		}
                    		
                    	}
                            

                    } else {
                        // 101??
                                                
                        // Length Type, no others defined so no need to keep
                        // checking
                        // Only happens once before a node sequence so push it
                        // on the count stack
                        int jumpToTarget = activeScriptCursor + (TokenBuilder.MAX_INSTANCE & fullScript[1+activeScriptCursor]) + 1;
                        //code generator will always return the next step in the script in order to build out all the needed fragments.
                        readLength(token,jumpToTarget, readFromIdx, reader); 
                        break;
                    }
                } else {
                    // 11???
                    // Dictionary Type, no others defined so no need to keep
                    // checking
                    if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                        readDictionaryReset(dictionaryMembers[TokenBuilder.MAX_INSTANCE & token]);
                    } else {
                        // OperatorMask.Dictionary_Read_From 0001
                        // next read will need to use this index to pull the
                        // right initial value.
                        // after it is used it must be cleared/reset to -1
                        readDictionaryFromField(token);
                    }
                }
            }
            
            ++activeScriptCursor;           
            
        } while (true);
        
        if (rbRingBuffer.writeTrailingCountOfBytesConsumed) {

        	genReadTotalMessageBytesUsed(rbRingBuffer.workingHeadPos, rbRingBuffer );
        	//this stopping logic is only needed for the interpreter, the generated version has this call injected at the right poing.
        	rbRingBuffer.writeTrailingCountOfBytesConsumed = false;
        } 
        genReadTotalMessageBytesResetUsed(rbRingBuffer);
        
        genReadGroupCloseMessage(reader, this); //active script cursor is set to end of messge by this call

        //this conditional is for the code generator so it need not check
        if (rbRingBuffer.workingHeadPos.value != rbRingBuffer.headPos.get()) {
	        assert (fragDataSize == ((int)(rbRingBuffer.workingHeadPos.value-rbRingBuffer.headPos.get()))) : "expected to write "+fragDataSize+" but wrote "+((int)(rbRingBuffer.workingHeadPos.value-rbRingBuffer.headPos.get()));
	        RingBuffer.publishHeadPositions(rbRingBuffer);  
        }
        assert(rbRingBuffer.byteWorkingHeadPos.value == rbRingBuffer.bytesHeadPos.get());
        
        return 1;//read one fragment 
    }

    public void dispatchReadByTokenForDecimal(PrimitiveReader reader, int expToken, int mantToken, RingBuffer rbRingBuffer) {
        //The previous dictionary value will need to have two read from values 
        //because these leverage the existing int/long implementations we only need to ensure readFromIdx is set between the two.
        // 0110? Decimal and DecimalOptional
        
        int expoToken = expToken;
                
        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_TYPE))) {
            
            readIntegerSigned(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader, rbRingBuffer);
            
            //exponent is NOT optional so do normal mantissa processing.
            if (0 == (mantToken & (1 << TokenBuilder.SHIFT_TYPE))) {
                // not optional
                readLongSigned(mantToken, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader, rbRingBuffer);
            } else {
                // optional
                readLongSignedOptional(mantToken, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader, rbRingBuffer);
            }
            
        } else {
            //exponent is optional so the mantissa bit may be absent
            decodeOptionalDecimal(reader, expoToken, mantToken, rbRingBuffer);
        }
        
        readFromIdx = -1; //reset for next field where it might be used. 
    }

    private void decodeOptionalDecimal(PrimitiveReader reader, int expoToken, int mantToken, RingBuffer rbRingBuffer) {
              
     //  System.err.println("MM decode : Exp:"+TokenBuilder.tokenToString(expoToken)+" Mant: "+TokenBuilder.tokenToString(mantToken));
      
       //In this method we split out by exponent operator then call the specific method needed
       //for the remaining split by mantissa.
        
        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (expoToken & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (expoToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                	
                	FieldReferenceOffsetManager r = RingBuffer.from(rbRingBuffer);
					int expoConstAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                                        
                    decodeOptionalDecimalNone(expoConstAbsent, mantToken, reader, rbRingBuffer);
                    
                } else {
                    // delta
                    int expoTarget = expoToken & MAX_INT_INSTANCE_MASK;
                    int expoSource = readFromIdx > 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : expoTarget;
					FieldReferenceOffsetManager r = RingBuffer.from(rbRingBuffer);
                    int expoConstAbsent =  FieldReferenceOffsetManager.getAbsent32Value(r);
                    
                    decodeOptionalDecimalDelta(expoTarget,expoSource,expoConstAbsent,mantToken, reader, rbRingBuffer);
                    
                }
            } else {
                // constant
                FieldReferenceOffsetManager r = RingBuffer.from(rbRingBuffer);
				int expoConstAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                int expoConstConst = rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK];
                
                decodeOptionalDecimalConstant(expoConstAbsent,expoConstConst,mantToken, reader, rbRingBuffer);
                
            }
        
        } else {
            // copy, default, increment
            if (0 == (expoToken & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (expoToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int expoTarget = expoToken & MAX_INT_INSTANCE_MASK;
                    int expoSource = readFromIdx > 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : expoTarget;
					FieldReferenceOffsetManager r = RingBuffer.from(rbRingBuffer);
                    int expoConstAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                                        
                    decodeOptionalDecimalCopy(expoTarget,expoSource,expoConstAbsent,mantToken, reader, rbRingBuffer);
                    
                } else {
                    // increment
                    int expoTarget = expoToken & MAX_INT_INSTANCE_MASK;
                    int expoSource = readFromIdx > 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : expoTarget;
					FieldReferenceOffsetManager r = RingBuffer.from(rbRingBuffer);
                    int expoConstAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                    
                    decodeOptionalDecimalIncrement(expoTarget,expoSource,expoConstAbsent,mantToken, reader, rbRingBuffer);
                    
                }
            } else {
                // default
                FieldReferenceOffsetManager r = RingBuffer.from(rbRingBuffer);
				int expoConstAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                int expoConstDefault = rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK] == 0 ? expoConstAbsent
                        : rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK];
                
                decodeOptionalDecimalDefault(expoConstAbsent,expoConstDefault,mantToken, reader, rbRingBuffer);
                
            }
        }                        
    }

    private void decodeOptionalDecimalDefault(int expoConstAbsent, int expoConstDefault, int mantToken, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalDefaultOptionalMantissaNone(expoConstAbsent, expoConstDefault, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalDefaultOptionalMantissaDelta(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rLongDictionary, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDefaultOptionalMantissaConstant(expoConstAbsent, expoConstDefault, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalDefaultOptionalMantissaCopy(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                } else {
                    // increment
                    genReadDecimalDefaultOptionalMantissaIncrement(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDefaultOptionalMantissaDefault(expoConstAbsent, expoConstDefault, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }
        }
        

    }

    private void decodeOptionalDecimalIncrement(int expoTarget, int expoSource, int expoConstAbsent, int mantToken, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalIncrementOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalIncrementOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalIncrementOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalIncrementOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                } else {
                    // increment
                    genReadDecimalIncrementOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalIncrementOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }
        }
    }

    //copy
    private void decodeOptionalDecimalCopy(int expoTarget, int expoSource, int expoConstAbsent, int mantToken, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        
    //	System.err.println("decode the decimal copy");
    	
    	if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalCopyOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalCopyOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalCopyOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalCopyOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                } else {
                    // increment
                    genReadDecimalCopyOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalCopyOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }
        }
    }

    private void decodeOptionalDecimalConstant(int expoConstAbsent, int expoConstConst, int mantToken, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalConstantOptionalMantissaNone(expoConstAbsent, expoConstConst, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalConstantOptionalMantissaDelta(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalConstantOptionalMantissaConstant(expoConstAbsent, expoConstConst, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalConstantOptionalMantissaCopy(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                } else {
                    // increment
                    genReadDecimalConstantOptionalMantissaIncrement(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalConstantOptionalMantissaDefault(expoConstAbsent, expoConstConst, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }
        }
    }

    //Delta
    private void decodeOptionalDecimalDelta(int expoTarget, int expoSource, int expoConstAbsent, int mantToken, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalDeltaOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalDeltaOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDeltaOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalDeltaOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                } else {
                    // increment
                    genReadDecimalDeltaOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDeltaOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }
        }
    }

    private void decodeOptionalDecimalNone(int expoConstAbsent, int mantToken, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalOptionalMantissaNone(expoConstAbsent, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalOptionalMantissaDelta(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalOptionalMantissaConstant(expoConstAbsent, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }
        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalOptionalMantissaCopy(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                } else {
                    // increment
                    genReadDecimalOptionalMantissaIncrement(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalOptionalMantissaDefault(expoConstAbsent, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.workingHeadPos);
            }
        }
    }
        
    //TODO: B, generator must track previous read from for text etc and  generator must track if previous is not used then do not write to dictionary.
    
    //TODO: B, add new genCopy for each dictionary type and call as needed before the gen methods, LATER: integrate this behavior.
    
    private void dispatchReadByTokenForBytes(int token, PrimitiveReader reader) {
        
        // 0111?
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            // 01110 ByteArray
            readByteArray(token, reader, RingBuffers.get(ringBuffers,activeScriptCursor));
        } else {
            // 01111 ByteArrayOptional
            readByteArrayOptional(token, reader, RingBuffers.get(ringBuffers,activeScriptCursor));
        }
        
    }


    private void readDictionaryFromField(int token) {
        readFromIdx = TokenBuilder.MAX_INSTANCE & token;
    }



    private void readDictionaryReset(int[] members) {

        int limit = members.length;
        int m = 0;   
        if (m<limit) {
	        int idx = members[m++];
	        while (m < limit) {
	            assert (idx < 0);
	
	            if (0 == (idx & 8)) {
	                if (0 == (idx & 4)) {
	                    // integer
	                    while (m < limit && (idx = members[m++]) >= 0) {
	                        genReadDictionaryIntegerReset(idx, rIntInit[idx], rIntDictionary);
	                    }
	                } else {
	                    // long
	                    // System.err.println("long");
	                    while (m < limit && (idx = members[m++]) >= 0) {
	                        genReadDictionaryLongReset(idx, rLongInit[idx], rLongDictionary);
	                    }
	                }
	            } else {
	                if (0 == (idx & 4)) {
	                    // text
	                    while (m < limit && (idx = members[m++]) >= 0) {
	                        genReadDictionaryTextReset(idx, byteHeap);
	                    }
	                } else {
	                    if (0 == (idx & 2)) {
	                        // decimal
	                        throw new UnsupportedOperationException("Implemented as int and long reset");
	                    } else {
	                        // bytes
	                        while (m < limit && (idx = members[m++]) >= 0) {
	                            genReadDictionaryBytesReset(idx, byteHeap);
	                        }
	                    }
	                }
	            }
	        }
        }
    }

    void dispatchReadByTokenForText(int token, PrimitiveReader reader) {
        // System.err.println(" CharToken:"+TokenBuilder.tokenToString(token));

        RingBuffer rb = RingBuffers.get(ringBuffers,activeScriptCursor);
        // 010??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0100?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 01000 TextASCII
                readTextASCII(token, reader, rb);
            } else {
                // 01001 TextASCIIOptional
                readTextASCIIOptional(token, reader, rb);
            }
        } else {
            // 0101?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 01010 TextUTF8
                readTextUTF8(token, reader, rb);
            } else {
                // 01011 TextUTF8Optional
                readTextUTF8Optional(token, reader, rb);
            }
        }
    }

    private void dispatchReadByTokenForLong(int token, PrimitiveReader reader, RingBuffer ringBuffer) {
        // 001??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0010?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00100 LongUnsigned
                readLongUnsigned(token, readFromIdx, reader, ringBuffer);
            } else {
                // 00101 LongUnsignedOptional
                readLongUnsignedOptional(token, readFromIdx, reader, ringBuffer);
            }
        } else {
            // 0011?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00110 LongSigned
                readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader, ringBuffer);
            } else {
                // 00111 LongSignedOptional
                readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader, ringBuffer);
            }
        }
    }

    private void dispatchReadByTokenForInteger(int token, PrimitiveReader reader, RingBuffer ringBuffer) {
        // 000??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0000?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00000 IntegerUnsigned
                readIntegerUnsigned(token, readFromIdx, reader, ringBuffer);
            } else {
                // 00001 IntegerUnsignedOptional
                readIntegerUnsignedOptional(token, readFromIdx, reader, ringBuffer);
            }
        } else {
            // 0001?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00010 IntegerSigned
                readIntegerSigned(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader, ringBuffer);
            } else {
                // 00011 IntegerSignedOptional
                readIntegerSignedOptional(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader, ringBuffer);
            }
        }
    }

    public void readLongSignedOptional(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
					long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongSignedNoneOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongSignedDeltaOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // constant
                FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
				long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);
                long constConst = rLongDictionary[token & instanceMask];

                genReadLongSignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongSignedCopyOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongSignedIncrementOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // default
                FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
				long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);
                long constDefault = rLongDictionary[token & instanceMask] == 0 ? constAbsent
                        : rLongDictionary[token & instanceMask];

                genReadLongSignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }

    }

    public void readLongSigned(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = token & instanceMask;
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    
                    genReadLongSignedNone(target, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);  
                } else {
                    // delta
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    if (source==target) {
                        genReadLongSignedDeltaTS(target, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    } else {
                        genReadLongSignedDelta(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    }
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & instanceMask];
                genReadLongSignedConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = token & instanceMask;
                int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy

                    genReadLongSignedCopy(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // increment

                    genReadLongSignedIncrement(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & instanceMask];

                genReadLongSignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }
    }

    public void readLongUnsignedOptional(int token, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
					long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongUnsignedOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongUnsignedDeltaOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // constant
                FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
				long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);
                long constConst = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongUnsignedCopyOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);

                    genReadLongUnsignedIncrementOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // default
                FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
				long constAbsent = FieldReferenceOffsetManager.getAbsent64Value(r);
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK] == 0 ? constAbsent
                        : rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }

    }

    public void readLongUnsigned(int token, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & MAX_LONG_INSTANCE_MASK;

                    genReadLongUnsignedNone(target, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedDelta(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
                genReadLongConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedCopy(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedIncrement(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }

    }

    public void readIntegerSignedOptional(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
					int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    genReadIntegerSignedOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // constant
                FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
				int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                int constConst = rIntDictionary[token & instanceMask];

                genReadIntegerSignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    genReadIntegerSignedCopyOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    genReadIntegerSignedIncrementOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // default
                FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
				int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                int constDefault = rIntDictionary[token & instanceMask] == 0 ? constAbsent
                        : rIntDictionary[token & instanceMask];

                genReadIntegerSignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }

    }

    public void readIntegerSigned(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & instanceMask;
                    genReadIntegerSignedNone(target, ringBuffer.buffer, ringBuffer.mask, reader, rIntDictionary, ringBuffer.workingHeadPos);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedDelta(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedCopy(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedIncrement(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // default
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerSignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }
    }

    public void readIntegerUnsignedOptional(int token, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    assert (readFromIdx < 0);
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    genReadIntegerUnsignedOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // delta
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // constant
                FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
				int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                int constConst = rIntDictionary[token & MAX_INT_INSTANCE_MASK];

                genReadIntegerUnsignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    genReadIntegerUnsignedCopyOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
					FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                    int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);

                    if (target==source) {
                        genReadIntegerUnsignedIncrementOptionalTS(target, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    } else {
                        genReadIntegerUnsignedIncrementOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    }
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
				FieldReferenceOffsetManager r = RingBuffer.from(ringBuffer);
                int constAbsent = FieldReferenceOffsetManager.getAbsent32Value(r);
                int t = rIntDictionary[source];
                int constDefault = t == 0 ? constAbsent : t - 1;

                genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }

    }

    public void readIntegerUnsigned(int token, int readFromIdx, PrimitiveReader reader, RingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadIntegerUnsigned(target, ringBuffer.buffer, ringBuffer.mask, reader, rIntDictionary, ringBuffer.workingHeadPos);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadIntegerUnsignedDelta(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadIntegerUnsignedConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    
                    if (target==source) {
                        genReadIntegerUnsignedCopyTS(target, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    } else {
                        genReadIntegerUnsignedCopy(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    }
                    
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    if (target==source) {
                        genReadIntegerUnsignedIncrementTS(target, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    } else {
                        genReadIntegerUnsignedIncrement(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
                    }
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadIntegerUnsignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.workingHeadPos);
            }
        }
    }

    private void readLength(int token, int jumpToTarget, int readFromIdx, PrimitiveReader reader) {
        //because the generator hacks this boolean return value it is not helpful here.
        int jumpToNext = activeScriptCursor+1;
        RingBuffer ringBuffer = RingBuffers.get(ringBuffers,activeScriptCursor);
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadLength(target, jumpToTarget, jumpToNext, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, rIntDictionary, reader, this);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadLengthDelta(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, reader, this);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadLengthConstant(constDefault, jumpToTarget, jumpToNext, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, this);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthCopy(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, reader, this);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthIncrement(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer.workingHeadPos, reader, this);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadLengthDefault(constDefault, jumpToTarget, jumpToNext, ringBuffer.buffer, reader, ringBuffer.mask, ringBuffer.workingHeadPos, this);
            }
        }

    }
    
    public int readBytes(int token, PrimitiveReader reader, RingBuffer ringBuffer) {

        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        // System.out.println("reading "+TokenBuilder.tokenToString(token));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            readByteArray(token, reader, ringBuffer);
        } else {
            readByteArrayOptional(token, reader, ringBuffer);
        }
        
        //NOTE: for testing we need to check what was written
        int value = RingBuffer.peek(ringBuffer.buffer, ringBuffer.workingHeadPos.value-2, ringBuffer.mask);
        //if the value is positive it no longer points to the byteHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & MAX_BYTE_INSTANCE_MASK;
    }

    private void readByteArray(int token, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & MAX_BYTE_INSTANCE_MASK;
                    genReadBytesNone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                } else {
                    // tail
                    int idx = token & MAX_BYTE_INSTANCE_MASK;
                    genReadBytesTail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int id =  token & MAX_BYTE_INSTANCE_MASK;
                    int idLength = LocalHeap.length(id,byteHeap);
                    genReadBytesConstant(id, idLength, rbRingBuffer.buffer, rbRingBuffer.mask, RingBuffer.bytesWriteBase(rbRingBuffer), rbRingBuffer.workingHeadPos);
                } else {
                    // delta
                    int idx = token & MAX_BYTE_INSTANCE_MASK;
                    genReadBytesDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                }
            }
        } else {
            int idx = token & MAX_BYTE_INSTANCE_MASK;
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx, 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);
                //TODO: T, need unit tests to cover null vs zero length optional copy byte arrays, this case may not be 100% covered
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK | idx;
                int idLength = LocalHeap.length(initId,byteHeap);
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap);
                
                genReadBytesDefault(idx,initIdx, idLength, 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer));
            }
        }
    }



    private void readByteArrayOptional(int token, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            int idx = token & MAX_BYTE_INSTANCE_MASK;
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNoneOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer));
                } else {
                    // tail
                    genReadBytesTailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId =    idx | LocalHeap.INIT_VALUE_MASK;
                    int constInitLen = LocalHeap.length(constId,byteHeap);
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    int constValue =    idx;
                    int constValueLen = LocalHeap.length(constValue,byteHeap);
                    
                    genReadBytesConstantOptional(constInit, constInitLen, constValue, constValueLen, rbRingBuffer.buffer, rbRingBuffer.mask, reader, RingBuffer.bytesWriteBase(rbRingBuffer), rbRingBuffer.workingHeadPos);
                } else {
                    // delta
                    genReadBytesDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & MAX_BYTE_INSTANCE_MASK;
                genReadBytesCopy(idx, 1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);
            } else {
                // default
                int constValue =    token & MAX_BYTE_INSTANCE_MASK;
                int initId = LocalHeap.INIT_VALUE_MASK | constValue;
                
                int constValueLen = LocalHeap.length(initId,byteHeap);
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap)|LocalHeap.INIT_VALUE_MASK;
                
                genReadBytesDefault(constValue,initIdx, constValueLen,1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer));
            }
        }
    }



    public void openGroup(int token, int pmapSize, PrimitiveReader reader) {

        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        if (pmapSize > 0) {
            PrimitiveReader.openPMap(pmapSize, reader);
        }
    }

    public void closeGroup(int token, int backvalue, PrimitiveReader reader) {

        assert (token < 0);
        assert (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        //TODO: B, this logic seems wrong could both happen?
        
        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            genReadGroupClose(reader);
            
        } 
        
        //token driven logic so nothing will need to be generated for this false case
        if (0!=(token & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER))) {
            int topCursorPos = activeScriptCursor-backvalue;//constant for compiled code
            genReadSequenceClose(topCursorPos, this);
        }         
    }



    public void readTextUTF8Optional(int token, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        int idx = token & MAX_BYTE_INSTANCE_MASK;
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNoneOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer));
                    
                } else {
                    // tail
                    genReadBytesTailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                    
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = (idx) | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    int constValue = idx;
                   
                    genReadBytesConstantOptional(constInit, LocalHeap.initLength(constId, byteHeap), constValue, LocalHeap.initLength(constValue, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, reader, RingBuffer.bytesWriteBase(rbRingBuffer), rbRingBuffer.workingHeadPos);
                                
                } else {
                    // delta   
                    genReadBytesDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                    
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx,1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK | idx;
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap)|LocalHeap.INIT_VALUE_MASK;
                
                genReadBytesDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), 1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos , rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer));
                
            }
        }
    }

    
    public int readASCII(int token, PrimitiveReader reader, RingBuffer ringBuffer) {

        // System.out.println("reading "+TokenBuilder.tokenToString(token));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
        	readTextASCII(token, reader, ringBuffer);
        } else {
        	readTextASCIIOptional(token, reader, ringBuffer);
        }
        
        //NOTE: for testing we need to check what was written
        int value = RingBuffer.peek(ringBuffer.buffer, ringBuffer.workingHeadPos.value-2, ringBuffer.mask);
        //if the value is positive it no longer points to the byteHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & MAX_BYTE_INSTANCE_MASK;
    }
    
    
    public void readTextASCII(int token, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        int idx = token & MAX_BYTE_INSTANCE_MASK;
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadASCIINone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, reader, byteHeap, rbRingBuffer.workingHeadPos, rbRingBuffer);//always dynamic
                } else {
                    // tail
                    genReadASCIITail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);//always dynamic
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    genReadTextConstant(constInit, LocalHeap.initLength(constId, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, RingBuffer.bytesWriteBase(rbRingBuffer), rbRingBuffer.workingHeadPos); //always fixed length
                } else {
                    // delta
                    genReadASCIIDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);//always dynamic
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadASCIICopy(idx, rbRingBuffer.mask, rbRingBuffer.buffer, reader, byteHeap, rbRingBuffer.workingHeadPos, rbRingBuffer); //always dynamic
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK|idx;
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap) |LocalHeap.INIT_VALUE_MASK;
                genReadASCIIDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), rbRingBuffer.mask, rbRingBuffer.buffer, reader, byteHeap, rbRingBuffer.workingHeadPos, rbRingBuffer.byteBuffer, rbRingBuffer.byteMask, rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer)); //dynamic or constant
            }
        }
    }

    public void readTextUTF8(int token, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        int idx = token & MAX_BYTE_INSTANCE_MASK;
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
         
                } else {
                    // tail
                    genReadBytesTail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    genReadBytesConstant(constInit, LocalHeap.initLength(constId, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, RingBuffer.bytesWriteBase(rbRingBuffer), rbRingBuffer.workingHeadPos);
                    
                } else {
                    // delta 
                    genReadBytesDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.workingHeadPos, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx,0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);
                
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK | idx;
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap)|LocalHeap.INIT_VALUE_MASK;
                
                genReadBytesDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos , rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer));
                
            }
        }
    }

    public void readTextASCIIOptional(int token, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        int idx = token & MAX_BYTE_INSTANCE_MASK;
        if (0 == (token & ((4 | 2 | 1) << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                // none
                genReadASCIINone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, reader, byteHeap, rbRingBuffer.workingHeadPos, rbRingBuffer);
            } else {
                // tail
                genReadASCIITailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIIDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);
                } else {
                    int constId = (token & MAX_BYTE_INSTANCE_MASK) | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    //TODO: B, redo text to avoid copy and have usage counter in text heap and, not sure we know which array to read from.
                    int constValue = token & MAX_BYTE_INSTANCE_MASK; 
                    genReadTextConstantOptional(constInit, constValue, LocalHeap.initLength(constId, byteHeap), LocalHeap.initLength(constValue, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, reader, RingBuffer.bytesWriteBase(rbRingBuffer), rbRingBuffer.workingHeadPos);
                }
            } else {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIICopyOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.workingHeadPos, rbRingBuffer);
                } else {
                    // for ASCII we don't need special behavior for optional
                    int initId = LocalHeap.INIT_VALUE_MASK|idx;
                    int initIdx = LocalHeap.initStartOffset(initId, byteHeap) |LocalHeap.INIT_VALUE_MASK;
                    genReadASCIIDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), rbRingBuffer.mask, rbRingBuffer.buffer, reader, byteHeap, rbRingBuffer.workingHeadPos, rbRingBuffer.byteBuffer, rbRingBuffer.byteMask, rbRingBuffer, RingBuffer.bytesWriteBase(rbRingBuffer));
                }
            }
        }
    }

    //Common API for generator.
    
    @Override
    public int getActiveScriptCursor() {
        return activeScriptCursor;
    }

    @Override
    public void setActiveScriptCursor(int cursor) {
        activeScriptCursor = cursor;        
    }

    @Override
    public int getActiveToken() {
        return fullScript[activeScriptCursor];
    }

    @Override
    public long getActiveFieldId() {
        return fieldIdScript[activeScriptCursor];
    }

    @Override
    public String getActiveFieldName() {
        return fieldNameScript[activeScriptCursor]; 
    }

    @Override
    public void runFromCursor(RingBuffer mockRB) {
        decode(null);
    }

    @Override
    public void runBeginMessage() {
        callBeginMessage(null);
    }

    @Override
    public int scriptLength() {
        return fullScript.length;
    }

}
