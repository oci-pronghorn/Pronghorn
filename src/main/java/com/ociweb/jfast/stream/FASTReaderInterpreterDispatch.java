//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.FASTReaderDispatchTemplates;
import com.ociweb.jfast.generator.Supervisor;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FASTReaderInterpreterDispatch extends FASTReaderDispatchTemplates implements GeneratorDriving  {


    //TODO: X, in this class we can determine when the pmap will switch to the next 7 bits and at runtime this is fixed and need not be computed.
    
    public int readFromIdx = -1;
    public final int[] rIntInit;
    public final long[] rLongInit;

    public final int MAX_INT_INSTANCE_MASK; 
    public final int byteInstanceMask; 
    public final int MAX_LONG_INSTANCE_MASK;
    public final int MAX_TEXT_INSTANCE_MASK;
    
    public final int nonTemplatePMapSize;
    public final int maxTemplatePMapSize;
    public final byte preambleDataLength;
    
    public final int[][] dictionaryMembers;
    
    //required for code generation and other documentation/debugging.
    protected final int[] fieldIdScript;
    protected final String[] fieldNameScript;
    
    protected final int[] fullScript;

        
    public FASTReaderInterpreterDispatch(byte[] catBytes) {
        this(new TemplateCatalogConfig(catBytes));
    }    
    
    public FASTReaderInterpreterDispatch(TemplateCatalogConfig catalog) {
        super(catalog);
        
        this.fieldIdScript = catalog.fieldIdScript();
        this.fieldNameScript = catalog.fieldNameScript();
        
        this.rIntInit = catalog.dictionaryFactory().integerDictionary();
        this.rLongInit = catalog.dictionaryFactory().longDictionary();
        
        this.nonTemplatePMapSize = catalog.maxNonTemplatePMapSize();
        this.maxTemplatePMapSize = catalog.maxTemplatePMapSize();
        this.preambleDataLength = (byte)catalog.clientConfig().getPreableBytes();
        
        this.dictionaryMembers = catalog.dictionaryResetMembers();
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));
        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));
        this.MAX_TEXT_INSTANCE_MASK = (null==byteHeap)?TokenBuilder.MAX_INSTANCE:Math.min(TokenBuilder.MAX_INSTANCE, (LocalHeap.itemCount(byteHeap)-1));
        this.byteInstanceMask = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,     LocalHeap.itemCount(byteHeap) - 1);
        
        this.fullScript = catalog.fullScript();        
        
    }

    public void callBeginMessage(PrimitiveReader reader) {
        beginMessage(reader);
    }
    
    private void beginMessage(PrimitiveReader reader) {
        // get next token id then immediately start processing the script
        // /read prefix bytes if any (only used by some implementations)
        //ring buffer is build on int32s so the implementation limits preamble to units of 4
        assert ((this.preambleDataLength&0x3)==0) : "Preable may only be in units of 4 bytes";
        assert (this.preambleDataLength<=8) : "Preable may only be 8 or fewer bytes";
        //Hold the preamble value here until we know the template and therefore the needed ring buffer.
        
        
        //TODO: redo to check for space before read so these values need not be saved in the base decoder class.
        
        //break out into series of gen calls to save int somewhere. units of 4 only.
        int p = this.preambleDataLength;
        if (p>0) {
            genReadPreambleA(reader, this);
             if (p>4) {
                genReadPreambleB(reader, this);
                assert(p==8) : "Unsupported large preamble";
            }
        }                                        
        
        
        // /////////////////
        // open message (special type of group)
        genReadTemplateId(preambleDataLength, maxTemplatePMapSize, reader, this); 
        
        
        //TODO: X, add mode for reading the preamble above but NOT writing to ring buffer because it is not needed.
        //break out into second half of gen.
        p = this.preambleDataLength;
        if (p>0) {
            genWritePreambleA(this);
            if (p>4) {
                genWritePreambleB(this);
            }
        }
        genWriteTemplateId(this);
    }
    
    //TODO: MUST only call when we know there is room for the biggest known fragment, must avoid additional checks.
    public int decode(PrimitiveReader reader) {

        if (activeScriptCursor<0) {
            if (PrimitiveReader.isEOF(reader)) { 
                return -1;
            }  
            beginMessage(reader); 
        }
        
        // move everything needed in this tight loop to the stack
        int limit = activeScriptLimit; //TODO: C, remvoe this by using the stackHead depth for all wrapping groups

        final FASTRingBuffer rbRingBuffer = RingBuffers.get(ringBuffers,activeScriptCursor);
        
        int token = fullScript[activeScriptCursor];

        do {
            token = fullScript[activeScriptCursor];
    
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
                            int source = token & MAX_TEXT_INSTANCE_MASK;
                            int target = readFromIdx & MAX_TEXT_INSTANCE_MASK;
                            genReadCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
                            readFromIdx = -1; //reset for next field where it might be used.
                        }
                        dispatchReadByTokenForText(token, reader);
                    } else {
                        // 011??
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            //Exponent token comes first and is of type Decimal, the Mantissa is second and is of type Long
                            decodeDecimal(reader, token, fullScript[++activeScriptCursor],rbRingBuffer); //pull second token);
                        } else {
                            if (readFromIdx>=0) {
                                int source = token & MAX_TEXT_INSTANCE_MASK;
                                int target = readFromIdx & MAX_TEXT_INSTANCE_MASK;
                                genReadCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
                                readFromIdx = -1; //reset for next field where it might be used.
                            }
                            dispatchFieldBytes(token, reader);
                        }
                    }
                }
            } else {
                // 1????
                if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                    // 10???
                    if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                        
                        //group.
                        
                            // 100??
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
                                   // System.err.println("xxx");
                                    //Do nothing because this is done by Reactor at this time.
                                    
                                }                                
                                
                            } else {
                                //Close group
                                
                                int idx = TokenBuilder.MAX_INSTANCE & token;
                                closeGroup(token,idx, reader);
                                break;
                                //FASTRingBuffer.unBlockFragment(rbRingBuffer); 
                                //return sequenceCountStackHead>=0;//doSequence;
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
                        //FASTRingBuffer.unBlockFragment(rbRingBuffer);
                        //return sequenceCountStackHead>=0;
                        //return true;

                    }
                } else {
                    // 11???
                    // Dictionary Type, no others defined so no need to keep
                    // checking
                    if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                        readDictionaryReset2(dictionaryMembers[TokenBuilder.MAX_INSTANCE & token]);
                    } else {
                        // OperatorMask.Dictionary_Read_From 0001
                        // next read will need to use this index to pull the
                        // right initial value.
                        // after it is used it must be cleared/reset to -1
                        readDictionaryFromField(token);
                    }
                }
            }
        } while (++activeScriptCursor < limit);
        FASTRingBuffer.unBlockFragment(rbRingBuffer.headPos,rbRingBuffer.addPos);
        
        //TODO: B, on normal fixed closed this is not needed so the conditional can be skipped.
        genReadGroupCloseMessage(reader, this); 
        return ringBufferIdx;
    }

    public void decodeDecimal(PrimitiveReader reader, int expToken, int mantToken, FASTRingBuffer rbRingBuffer) {
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

    private void decodeOptionalDecimal(PrimitiveReader reader, int expoToken, int mantToken, FASTRingBuffer rbRingBuffer) {
              
       // System.err.println("decode : Exp:"+TokenBuilder.tokenToString(expoToken)+" Mant: "+TokenBuilder.tokenToString(mantToken));
       //In this method we split out by exponent operator then call the specific method needed
       //for the remaining split by mantissa.
        
        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (expoToken & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (expoToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                    
                    decodeOptionalDecimalNone(expoConstAbsent, mantToken, reader, rbRingBuffer);
                    
                } else {
                    // delta
                    int expoTarget = expoToken & MAX_INT_INSTANCE_MASK;
                    int expoSource = readFromIdx > 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : expoTarget;
                    int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                    
                    decodeOptionalDecimalDelta(expoTarget,expoSource,expoConstAbsent,mantToken, reader, rbRingBuffer);
                    
                }
            } else {
                // constant
                int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
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
                    int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                    
                    decodeOptionalDecimalCopy(expoTarget,expoSource,expoConstAbsent,mantToken, reader, rbRingBuffer);
                    
                } else {
                    // increment
                    int expoTarget = expoToken & MAX_INT_INSTANCE_MASK;
                    int expoSource = readFromIdx > 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : expoTarget;
                    int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                    
                    decodeOptionalDecimalIncrement(expoTarget,expoSource,expoConstAbsent,mantToken, reader, rbRingBuffer);
                    
                }
            } else {
                // default
                int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                int expoConstDefault = rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK] == 0 ? expoConstAbsent
                        : rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK];
                
                decodeOptionalDecimalDefault(expoConstAbsent,expoConstDefault,mantToken, reader, rbRingBuffer);
                
            }
        }                        
    }

    private void decodeOptionalDecimalDefault(int expoConstAbsent, int expoConstDefault, int mantToken, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalDefaultOptionalMantissaNone(expoConstAbsent, expoConstDefault, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalDefaultOptionalMantissaDelta(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rLongDictionary, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDefaultOptionalMantissaConstant(expoConstAbsent, expoConstDefault, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalDefaultOptionalMantissaCopy(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                } else {
                    // increment
                    genReadDecimalDefaultOptionalMantissaIncrement(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDefaultOptionalMantissaDefault(expoConstAbsent, expoConstDefault, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }
        }
        

    }

    private void decodeOptionalDecimalIncrement(int expoTarget, int expoSource, int expoConstAbsent, int mantToken, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalIncrementOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalIncrementOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalIncrementOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalIncrementOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                } else {
                    // increment
                    genReadDecimalIncrementOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalIncrementOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }
        }
    }

    //copy
    private void decodeOptionalDecimalCopy(int expoTarget, int expoSource, int expoConstAbsent, int mantToken, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalCopyOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalCopyOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalCopyOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalCopyOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                } else {
                    // increment
                    genReadDecimalCopyOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalCopyOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }
        }
    }

    private void decodeOptionalDecimalConstant(int expoConstAbsent, int expoConstConst, int mantToken, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalConstantOptionalMantissaNone(expoConstAbsent, expoConstConst, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalConstantOptionalMantissaDelta(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalConstantOptionalMantissaConstant(expoConstAbsent, expoConstConst, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalConstantOptionalMantissaCopy(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                } else {
                    // increment
                    genReadDecimalConstantOptionalMantissaIncrement(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalConstantOptionalMantissaDefault(expoConstAbsent, expoConstConst, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }
        }
    }

    //Delta
    private void decodeOptionalDecimalDelta(int expoTarget, int expoSource, int expoConstAbsent, int mantToken, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalDeltaOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalDeltaOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDeltaOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalDeltaOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                } else {
                    // increment
                    genReadDecimalDeltaOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDeltaOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }
        }
    }

    private void decodeOptionalDecimalNone(int expoConstAbsent, int mantToken, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        
        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                // none, delta
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadDecimalOptionalMantissaNone(expoConstAbsent, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalOptionalMantissaDelta(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalOptionalMantissaConstant(expoConstAbsent, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }
        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalOptionalMantissaCopy(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                } else {
                    // increment
                    genReadDecimalOptionalMantissaIncrement(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalOptionalMantissaDefault(expoConstAbsent, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
            }
        }
    }
    
    
    //TODO: B, generator must track previous read from for text etc and  generator must track if previous is not used then do not write to dictionary.
    //TODO: B, add new genCopy for each dictionary type and call as needed before the gen methods, LATER: integrate this behavior.
    
    private void dispatchFieldBytes(int token, PrimitiveReader reader) {
        
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



    private void readDictionaryReset2(int[] members) {

        int limit = members.length;
        int m = 0;
        int idx = members[m++]; // assumes that a dictionary always has at lest
                                // 1 member
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

    void dispatchReadByTokenForText(int token, PrimitiveReader reader) {
        // System.err.println(" CharToken:"+TokenBuilder.tokenToString(token));

        FASTRingBuffer rb = RingBuffers.get(ringBuffers,activeScriptCursor);
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

    private void dispatchReadByTokenForLong(int token, PrimitiveReader reader, FASTRingBuffer ringBuffer) {
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

    private void dispatchReadByTokenForInteger(int token, PrimitiveReader reader, FASTRingBuffer ringBuffer) {
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

    public void readLongSignedOptional(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedNoneOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedDeltaOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & instanceMask];

                genReadLongSignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedCopyOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedIncrementOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & instanceMask] == 0 ? constAbsent
                        : rLongDictionary[token & instanceMask];

                genReadLongSignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }

    }

    public void readLongSigned(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = token & instanceMask;
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    
                    genReadLongSignedNone(target, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);  
                } else {
                    // delta
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedDelta(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & instanceMask];
                genReadLongSignedConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = token & instanceMask;
                int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy

                    genReadLongSignedCopy(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // increment

                    genReadLongSignedIncrement(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & instanceMask];

                genReadLongSignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }
    }

    public void readLongUnsignedOptional(int token, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedDeltaOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedCopyOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedIncrementOptional(target, source, constAbsent, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK] == 0 ? constAbsent
                        : rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }

    }

    public void readLongUnsigned(int token, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & MAX_LONG_INSTANCE_MASK;

                    genReadLongUnsignedNone(target, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedDelta(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
                genReadLongConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedCopy(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedIncrement(target, source, rLongDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }

    }

    public void readIntegerSignedOptional(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & instanceMask];

                genReadIntegerSignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedCopyOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedIncrementOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constDefault = rIntDictionary[token & instanceMask] == 0 ? constAbsent
                        : rIntDictionary[token & instanceMask];

                genReadIntegerSignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }

    }

    public void readIntegerSigned(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & instanceMask;
                    genReadIntegerSignedNone(target, ringBuffer.buffer, ringBuffer.mask, reader, rIntDictionary, ringBuffer.addPos);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedDelta(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedCopy(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedIncrement(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerSignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }
    }

    public void readIntegerUnsignedOptional(int token, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    assert (readFromIdx < 0);
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedOptional(constAbsent, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // delta
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & MAX_INT_INSTANCE_MASK];

                genReadIntegerUnsignedConstantOptional(constAbsent, constConst, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedCopyOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedIncrementOptional(target, source, constAbsent, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int t = rIntDictionary[source];
                int constDefault = t == 0 ? constAbsent : t - 1;

                genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }

    }

    public void readIntegerUnsigned(int token, int readFromIdx, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadIntegerUnsigned(target, ringBuffer.buffer, ringBuffer.mask, reader, rIntDictionary, ringBuffer.addPos);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadIntegerUnsignedDelta(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadIntegerUnsignedConstant(constDefault, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos);
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
                        genReadIntegerUnsignedCopyUnWatched(target, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                    } else {
                        genReadIntegerUnsignedCopy(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                    }
                    
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadIntegerUnsignedIncrement(target, source, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadIntegerUnsignedDefault(constDefault, ringBuffer.buffer, ringBuffer.mask, reader, ringBuffer.addPos);
            }
        }
    }

    private void readLength(int token, int jumpToTarget, int readFromIdx, PrimitiveReader reader) {
        //because the generator hacks this boolean return value it is not helpful here.
        int jumpToNext = activeScriptCursor+1;
        FASTRingBuffer ringBuffer = RingBuffers.get(ringBuffers,activeScriptCursor);
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadLength(target, jumpToTarget, jumpToNext, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos, rIntDictionary, reader, this);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadLengthDelta(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos, reader, this);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadLengthConstant(constDefault, jumpToTarget, jumpToNext, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos, this);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthCopy(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos, reader, this);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthIncrement(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer.addPos, reader, this);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadLengthDefault(constDefault, jumpToTarget, jumpToNext, ringBuffer.buffer, reader, ringBuffer.mask, ringBuffer.addPos, this);
            }
        }

    }
    
    public int readBytes(int token, PrimitiveReader reader, FASTRingBuffer ringBuffer) {

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
        int value = FASTRingBuffer.peek(ringBuffer.buffer, ringBuffer.addPos.value-2, ringBuffer.mask);
        //if the value is positive it no longer points to the byteHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & MAX_TEXT_INSTANCE_MASK;
    }

    private void readByteArray(int token, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & byteInstanceMask;
                    genReadBytesNone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                } else {
                    // tail
                    int idx = token & byteInstanceMask;
                    genReadBytesTail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int id =  token & byteInstanceMask;
                    int idLength = LocalHeap.length(id,byteHeap);
                    genReadBytesConstant(id, idLength, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.addPos);
                } else {
                    // delta
                    int idx = token & byteInstanceMask;
                    genReadBytesDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                }
            }
        } else {
            int idx = token & byteInstanceMask;
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx, 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK | idx;
                int idLength = LocalHeap.length(initId,byteHeap);
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap);
                
                genReadBytesDefault(idx,initIdx, idLength, 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
            }
        }
    }



    private void readByteArrayOptional(int token, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            int idx = token & byteInstanceMask;
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNoneOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                } else {
                    // tail
                    genReadBytesTailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
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
                    
                    genReadBytesConstantOptional(constInit, constInitLen, constValue, constValueLen, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                } else {
                    // delta
                    genReadBytesDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & byteInstanceMask;
                genReadBytesCopy(idx, 1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
            } else {
                // default
                int constValue =    token & byteInstanceMask;
                int initId = LocalHeap.INIT_VALUE_MASK | constValue;
                
                int constValueLen = LocalHeap.length(initId,byteHeap);
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap)|LocalHeap.INIT_VALUE_MASK;
                
                genReadBytesDefault(constValue,initIdx, constValueLen,1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
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

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            genReadGroupClose(reader);
            
        }

        //token driven logic so nothing will need to be generated for this false case
        if (0!=(token & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER))) {
            int topCursorPos = activeScriptCursor-backvalue;//constant for compiled code
            genReadSequenceClose(backvalue, topCursorPos, this);
        }         
    }



    public void readTextUTF8Optional(int token, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNoneOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                    
                } else {
                    // tail
                    genReadBytesTailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                    
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = (idx) | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    int constValue = idx;
                   
                    genReadBytesConstantOptional(constInit, LocalHeap.initLength(constId, byteHeap), constValue, LocalHeap.initLength(constValue, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                                
                } else {
                    // delta   
                    genReadBytesDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                    
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx,1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK | idx;
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap)|LocalHeap.INIT_VALUE_MASK;
                
                genReadBytesDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), 1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos , rbRingBuffer);
                
            }
        }
    }

    public void readTextASCII(int token, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadASCIINone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, reader, byteHeap, rbRingBuffer.addPos, rbRingBuffer);//always dynamic
                } else {
                    // tail
                    genReadASCIITail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);//always dynamic
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    genReadTextConstant(constInit, LocalHeap.initLength(constId, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.addPos); //always fixed length
                } else {
                    // delta
                    genReadASCIIDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);//always dynamic
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadASCIICopy(idx, rbRingBuffer.mask, rbRingBuffer.buffer, reader, byteHeap, rbRingBuffer.addPos, rbRingBuffer); //always dynamic
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK|idx;
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap) |LocalHeap.INIT_VALUE_MASK;
                genReadASCIIDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), rbRingBuffer.mask, rbRingBuffer.buffer, reader, byteHeap, rbRingBuffer.addPos, rbRingBuffer); //dynamic or constant
            }
        }
    }

    public void readTextUTF8(int token, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
         
                } else {
                    // tail
                    genReadBytesTail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    genReadBytesConstant(constInit, LocalHeap.initLength(constId, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.addPos);
                    
                } else {
                    // delta 
                    genReadBytesDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer.addPos, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx,0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
                
            } else {
                // default
                int initId = LocalHeap.INIT_VALUE_MASK | idx;
                int initIdx = LocalHeap.initStartOffset(initId, byteHeap)|LocalHeap.INIT_VALUE_MASK;
                
                genReadBytesDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos , rbRingBuffer);
                
            }
        }
    }

    public void readTextASCIIOptional(int token, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;
        if (0 == (token & ((4 | 2 | 1) << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                // none
                genReadASCIINone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, reader, byteHeap, rbRingBuffer.addPos, rbRingBuffer);
            } else {
                // tail
                genReadASCIITailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIIDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
                } else {
                    int constId = (token & MAX_TEXT_INSTANCE_MASK) | LocalHeap.INIT_VALUE_MASK;
                    int constInit = LocalHeap.initStartOffset(constId, byteHeap)| LocalHeap.INIT_VALUE_MASK;
                    
                    //TODO: B, redo text to avoid copy and have usage counter in text heap and, not sure we know which array to read from.
                    int constValue = token & MAX_TEXT_INSTANCE_MASK; 
                    genReadTextConstantOptional(constInit, constValue, LocalHeap.initLength(constId, byteHeap), LocalHeap.initLength(constValue, byteHeap), rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer.addPos);
                }
            } else {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIICopyOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer.addPos, rbRingBuffer);
                } else {
                    // for ASCII we don't need special behavior for optional
                    int initId = LocalHeap.INIT_VALUE_MASK|idx;
                    int initIdx = LocalHeap.initStartOffset(initId, byteHeap) |LocalHeap.INIT_VALUE_MASK;
                    genReadASCIIDefault(idx, initIdx, LocalHeap.initLength(initId, byteHeap), rbRingBuffer.mask, rbRingBuffer.buffer, reader, byteHeap, rbRingBuffer.addPos, rbRingBuffer);
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
    public void setActiveScriptLimit(int limit) {
        activeScriptLimit = limit;
        
    }

    @Override
    public int getActiveToken() {
        return fullScript[activeScriptCursor];
    }

    @Override
    public int getActiveFieldId() {
        return fieldIdScript[activeScriptCursor];
    }

    @Override
    public String getActiveFieldName() {
        return fieldNameScript[activeScriptCursor]; 
    }

    @Override
    public void runFromCursor() {
        decode(null);
    }

    @Override
    public void runBeginMessage() {
        callBeginMessage(null);
    }

}
