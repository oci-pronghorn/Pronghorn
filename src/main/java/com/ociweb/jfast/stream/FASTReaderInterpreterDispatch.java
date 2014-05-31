//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.generator.FASTReaderDispatchTemplates;
import com.ociweb.jfast.generator.Supervisor;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FASTReaderInterpreterDispatch extends FASTReaderDispatchTemplates  {


    public int readFromIdx = -1;
    public final int[] rIntInit;
    public final long[] rLongInit;

    public final int MAX_INT_INSTANCE_MASK; 
    public final int byteInstanceMask; 
    public final int MAX_LONG_INSTANCE_MASK;
    public final int MAX_TEXT_INSTANCE_MASK;
    public final int nonTemplatePMapSize;
    public final int[][] dictionaryMembers;
    
    //required for code generation and other documentation/debugging.
    protected final int[] fieldIdScript;
    protected final String[] fieldNameScript;
    
    protected final int[] fullScript;
    
    private final FASTRingBuffer[] arrayRingBuffers;
    
//  arrayRingBuffers = new FASTRingBuffer[1];
//  arrayRingBuffers[0] = FASTDecoder.ringBufferBuilder(8, 7, this);
    
    public FASTReaderInterpreterDispatch(byte[] catBytes) {
        this(new TemplateCatalog(catBytes));
    }    
    
    public FASTReaderInterpreterDispatch(TemplateCatalog catalog) {
        super(catalog);
        
        this.fieldIdScript = catalog.fieldIdScript();
        this.fieldNameScript = catalog.fieldNameScript();
        
        this.rIntInit = catalog.dictionaryFactory().integerDictionary();
        this.rLongInit = catalog.dictionaryFactory().longDictionary();
        
        this.nonTemplatePMapSize = catalog.maxNonTemplatePMapSize();
        this.dictionaryMembers = catalog.dictionaryResetMembers();
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));
        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));
        this.MAX_TEXT_INSTANCE_MASK = (null==textHeap)?TokenBuilder.MAX_INSTANCE:Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount()-1));
        this.byteInstanceMask = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,     byteHeap.itemCount() - 1);
        
        this.fullScript = catalog.fullScript();
        
        this.arrayRingBuffers = new FASTRingBuffer[1];
        this.arrayRingBuffers[0] = FASTDecoder.ringBufferBuilder(8, 7, this);
    }
    
    public FASTReaderInterpreterDispatch(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
            int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript, int maxNestedGroupDepth,
            int primaryRingBits, int textRingBits, int stackPMapInBytes, int preambleSize) {
        super(dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap, fullScript, maxNestedGroupDepth,
                primaryRingBits, textRingBits, 42, null, null,stackPMapInBytes, preambleSize);
        this.fieldIdScript = null;
        this.fieldNameScript = null;
        this.rIntInit = dcr.integerDictionary();
        this.rLongInit = dcr.longDictionary();
        this.nonTemplatePMapSize = nonTemplatePMapSize;
        this.dictionaryMembers = dictionaryMembers;
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));
        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));
        this.MAX_TEXT_INSTANCE_MASK = (null==textHeap)?TokenBuilder.MAX_INSTANCE:Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount()-1));
        this.byteInstanceMask = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,     byteHeap.itemCount() - 1);
        
        this.fullScript = fullScript;
        
        this.arrayRingBuffers = new FASTRingBuffer[1];
        this.arrayRingBuffers[0] = FASTDecoder.ringBufferBuilder(8, 7, this);

    }


    public boolean decode(PrimitiveReader reader) {

        // move everything needed in this tight loop to the stack
        int limit = activeScriptLimit;

        //TODO: AA, based on activeScriptCursor set ring buffer member to be used.
        //FASTRingBuffer ringbuffer = this.ringBuffer(activeScriptCursor)
        
        do {
            int token = fullScript[activeScriptCursor];

            assert (gatherReadData(reader, activeScriptCursor, token));
            
            //System.err.println("write to "+(arrayRingBuffers[0].mask &arrayRingBuffers[0].addPos)+" "+fieldNameScript[activeScriptCursor]+" token: "+TokenBuilder.tokenToString(token));

            // The trick here is to keep all the conditionals in this method and
            // do the work elsewhere.
            if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
                // 0????
                if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                    // 00???
                    if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                        dispatchReadByTokenForInteger(token, reader);
                    } else {
                        dispatchReadByTokenForLong(token, reader);
                    }
                    readFromIdx = -1; //reset for next field where it might be used.
                } else {
                    // 01???
                    if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                        if (readFromIdx>=0) {
                            int source = token & MAX_TEXT_INSTANCE_MASK;
                            int target = readFromIdx & MAX_TEXT_INSTANCE_MASK;
                            genReadCopyText(source, target, textHeap); //NOTE: may find better way to suppor this with text, requires research.
                            readFromIdx = -1; //reset for next field where it might be used.
                        }
                        dispatchReadByTokenForText(token, reader);
                    } else {
                        // 011??
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            decodeDecimal(reader, token, fullScript[++activeScriptCursor]); //pull second token);
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
                        // 100??
                        // Group Type, no others defined so no need to keep
                        // checking
                        if (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) {
                            // this is NOT a message/template so the
                            // non-template pmapSize is used.
                            if (nonTemplatePMapSize > 0) {
                                genReadGroupPMapOpen(nonTemplatePMapSize, reader);
                            }
                        } else {
                            int idx = TokenBuilder.MAX_INSTANCE & token;
                            closeGroup(token,idx, reader);
                            return sequenceCountStackHead>=0;//doSequence;
                        }

                    } else {
                        // 101??

                        // Length Type, no others defined so no need to keep
                        // checking
                        // Only happens once before a node sequence so push it
                        // on the count stack
                        int jumpToTarget = activeScriptCursor + (TokenBuilder.MAX_INSTANCE & fullScript[1+activeScriptCursor]) + 1;
                        //code generator will always return the next step in the script in order to build out all the needed fragments.
                        activeScriptCursor = readLength(token,jumpToTarget, readFromIdx, reader);
                        return sequenceCountStackHead>=0;
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
        return sequenceCountStackHead>=0;//false;
    }

    public void decodeDecimal(PrimitiveReader reader, int expToken, int mantToken) {
        //The previous dictionary value will need to have two read from values 
        //because these leverage the existing int/long implementations we only need to ensure readFromIdx is set between the two.
        // 0110? Decimal and DecimalOptional
        
        int expoToken = expToken;
                
        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_TYPE))) {
            
            readIntegerSigned(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader);
            
            //exponent is NOT optional so do normal mantissa processing.
            if (0 == (mantToken & (1 << TokenBuilder.SHIFT_TYPE))) {
                // not optional
                readLongSigned(mantToken, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
            } else {
                // optional
                readLongSignedOptional(mantToken, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
            }
            
        } else {
            //exponent is optional so the mantissa bit may be absent
            decodeOptionalDecimal(reader, expoToken, mantToken);
        }
        
        readFromIdx = -1; //reset for next field where it might be used. 
    }

    private void decodeOptionalDecimal(PrimitiveReader reader, int expoToken, int mantToken) {
              
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
                    
                    decodeOptionalDecimalNone(expoConstAbsent, mantToken, reader, arrayRingBuffers[0]);
                    
                } else {
                    // delta
                    int expoTarget = expoToken & MAX_INT_INSTANCE_MASK;
                    int expoSource = readFromIdx > 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : expoTarget;
                    int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                    
                    decodeOptionalDecimalDelta(expoTarget,expoSource,expoConstAbsent,mantToken, reader, arrayRingBuffers[0]);
                    
                }
            } else {
                // constant
                int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                int expoConstConst = rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK];
                
                decodeOptionalDecimalConstant(expoConstAbsent,expoConstConst,mantToken, reader, arrayRingBuffers[0]);
                
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
                    
                    decodeOptionalDecimalCopy(expoTarget,expoSource,expoConstAbsent,mantToken, reader, arrayRingBuffers[0]);
                    
                } else {
                    // increment
                    int expoTarget = expoToken & MAX_INT_INSTANCE_MASK;
                    int expoSource = readFromIdx > 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : expoTarget;
                    int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                    
                    decodeOptionalDecimalIncrement(expoTarget,expoSource,expoConstAbsent,mantToken, reader, arrayRingBuffers[0]);
                    
                }
            } else {
                // default
                int expoConstAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(expoToken));
                int expoConstDefault = rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK] == 0 ? expoConstAbsent
                        : rIntDictionary[expoToken & MAX_INT_INSTANCE_MASK];
                
                decodeOptionalDecimalDefault(expoConstAbsent,expoConstDefault,mantToken, reader, arrayRingBuffers[0]);
                
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
                    genReadDecimalDefaultOptionalMantissaNone(expoConstAbsent, expoConstDefault, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalDefaultOptionalMantissaDelta(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rLongDictionary, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDefaultOptionalMantissaConstant(expoConstAbsent, expoConstDefault, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalDefaultOptionalMantissaCopy(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    genReadDecimalDefaultOptionalMantissaIncrement(expoConstAbsent, expoConstDefault, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDefaultOptionalMantissaDefault(expoConstAbsent, expoConstDefault, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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
                    genReadDecimalIncrementOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalIncrementOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalIncrementOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalIncrementOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    genReadDecimalIncrementOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalIncrementOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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
                    genReadDecimalCopyOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalCopyOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalCopyOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalCopyOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    genReadDecimalCopyOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalCopyOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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
                    genReadDecimalConstantOptionalMantissaNone(expoConstAbsent, expoConstConst, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalConstantOptionalMantissaDelta(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalConstantOptionalMantissaConstant(expoConstAbsent, expoConstConst, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalConstantOptionalMantissaCopy(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    genReadDecimalConstantOptionalMantissaIncrement(expoConstAbsent, expoConstConst, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalConstantOptionalMantissaDefault(expoConstAbsent, expoConstConst, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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
                    genReadDecimalDeltaOptionalMantissaNone(expoTarget, expoSource, expoConstAbsent, mantissaTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalDeltaOptionalMantissaDelta(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDeltaOptionalMantissaConstant(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalDeltaOptionalMantissaCopy(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    genReadDecimalDeltaOptionalMantissaIncrement(expoTarget, expoSource, expoConstAbsent, mantissaTarget, mantissaSource, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalDeltaOptionalMantissaDefault(expoTarget, expoSource, expoConstAbsent, mantissaConstDefault, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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
                    genReadDecimalOptionalMantissaNone(expoConstAbsent, mantissaTarget, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                } else {
                    // delta
                    int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                    genReadDecimalOptionalMantissaDelta(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer, rLongDictionary);
                }
            } else {
                // constant
                // always return this required value.
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalOptionalMantissaConstant(expoConstAbsent, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }
        } else {
            // copy, default, increment
            if (0 == (mantToken & (2 << TokenBuilder.SHIFT_OPER))) {
                int mantissaTarget = mantToken & MAX_LONG_INSTANCE_MASK;
                int mantissaSource = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : mantissaTarget;
                // copy, increment
                if (0 == (mantToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy      
                    genReadDecimalOptionalMantissaCopy(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    genReadDecimalOptionalMantissaIncrement(expoConstAbsent, mantissaTarget, mantissaSource, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {

                // default
                long mantissaConstDefault = rLongDictionary[mantToken & MAX_LONG_INSTANCE_MASK];
                genReadDecimalOptionalMantissaDefault(expoConstAbsent, mantissaConstDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }
        }
    }
    
    
    //TODO: B, generator must track previous read from for text etc and  generator must track if previous is not used then do not write to dictionary.
    //TODO: B, add new genCopy for each dictionary type and call as needed before the gen methods, LATER: integrate this behavior.
    
    private void dispatchFieldBytes(int token, PrimitiveReader reader) {
        
        // 0111?
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            // 01110 ByteArray
            readByteArray(token, reader);
        } else {
            // 01111 ByteArrayOptional
            readByteArrayOptional(token, reader);
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
                        genReadDictionaryTextReset(idx, textHeap);
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

    private void dispatchReadByTokenForText(int token, PrimitiveReader reader) {
        // System.err.println(" CharToken:"+TokenBuilder.tokenToString(token));

        // 010??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0100?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 01000 TextASCII
                readTextASCII(token, reader);
            } else {
                // 01001 TextASCIIOptional
                readTextASCIIOptional(token, reader);
            }
        } else {
            // 0101?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 01010 TextUTF8
                readTextUTF8(token, reader);
            } else {
                // 01011 TextUTF8Optional
                readTextUTF8Optional(token, reader);
            }
        }
    }

    private void dispatchReadByTokenForLong(int token, PrimitiveReader reader) {
        // 001??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0010?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00100 LongUnsigned
                readLongUnsigned(token, readFromIdx, reader);
            } else {
                // 00101 LongUnsignedOptional
                readLongUnsignedOptional(token, readFromIdx, reader);
            }
        } else {
            // 0011?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00110 LongSigned
                readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
            } else {
                // 00111 LongSignedOptional
                readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
            }
        }
    }

    private void dispatchReadByTokenForInteger(int token, PrimitiveReader reader) {
        // 000??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0000?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00000 IntegerUnsigned
                readIntegerUnsigned(token, readFromIdx, reader);
            } else {
                // 00001 IntegerUnsignedOptional
                readIntegerUnsignedOptional(token, readFromIdx, reader);
            }
        } else {
            // 0001?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00010 IntegerSigned
                readIntegerSigned(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader);
            } else {
                // 00011 IntegerSignedOptional
                readIntegerSignedOptional(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader);
            }
        }
    }

    public long readLong(int token, PrimitiveReader reader) {

        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readLongUnsigned(token, readFromIdx, reader);
            } else {
                readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readLongUnsignedOptional(token, readFromIdx, reader);
            } else {
                readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
            }
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peekLong(arrayRingBuffers[0].buffer, arrayRingBuffers[0].addPos-2, arrayRingBuffers[0].mask);
    }

    public void readLongSignedOptional(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedNoneOptional(constAbsent, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedDeltaOptional(target, source, constAbsent, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & instanceMask];

                genReadLongSignedConstantOptional(constAbsent, constConst, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
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

                    genReadLongSignedCopyOptional(target, source, constAbsent, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedIncrementOptional(target, source, constAbsent, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & instanceMask] == 0 ? constAbsent
                        : rLongDictionary[token & instanceMask];

                genReadLongSignedDefaultOptional(constAbsent, constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }

    }

    public void readLongSigned(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = token & instanceMask;
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    
                    genReadLongSignedNone(target, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);  
                } else {
                    // delta
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedDelta(target, source, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & instanceMask];
                genReadLongSignedConstant(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, arrayRingBuffers[0]);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = token & instanceMask;
                int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy

                    genReadLongSignedCopy(target, source, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment

                    genReadLongSignedIncrement(target, source, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & instanceMask];

                genReadLongSignedDefault(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }
    }

    private void readLongUnsignedOptional(int token, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedOptional(constAbsent, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedDeltaOptional(target, source, constAbsent, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedConstantOptional(constAbsent, constConst, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
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

                    genReadLongUnsignedCopyOptional(target, source, constAbsent, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedIncrementOptional(target, source, constAbsent, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK] == 0 ? constAbsent
                        : rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefaultOptional(constAbsent, constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }

    }

    private void readLongUnsigned(int token, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & MAX_LONG_INSTANCE_MASK;

                    genReadLongUnsignedNone(target, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedDelta(target, source, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
                genReadLongConstant(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, arrayRingBuffers[0]);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedCopy(target, source, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedIncrement(target, source, rLongDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefault(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }

    }

    public int readInt(int token, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readIntegerUnsigned(token, readFromIdx, reader);
            } else {
                readIntegerSigned(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readIntegerUnsignedOptional(token, readFromIdx, reader);
            } else {
                readIntegerSignedOptional(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader);
            }
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peek(arrayRingBuffers[0].buffer, arrayRingBuffers[0].addPos-1, arrayRingBuffers[0].mask);
    }

    public void readIntegerSignedOptional(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedOptional(constAbsent, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & instanceMask];

                genReadIntegerSignedConstantOptional(constAbsent, constConst, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
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

                    genReadIntegerSignedCopyOptional(target, source, constAbsent, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedIncrementOptional(target, source, constAbsent, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constDefault = rIntDictionary[token & instanceMask] == 0 ? constAbsent
                        : rIntDictionary[token & instanceMask];

                genReadIntegerSignedDefaultOptional(constAbsent, constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }

    }

    public void readIntegerSigned(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & instanceMask;
                    genReadIntegerSignedNone(target, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, rIntDictionary, arrayRingBuffers[0]);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedDelta(target, source, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerConstant(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, arrayRingBuffers[0]);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedCopy(target, source, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedIncrement(target, source, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerSignedDefault(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }
    }

    private void readIntegerUnsignedOptional(int token, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    assert (readFromIdx < 0);
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedOptional(constAbsent, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // delta
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & MAX_INT_INSTANCE_MASK];

                genReadIntegerUnsignedConstantOptional(constAbsent, constConst, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
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

                    genReadIntegerUnsignedCopyOptional(target, source, constAbsent, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedIncrementOptional(target, source, constAbsent, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int t = rIntDictionary[source];
                int constDefault = t == 0 ? constAbsent : t - 1;

                genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }

    }

    private void readIntegerUnsigned(int token, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadIntegerUnsigned(target, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, rIntDictionary, arrayRingBuffers[0]);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadIntegerUnsignedDelta(target, source, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadIntegerUnsignedConstant(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, arrayRingBuffers[0]);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadIntegerUnsignedCopy(target, source, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadIntegerUnsignedIncrement(target, source, rIntDictionary, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadIntegerUnsignedDefault(constDefault, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
            }
        }
    }

    private int readLength(int token, int jumpToTarget, int readFromIdx, PrimitiveReader reader) {
        //because the generator hacks this boolean return value it is not helpful here.
        int jumpToNext = activeScriptCursor+1;
        FASTRingBuffer ringBuffer = arrayRingBuffers[0];
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    return genReadLength(target, jumpToTarget, jumpToNext, ringBuffer.buffer, ringBuffer.mask, ringBuffer, rIntDictionary, reader, this);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    return genReadLengthDelta(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, arrayRingBuffers[0], reader, this);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                return genReadLengthConstant(constDefault, jumpToTarget, jumpToNext, ringBuffer.buffer, ringBuffer.mask, ringBuffer, this);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    return genReadLengthCopy(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer, reader, this);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    return genReadLengthIncrement(target, source, jumpToTarget, jumpToNext, rIntDictionary, ringBuffer.buffer, ringBuffer.mask, ringBuffer, reader, this);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                return genReadLengthDefault(constDefault, jumpToTarget, jumpToNext, ringBuffer.buffer, reader, ringBuffer.mask, ringBuffer, this);
            }
        }
    }
    
    public int readBytes(int token, PrimitiveReader reader) {

        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        // System.out.println("reading "+TokenBuilder.tokenToString(token));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            readByteArray(token, reader);
        } else {
            readByteArrayOptional(token, reader);
        }
        
        //NOTE: for testing we need to check what was written
        int value = FASTRingBuffer.peek(arrayRingBuffers[0].buffer, arrayRingBuffers[0].addPos-2, arrayRingBuffers[0].mask);
        //if the value is positive it no longer points to the textHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & MAX_TEXT_INSTANCE_MASK;
    }

    private void readByteArray(int token, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & byteInstanceMask;
                    genReadBytesNone(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, arrayRingBuffers[0], reader);
                } else {
                    // tail
                    int idx = token & byteInstanceMask;
                    genReadBytesTail(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, arrayRingBuffers[0], reader);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int id =  token & byteInstanceMask;
                    int idLength = byteHeap.length(id);
                    genReadBytesConstant(id, idLength, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, arrayRingBuffers[0]);
                } else {
                    // delta
                    int idx = token & byteInstanceMask;
                    genReadBytesDelta(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, arrayRingBuffers[0], reader);
                }
            }
        } else {
            int idx = token & byteInstanceMask;
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx, 0, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, reader, arrayRingBuffers[0]);
            } else {
                // default
                int initId = TextHeap.INIT_VALUE_MASK | idx;
                int idLength = byteHeap.length(initId);
                int initIdx = byteHeap.initStartOffset(initId);
                
                genReadBytesDefault(idx,initIdx, idLength, 0, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, reader, arrayRingBuffers[0]);
            }
        }
    }



    private void readByteArrayOptional(int token, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            int idx = token & byteInstanceMask;
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNoneOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, arrayRingBuffers[0], reader);
                } else {
                    // tail
                    genReadBytesTailOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, arrayRingBuffers[0], reader);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId =    idx | TextHeap.INIT_VALUE_MASK;
                    int constInitLen = byteHeap.length(constId);
                    int constInit = textHeap.initStartOffset(constId)| TextHeap.INIT_VALUE_MASK;
                    
                    int constValue =    idx;
                    int constValueLen = byteHeap.length(constValue);
                    
                    genReadBytesConstantOptional(constInit, constInitLen, constValue, constValueLen, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // delta
                    genReadBytesDeltaOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, arrayRingBuffers[0], reader);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & byteInstanceMask;
                genReadBytesCopy(idx, 1, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, reader, arrayRingBuffers[0]);
            } else {
                // default
                int constValue =    token & byteInstanceMask;
                int initId = TextHeap.INIT_VALUE_MASK | constValue;
                
                int constValueLen = byteHeap.length(initId);
                int initIdx = textHeap.initStartOffset(initId)|TextHeap.INIT_VALUE_MASK;
                
                genReadBytesDefault(constValue,initIdx, constValueLen,1, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, byteHeap, reader, arrayRingBuffers[0]);
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


    
    public int readText(int token, PrimitiveReader reader) {
        assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii
                readTextASCII(token, reader);
            } else {
                // utf8
                readTextUTF8(token, reader);
            }
        } else {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii optional
                readTextASCIIOptional(token, reader);
            } else {
                // utf8 optional
                readTextUTF8Optional(token, reader);
            }
        }
        
        //NOTE: for testing we need to check what was written
        int value = FASTRingBuffer.peek(arrayRingBuffers[0].buffer, arrayRingBuffers[0].addPos-2, arrayRingBuffers[0].mask);
        //if the value is positive it no longer points to the textHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & MAX_TEXT_INSTANCE_MASK;
    }

    private void readTextUTF8Optional(int token, PrimitiveReader reader) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadUTF8None(idx,1, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                } else {
                    // tail
                    genReadUTF8TailOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = (idx) | TextHeap.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| TextHeap.INIT_VALUE_MASK;
                    
                    int constValue = idx;
                    genReadTextConstantOptional(constInit, constValue, textHeap.initLength(constId), textHeap.initLength(constValue), arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                } else {
                    // delta
                    genReadUTF8DeltaOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadUTF8Copy(idx,1, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, textHeap, arrayRingBuffers[0]);
            } else {
                // default
                int initId = TextHeap.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|TextHeap.INIT_VALUE_MASK;
                
                genReadUTF8Default(idx, initIdx, textHeap.initLength(initId), 1, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
            }
        }
    }

    private void readTextASCII(int token, PrimitiveReader reader) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadASCIINone(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, textHeap, arrayRingBuffers[0]);//always dynamic
                } else {
                    // tail
                    genReadASCIITail(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);//always dynamic
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | TextHeap.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| TextHeap.INIT_VALUE_MASK;
                    
                    genReadTextConstant(constInit, textHeap.initLength(constId), arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, arrayRingBuffers[0]); //always fixed length
                } else {
                    // delta
                    genReadASCIIDelta(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);//always dynamic
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadASCIICopy(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, textHeap, arrayRingBuffers[0]); //always dynamic
            } else {
                // default
                int initId = TextHeap.INIT_VALUE_MASK|idx;
                int initIdx = textHeap.initStartOffset(initId) |TextHeap.INIT_VALUE_MASK;
                genReadASCIIDefault(idx, initIdx, textHeap.initLength(initId), arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, textHeap, arrayRingBuffers[0]); //dynamic or constant
            }
        }
    }

    private void readTextUTF8(int token, PrimitiveReader reader) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadUTF8None(idx,0, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                } else {
                    // tail
                    genReadUTF8Tail(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | TextHeap.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| TextHeap.INIT_VALUE_MASK;
                    
                    genReadTextConstant(constInit, textHeap.initLength(constId), arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, arrayRingBuffers[0]);
                } else {
                    // delta
                    genReadUTF8Delta(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadUTF8Copy(idx,0, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, textHeap, arrayRingBuffers[0]);
            } else {
                // default
                int initId = TextHeap.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|TextHeap.INIT_VALUE_MASK;
                
                genReadUTF8Default(idx, initIdx, textHeap.initLength(initId), 0, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
            }
        }
    }

    private void readTextASCIIOptional(int token, PrimitiveReader reader) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;
        if (0 == (token & ((4 | 2 | 1) << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                // none
                genReadASCIINone(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, textHeap, arrayRingBuffers[0]);
            } else {
                // tail
                genReadASCIITailOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIIDeltaOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                } else {
                    int constId = (token & MAX_TEXT_INSTANCE_MASK) | TextHeap.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| TextHeap.INIT_VALUE_MASK;
                    
                    //TODO: B, redo text to avoid copy and have usage counter in text heap and, not sure we know which array to read from.
                    int constValue = token & MAX_TEXT_INSTANCE_MASK; 
                    genReadTextConstantOptional(constInit, constValue, textHeap.initLength(constId), textHeap.initLength(constValue), arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, arrayRingBuffers[0]);
                }
            } else {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIICopyOptional(idx, arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, textHeap, reader, arrayRingBuffers[0]);
                } else {
                    // for ASCII we don't need special behavior for optional
                    int initId = TextHeap.INIT_VALUE_MASK|idx;
                    int initIdx = textHeap.initStartOffset(initId) |TextHeap.INIT_VALUE_MASK;
                    genReadASCIIDefault(idx, initIdx, textHeap.initLength(initId), arrayRingBuffers[0].buffer, arrayRingBuffers[0].mask, reader, textHeap, arrayRingBuffers[0]);
                }
            }
        }
    }

}
