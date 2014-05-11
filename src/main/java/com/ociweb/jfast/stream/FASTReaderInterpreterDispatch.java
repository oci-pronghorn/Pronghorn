//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.generator.FASTReaderDispatchTemplates;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FASTReaderInterpreterDispatch extends FASTReaderDispatchTemplates  {


    private int readFromIdx = -1;
    public final int[] rIntInit;
    public final long[] rLongInit;

    public final int MAX_INT_INSTANCE_MASK; 
    public final int byteInstanceMask; 
    public final int MAX_LONG_INSTANCE_MASK;
    public final int MAX_TEXT_INSTANCE_MASK;
    public final int nonTemplatePMapSize;
    public final int[][] dictionaryMembers;

    public FASTReaderInterpreterDispatch(byte[] catBytes) {
        this(new TemplateCatalog(catBytes));
    }    
    
    public FASTReaderInterpreterDispatch(TemplateCatalog catalog) {
        super(catalog);
        this.rIntInit = catalog.dictionaryFactory().integerDictionary();
        this.rLongInit = catalog.dictionaryFactory().longDictionary();
        
        this.nonTemplatePMapSize = catalog.maxNonTemplatePMapSize();
        this.dictionaryMembers = catalog.dictionaryResetMembers();
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));
        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));
        this.MAX_TEXT_INSTANCE_MASK = (null==textHeap)?TokenBuilder.MAX_INSTANCE:Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount()-1));
        byteInstanceMask = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,     byteHeap.itemCount() - 1);
    }
    
    public FASTReaderInterpreterDispatch(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
            int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript, int maxNestedGroupDepth,
            int primaryRingBits, int textRingBits) {
        super(dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap, fullScript, maxNestedGroupDepth,
                primaryRingBits, textRingBits);
        this.rIntInit = dcr.integerDictionary();
        this.rLongInit = dcr.longDictionary();
        
        this.nonTemplatePMapSize = nonTemplatePMapSize;
        this.dictionaryMembers = dictionaryMembers;
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));
        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));
        this.MAX_TEXT_INSTANCE_MASK = (null==textHeap)?TokenBuilder.MAX_INSTANCE:Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount()-1));
        byteInstanceMask = null == byteHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE,     byteHeap.itemCount() - 1);
    }

    public FASTRingBuffer ringBuffer() {
        return rbRingBuffer;
    }


    public boolean dispatchReadByToken(PrimitiveReader reader) {

        // move everything needed in this tight loop to the stack
        int limit = activeScriptLimit;

        do {
            int token = fullScript[activeScriptCursor];

            assert (gatherReadData(reader, activeScriptCursor));

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
                            //The previous dictionary value will need to have two read from values 
                            //because these leverage the existing int/long implementations we only need to ensure readFromIdx is set between the two.
                            
                            // 0110? Decimal and DecimalOptional
                            readDecimalExponent(token, reader); //TODO: can decimal copy previous from different spots?
                            
                            //TODO: A, Pass the optional bit flag on to Mantissa for var bit optionals
                            
                            token = fullScript[++activeScriptCursor]; //pull second token
                            //TODO: this token MAY be the readFromIdx and if so we will need to pull another.
                            
                            
                            readDecimalMantissa(token, reader);
                            readFromIdx = -1; //reset for next field where it might be used. TODO: not sure this is right for both parts of decimal
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
                            return doSequence;
                        }

                    } else {
                        // 101??

                        // Length Type, no others defined so no need to keep
                        // checking
                        // Only happens once before a node sequence so push it
                        // on the count stack
                        readLength(token,activeScriptCursor + (TokenBuilder.MAX_INSTANCE & fullScript[1+activeScriptCursor]) + 1, readFromIdx, reader);

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
        return false;
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
    protected int sequenceJump(int length, int cursor) {
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            cursor += (TokenBuilder.MAX_INSTANCE & fullScript[++cursor]) + 1;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = length;
        }
        return cursor;
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
        return FASTRingBuffer.peekLong(rbRingBuffer.buffer, rbRingBuffer.addPos-2, rbRingBuffer.mask);
    }

    private void readLongSignedOptional(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedNoneOptional(constAbsent, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedDeltaOptional(target, source, constAbsent, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & instanceMask];

                genReadLongSignedConstantOptional(constAbsent, constConst, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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

                    genReadLongSignedCopyOptional(target, source, constAbsent, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedIncrementOptional(target, source, constAbsent, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & instanceMask] == 0 ? constAbsent
                        : rLongDictionary[token & instanceMask];

                genReadLongSignedDefaultOptional(constAbsent, constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }
        }

    }

    private void readLongSigned(int token, long[] rLongDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & instanceMask;

                    genReadLongSignedNone(target, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedDelta(target, source, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & instanceMask];
                genReadLongSignedConstant(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedCopy(target, source, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedIncrement(target, source, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & instanceMask];

                genReadLongSignedDefault(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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

                    genReadLongUnsignedOptional(constAbsent, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedDeltaOptional(target, source, constAbsent, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedConstantOptional(constAbsent, constConst, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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

                    genReadLongUnsignedCopyOptional(target, source, constAbsent, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedIncrementOptional(target, source, constAbsent, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK] == 0 ? constAbsent
                        : rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefaultOptional(constAbsent, constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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

                    genReadLongUnsignedNone(target, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedDelta(target, source, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
                genReadLongConstant(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedCopy(target, source, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedIncrement(target, source, rLongDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefault(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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
        return FASTRingBuffer.peek(rbRingBuffer.buffer, rbRingBuffer.addPos-1, rbRingBuffer.mask);
    }

    private void readIntegerSignedOptional(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedOptional(constAbsent, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & instanceMask];

                genReadIntegerSignedConstantOptional(constAbsent, constConst, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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

                    genReadIntegerSignedCopyOptional(target, source, constAbsent, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedIncrementOptional(target, source, constAbsent, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constDefault = rIntDictionary[token & instanceMask] == 0 ? constAbsent
                        : rIntDictionary[token & instanceMask];

                genReadIntegerSignedDefaultOptional(constAbsent, constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }
        }

    }

    private void readIntegerSigned(int token, int[] rIntDictionary, int instanceMask, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & instanceMask;
                    genReadIntegerSignedNone(target, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rIntDictionary, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedDelta(target, source, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerConstant(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedCopy(target, source, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedIncrement(target, source, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerSignedDefault(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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

                    genReadIntegerUnsignedOptional(constAbsent, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & MAX_INT_INSTANCE_MASK];

                genReadIntegerUnsignedConstantOptional(constAbsent, constConst, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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

                    genReadIntegerUnsignedCopyOptional(target, source, constAbsent, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedIncrementOptional(target, source, constAbsent, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int t = rIntDictionary[source];
                int constDefault = t == 0 ? constAbsent : t - 1;

                genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
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
                    genReadIntegerUnsigned(target, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rIntDictionary, rbRingBuffer);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadIntegerUnsignedDelta(target, source, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadIntegerUnsignedConstant(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadIntegerUnsignedIncrement(target, source, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadIntegerUnsignedDefault(constDefault, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
            }
        }
    }

    private void readLength(int token, int jumpToTarget, int readFromIdx, PrimitiveReader reader) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadLength(target, jumpToTarget, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer, rIntDictionary, reader, this);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadLengthDelta(target, source, jumpToTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer, reader, this);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadLengthConstant(constDefault, jumpToTarget, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer, this);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthCopy(target, source, jumpToTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer, reader, this);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthIncrement(target, source, jumpToTarget, rIntDictionary, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer, reader, this);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadLengthDefault(constDefault, jumpToTarget, rbRingBuffer.buffer, reader, rbRingBuffer.mask, rbRingBuffer, this);
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
        int value = FASTRingBuffer.peek(rbRingBuffer.buffer, rbRingBuffer.addPos-2, rbRingBuffer.mask);
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
                    genReadBytesNone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer, reader);
                } else {
                    // tail
                    int idx = token & byteInstanceMask;
                    genReadBytesTail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer, reader);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int id =  token & byteInstanceMask;
                    int idLength = byteHeap.length(id);
                    genReadBytesConstant(id, idLength, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer);
                } else {
                    // delta
                    int idx = token & byteInstanceMask;
                    genReadBytesDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer, reader);
                }
            }
        } else {
            int idx = token & byteInstanceMask;
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx, 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer);
            } else {
                // default
                int initId = FASTReaderInterpreterDispatch.INIT_VALUE_MASK | idx;
                int idLength = byteHeap.length(initId);
                int initIdx = byteHeap.initStartOffset(initId);
                
                genReadBytesDefault(idx,initIdx, idLength, 0, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer);
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
                    genReadBytesNoneOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer, reader);
                } else {
                    // tail
                    genReadBytesTailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer, reader);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId =    idx | FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    int constInitLen = byteHeap.length(constId);
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    
                    int constValue =    idx;
                    int constValueLen = byteHeap.length(constValue);
                    
                    genReadBytesConstantOptional(constInit, constInitLen, constValue, constValueLen, rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    genReadBytesDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, rbRingBuffer, reader);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & byteInstanceMask;
                genReadBytesCopy(idx, 1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer);
            } else {
                // default
                int constValue =    token & byteInstanceMask;
                int initId = FASTReaderInterpreterDispatch.INIT_VALUE_MASK | constValue;
                
                int constValueLen = byteHeap.length(initId);
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                
                genReadBytesDefault(constValue,initIdx, constValueLen,1, rbRingBuffer.buffer, rbRingBuffer.mask, byteHeap, reader, rbRingBuffer);
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
            genReadSequenceClose(backvalue, this);
        } else {
            doSequence = false;
        }
        
    }


    
    public int readDecimalExponent(int token, PrimitiveReader reader) {
        assert (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
        
        // TODO: A, Optional absent null is not implemented yet for Decimal type. should swap in "special" operation for mantissa.
        // This can be done in a similar way to how the token is adjusted in order to use the normal int processing.
        
        //Use Int exponent but we need to shift the bits first to move the operator
        
        int expoToken = token&
                      ((TokenBuilder.MASK_TYPE<<TokenBuilder.SHIFT_TYPE)| 
                       (TokenBuilder.MASK_ABSENT<<TokenBuilder.SHIFT_ABSENT)|
                       (TokenBuilder.MAX_INSTANCE));
        expoToken |= (token>>TokenBuilder.SHIFT_OPER_DECIMAL_EX)&(TokenBuilder.MASK_OPER<<TokenBuilder.SHIFT_OPER);
        expoToken |= 0x80000000;
        
        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_TYPE))) {
            // 00010 IntegerSigned
            readIntegerSigned(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader);
        } else {
            // 00011 IntegerSignedOptional
            readIntegerSignedOptional(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx, reader);
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peek(rbRingBuffer.buffer, rbRingBuffer.addPos-1, rbRingBuffer.mask);
    }

    public long readDecimalMantissa(int token, PrimitiveReader reader) {
        assert (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            // not optional
            readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
        } else {
            // optional
            readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx, reader);
        }
        
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peekLong(rbRingBuffer.buffer, rbRingBuffer.addPos-2, rbRingBuffer.mask);
    };

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
        int value = FASTRingBuffer.peek(rbRingBuffer.buffer, rbRingBuffer.addPos-2, rbRingBuffer.mask);
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
                    genReadUTF8None(idx,1, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                } else {
                    // tail
                    genReadUTF8TailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = (idx) | FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    
                    int constValue = idx;
                    genReadTextConstantOptional(constInit, constValue, textHeap.initLength(constId), textHeap.initLength(constValue), rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                } else {
                    // delta
                    genReadUTF8DeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadUTF8Copy(idx,1, rbRingBuffer.buffer, rbRingBuffer.mask, reader, textHeap, rbRingBuffer);
            } else {
                // default
                int initId = FASTReaderInterpreterDispatch.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                
                genReadUTF8Default(idx, initIdx, textHeap.initLength(initId), 1, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
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
                    genReadASCIINone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, reader, textHeap, rbRingBuffer);//always dynamic
                } else {
                    // tail
                    int fromIdx = readFromIdx;
                    genReadASCIITail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);//always dynamic
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    
                    genReadTextConstant(constInit, textHeap.initLength(constId), rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer); //always fixed length
                } else {
                    // delta
                    genReadASCIIDelta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);//always dynamic
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadASCIICopy(idx, rbRingBuffer.buffer, rbRingBuffer.mask, reader, textHeap, rbRingBuffer); //always dynamic
            } else {
                // default
                int initId = FASTReaderInterpreterDispatch.INIT_VALUE_MASK|idx;
                int initIdx = textHeap.initStartOffset(initId) |FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                genReadASCIIDefault(idx, initIdx, textHeap.initLength(initId), rbRingBuffer.buffer, rbRingBuffer.mask, reader, textHeap, rbRingBuffer); //dynamic or constant
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
                    genReadUTF8None(idx,0, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                } else {
                    // tail
                    genReadUTF8Tail(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    
                    genReadTextConstant(constInit, textHeap.initLength(constId), rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer);
                } else {
                    // delta
                    genReadUTF8Delta(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadUTF8Copy(idx,0, rbRingBuffer.buffer, rbRingBuffer.mask, reader, textHeap, rbRingBuffer);
            } else {
                // default
                int initId = FASTReaderInterpreterDispatch.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                
                genReadUTF8Default(idx, initIdx, textHeap.initLength(initId), 0, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
            }
        }
    }

    private void readTextASCIIOptional(int token, PrimitiveReader reader) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;
        if (0 == (token & ((4 | 2 | 1) << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                // none
                genReadASCIINone(idx, rbRingBuffer.buffer, rbRingBuffer.mask, reader, textHeap, rbRingBuffer);
            } else {
                // tail
                genReadASCIITailOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIIDeltaOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                } else {
                    int constId = (token & MAX_TEXT_INSTANCE_MASK) | FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    
                    //TODO: redo text to avoid copy and have usage counter in text heap.
                    int constValue = token & MAX_TEXT_INSTANCE_MASK; //TODO: A, is this real? how do we know where to copy from ?
                    genReadTextConstantOptional(constInit, constValue, textHeap.initLength(constId), textHeap.initLength(constValue), rbRingBuffer.buffer, rbRingBuffer.mask, reader, rbRingBuffer);
                }
            } else {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIICopyOptional(idx, rbRingBuffer.buffer, rbRingBuffer.mask, textHeap, reader, rbRingBuffer);
                } else {
                    // for ASCII we don't need special behavior for optional
                    int initId = FASTReaderInterpreterDispatch.INIT_VALUE_MASK|idx;
                    int initIdx = textHeap.initStartOffset(initId) |FASTReaderInterpreterDispatch.INIT_VALUE_MASK;
                    genReadASCIIDefault(idx, initIdx, textHeap.initLength(initId), rbRingBuffer.buffer, rbRingBuffer.mask, reader, textHeap, rbRingBuffer);
                }
            }
        }
    }

}
