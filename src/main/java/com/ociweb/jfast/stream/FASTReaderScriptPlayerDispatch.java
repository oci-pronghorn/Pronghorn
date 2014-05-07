//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

public class FASTReaderScriptPlayerDispatch extends FASTReaderDispatchTemplates  {



    private int readFromIdx = -1;


    public FASTReaderScriptPlayerDispatch(PrimitiveReader reader, DictionaryFactory dcr, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
            int maxNestedGroupDepth, int primaryRingBits, int textRingBits) {
        super(reader, dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap, fullScript,
                maxNestedGroupDepth, primaryRingBits, textRingBits);
    }

    public FASTRingBuffer ringBuffer() {
        return rbRingBuffer;
    }




    
    // long totalReadFields = 0;

    // The nested IFs for this short tree are slightly faster than switch
    // for more JVM configurations and when switch is faster (eg lots of JVM
    // -XX: args)
    // it is only slightly faster.

    // For a dramatic speed up of this dispatch code look into code generation
    // of the
    // script as a series of function calls against the specific
    // FieldReader*.class
    // This is expected to save 4ns per field on the AMD hardware or a speedup >
    // 12%.

    // Yet another idea is to process two tokens together and add a layer of
    // mangled functions that have "pre-coded" scripts. What if we just repeat
    // the same type?

    // totalReadFields++;

    
    
    // THOUGHTS
    // Build fixed length and put all in ring buffer, consumers can
    // look at leading int to determine what kind of message they have
    // and the script position can be looked up by field id once for their
    // needs.
    // each "mini-message is expected to be very small" and all in cache
    // package protected, unless we find a need to expose it?
    public boolean dispatchReadByToken() {

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
                        dispatchReadByTokenForInteger(token);
                    } else {
                        dispatchReadByTokenForLong(token);
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
                        dispatchReadByTokenForText(token);
                    } else {
                        // 011??
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            //The previous dictionary value will need to have two read from values 
                            //because these leverage the existing int/long implementations we only need to ensure readFromIdx is set between the two.
                            
                            // 0110? Decimal and DecimalOptional
                            readDecimalExponent(token); //TODO: can decimal copy previous from different spots?
                            
                            //TODO: A, Pass the optional bit flag on to Mantissa for var bit optionals
                            
                            token = fullScript[++activeScriptCursor]; //pull second token
                            //TODO: this token MAY be the readFromIdx and if so we will need to pull another.
                            
                            
                            readDecimalMantissa(token);
                            readFromIdx = -1; //reset for next field where it might be used. TODO: not sure this is right for both parts of decimal
                        } else {
                            if (readFromIdx>=0) {
                                int source = token & MAX_TEXT_INSTANCE_MASK;
                                int target = readFromIdx & MAX_TEXT_INSTANCE_MASK;
                                genReadCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
                                readFromIdx = -1; //reset for next field where it might be used.
                            }
                            dispatchFieldBytes(token);
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
                            closeGroup(token,idx);
                            return doSequence;
                        }

                    } else {
                        // 101??

                        // Length Type, no others defined so no need to keep
                        // checking
                        // Only happens once before a node sequence so push it
                        // on the count stack
                        readLength(token,activeScriptCursor + (TokenBuilder.MAX_INSTANCE & fullScript[1+activeScriptCursor]) + 1, readFromIdx);

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
    
    private void dispatchFieldBytes(int token) {
        
        // 0111?
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            // 01110 ByteArray
            readByteArray(token);
        } else {
            // 01111 ByteArrayOptional
            readByteArrayOptional(token);
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
                        genReadDictionaryIntegerReset(idx, rIntDictionary, rIntInit);
                    }
                } else {
                    // long
                    // System.err.println("long");
                    while (m < limit && (idx = members[m++]) >= 0) {
                        genReadDictionaryLongReset(idx, rLongDictionary, rLongInit);
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

    private void dispatchReadByTokenForText(int token) {
        // System.err.println(" CharToken:"+TokenBuilder.tokenToString(token));

        // 010??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0100?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 01000 TextASCII
                readTextASCII(token);
            } else {
                // 01001 TextASCIIOptional
                readTextASCIIOptional(token);
            }
        } else {
            // 0101?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 01010 TextUTF8
                readTextUTF8(token);
            } else {
                // 01011 TextUTF8Optional
                readTextUTF8Optional(token);
            }
        }
    }

    private void dispatchReadByTokenForLong(int token) {
        // 001??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0010?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00100 LongUnsigned
                readLongUnsigned(token, readFromIdx);
            } else {
                // 00101 LongUnsignedOptional
                readLongUnsignedOptional(token, readFromIdx);
            }
        } else {
            // 0011?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00110 LongSigned
                readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx);
            } else {
                // 00111 LongSignedOptional
                readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx);
            }
        }
    }

    private void dispatchReadByTokenForInteger(int token) {
        // 000??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0000?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00000 IntegerUnsigned
                readIntegerUnsigned(token, readFromIdx);
            } else {
                // 00001 IntegerUnsignedOptional
                readIntegerUnsignedOptional(token, readFromIdx);
            }
        } else {
            // 0001?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00010 IntegerSigned
                readIntegerSigned(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx);
            } else {
                // 00011 IntegerSignedOptional
                readIntegerSignedOptional(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx);
            }
        }
    }

    public long readLong(int token) {

        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readLongUnsigned(token, readFromIdx);
            } else {
                readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readLongUnsignedOptional(token, readFromIdx);
            } else {
                readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx);
            }
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peekLong(rbB, rbRingBuffer.addPos-2, rbMask);
    }

    private void readLongSignedOptional(int token, long[] rLongDictionary, int instanceMask, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedNoneOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedDeltaOptional(target, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & instanceMask];

                genReadLongSignedConstantOptional(constAbsent, constConst, rbB, rbMask, reader, rbRingBuffer);
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

                    genReadLongSignedCopyOptional(target, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongSignedIncrementOptional(target, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & instanceMask] == 0 ? constAbsent
                        : rLongDictionary[token & instanceMask];

                genReadLongSignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }

    }

    private void readLongSigned(int token, long[] rLongDictionary, int instanceMask, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & instanceMask;

                    genReadLongSignedNone(target, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedDelta(target, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & instanceMask];
                genReadLongSignedConstant(constDefault, rbB, rbMask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedCopy(target, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;

                    genReadLongSignedIncrement(target, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & instanceMask];

                genReadLongSignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }
    }

    private void readLongUnsignedOptional(int token, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedDeltaOptional(target, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constConst = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedConstantOptional(constAbsent, constConst, rbB, rbMask, reader, rbRingBuffer);
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

                    genReadLongUnsignedCopyOptional(target, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;
                    long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));

                    genReadLongUnsignedIncrementOptional(target, source, constAbsent, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constAbsent = TokenBuilder.absentValue64(TokenBuilder.extractAbsent(token));
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK] == 0 ? constAbsent
                        : rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }

    }

    private void readLongUnsigned(int token, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & MAX_LONG_INSTANCE_MASK;

                    genReadLongUnsignedNone(target, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedDelta(target, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];
                genReadLongConstant(constDefault, rbB, rbMask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedCopy(target, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_LONG_INSTANCE_MASK;
                    int source = readFromIdx > 0 ? readFromIdx & MAX_LONG_INSTANCE_MASK : target;

                    genReadLongUnsignedIncrement(target, source, rLongDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                long constDefault = rLongDictionary[token & MAX_LONG_INSTANCE_MASK];

                genReadLongUnsignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }

    }

    public int readInt(int token) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readIntegerUnsigned(token, readFromIdx);
            } else {
                readIntegerSigned(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readIntegerUnsignedOptional(token, readFromIdx);
            } else {
                readIntegerSignedOptional(token, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx);
            }
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peek(rbB, rbRingBuffer.addPos-1, rbMask);
    }

    private void readIntegerSignedOptional(int token, int[] rIntDictionary, int instanceMask, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & instanceMask];

                genReadIntegerSignedConstantOptional(constAbsent, constConst, rbB, rbMask, reader, rbRingBuffer);
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

                    genReadIntegerSignedCopyOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerSignedIncrementOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constDefault = rIntDictionary[token & instanceMask] == 0 ? constAbsent
                        : rIntDictionary[token & instanceMask];

                genReadIntegerSignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }

    }

    private void readIntegerSigned(int token, int[] rIntDictionary, int instanceMask, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int target = token & instanceMask;
                    genReadIntegerSignedNone(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer);
                } else {
                    // delta
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedDelta(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerConstant(constDefault, rbB, rbMask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & instanceMask;
                    int source = readFromIdx > 0 ? readFromIdx & instanceMask : target;
                    genReadIntegerSignedIncrement(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                int constDefault = rIntDictionary[token & instanceMask];
                genReadIntegerSignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }
    }

    private void readIntegerUnsignedOptional(int token, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    assert (readFromIdx < 0);
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedOptional(constAbsent, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedDeltaOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int constConst = rIntDictionary[token & MAX_INT_INSTANCE_MASK];

                genReadIntegerUnsignedConstantOptional(constAbsent, constConst, rbB, rbMask, reader, rbRingBuffer);
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

                    genReadIntegerUnsignedCopyOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));

                    genReadIntegerUnsignedIncrementOptional(target, source, constAbsent, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constAbsent = TokenBuilder.absentValue32(TokenBuilder.extractAbsent(token));
                int t = rIntDictionary[source];
                int constDefault = t == 0 ? constAbsent : t - 1;

                genReadIntegerUnsignedDefaultOptional(constAbsent, constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }

    }

    private void readIntegerUnsigned(int token, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadIntegerUnsigned(target, rbB, rbMask, reader, rIntDictionary, rbRingBuffer);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadIntegerUnsignedDelta(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadIntegerUnsignedConstant(constDefault, rbB, rbMask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadIntegerUnsignedCopy(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadIntegerUnsignedIncrement(target, source, rIntDictionary, rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadIntegerUnsignedDefault(constDefault, rbB, rbMask, reader, rbRingBuffer);
            }
        }
    }

    private void readLength(int token, int jumpToTarget, int readFromIdx) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadLength(target, jumpToTarget, rbB, rbMask, rbRingBuffer, rIntDictionary, reader, this);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadLengthDelta(target, source, jumpToTarget, rIntDictionary, rbB, rbMask, rbRingBuffer, reader, this);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadLengthConstant(constDefault, jumpToTarget, rbB, rbMask, rbRingBuffer, this);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthCopy(target, source, jumpToTarget, rIntDictionary, rbB, rbMask, rbRingBuffer, reader, this);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthIncrement(target, source, jumpToTarget, rIntDictionary, rbB, rbMask, rbRingBuffer, reader, this);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadLengthDefault(constDefault, jumpToTarget, rbB, reader, rbMask, rbRingBuffer, this);
            }
        }
    }
    
    public int readBytes(int token) {

        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        // System.out.println("reading "+TokenBuilder.tokenToString(token));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            readByteArray(token);
        } else {
            readByteArrayOptional(token);
        }
        
        //NOTE: for testing we need to check what was written
        int value = FASTRingBuffer.peek(rbB, rbRingBuffer.addPos-2, rbMask);
        //if the value is positive it no longer points to the textHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & MAX_TEXT_INSTANCE_MASK;
    }

    private void readByteArray(int token) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & byteInstanceMask;
                    genReadBytesNone(idx, rbB, rbMask, byteHeap, rbRingBuffer);
                } else {
                    // tail
                    int idx = token & byteInstanceMask;
                    genReadBytesTail(idx, rbB, rbMask, byteHeap, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int id =  token & byteInstanceMask;
                    int idLength = byteHeap.length(id);
                    genReadBytesConstant(id, idLength, rbB, rbMask, rbRingBuffer);
                } else {
                    // delta
                    int idx = token & byteInstanceMask;
                    genReadBytesDelta(idx, rbB, rbMask, byteHeap, rbRingBuffer);
                }
            }
        } else {
            int idx = token & byteInstanceMask;
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx, 0, rbB, rbMask, byteHeap, reader, rbRingBuffer);
            } else {
                // default
                int initId = FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK | idx;
                int idLength = byteHeap.length(initId);
                int initIdx = byteHeap.initStartOffset(initId);
                
                genReadBytesDefault(idx,initIdx, idLength, 0, rbB, rbMask, byteHeap, reader, rbRingBuffer);
            }
        }
    }



    private void readByteArrayOptional(int token) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            int idx = token & byteInstanceMask;
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNoneOptional(idx, rbB, rbMask, byteHeap, rbRingBuffer);
                } else {
                    // tail
                    genReadBytesTailOptional(idx, rbB, rbMask, byteHeap, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId =    idx | FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    int constInitLen = byteHeap.length(constId);
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    
                    int constValue =    idx;
                    int constValueLen = byteHeap.length(constValue);
                    
                    genReadBytesConstantOptional(constInit, constInitLen, constValue, constValueLen, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    genReadBytesDeltaOptional(idx, rbB, rbMask, byteHeap, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & byteInstanceMask;
                genReadBytesCopy(idx, 1, rbB, rbMask, byteHeap, reader, rbRingBuffer);
            } else {
                // default
                int constValue =    token & byteInstanceMask;
                int initId = FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK | constValue;
                
                int constValueLen = byteHeap.length(initId);
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                
                genReadBytesDefault(constValue,initIdx, constValueLen,1, rbB, rbMask, byteHeap, reader, rbRingBuffer);
            }
        }
    }



    public void openGroup(int token, int pmapSize) {

        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        if (pmapSize > 0) {
            PrimitiveReader.openPMap(pmapSize, reader);
        }
    }

    public void closeGroup(int token, int backvalue) {

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


    
    public int readDecimalExponent(int token) {
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
            readIntegerSigned(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx);
        } else {
            // 00011 IntegerSignedOptional
            readIntegerSignedOptional(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK, readFromIdx);
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peek(rbB, rbRingBuffer.addPos-1, rbMask);
    }

    public long readDecimalMantissa(int token) {
        assert (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE))) : TokenBuilder.tokenToString(token);

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            // not optional
            readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx);
        } else {
            // optional
            readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK, readFromIdx);
        }
        
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peekLong(rbB, rbRingBuffer.addPos-2, rbMask);
    };

    public int readText(int token) {
        assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii
                readTextASCII(token);
            } else {
                // utf8
                readTextUTF8(token);
            }
        } else {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii optional
                readTextASCIIOptional(token);
            } else {
                // utf8 optional
                readTextUTF8Optional(token);
            }
        }
        
        //NOTE: for testing we need to check what was written
        int value = FASTRingBuffer.peek(rbB, rbRingBuffer.addPos-2, rbMask);
        //if the value is positive it no longer points to the textHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & MAX_TEXT_INSTANCE_MASK;
    }

    private void readTextUTF8Optional(int token) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadUTF8None(idx,1, rbB, rbMask, textHeap, reader, rbRingBuffer);
                } else {
                    // tail
                    genReadUTF8TailOptional(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = (idx) | FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    
                    int constValue = idx;
                    genReadTextConstantOptional(constInit, constValue, textHeap.initLength(constId), textHeap.initLength(constValue), rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    genReadUTF8DeltaOptional(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadUTF8Copy(idx,1, rbB, rbMask, reader, textHeap, rbRingBuffer);
            } else {
                // default
                int initId = FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                
                genReadUTF8Default(idx, initIdx, textHeap.initLength(initId), 1, rbB, rbMask, textHeap, reader, rbRingBuffer);
            }
        }
    }

    private void readTextASCII(int token) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadASCIINone(idx, rbB, rbMask, reader, textHeap, rbRingBuffer);//always dynamic
                } else {
                    // tail
                    int fromIdx = readFromIdx;
                    genReadASCIITail(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);//always dynamic
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    
                    genReadTextConstant(constInit, textHeap.initLength(constId), rbB, rbMask, rbRingBuffer); //always fixed length
                } else {
                    // delta
                    genReadASCIIDelta(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);//always dynamic
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadASCIICopy(idx, rbB, rbMask, reader, textHeap, rbRingBuffer); //always dynamic
            } else {
                // default
                int initId = FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK|idx;
                int initIdx = textHeap.initStartOffset(initId) |FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                genReadASCIIDefault(idx, initIdx, textHeap.initLength(initId), rbB, rbMask, reader, textHeap, rbRingBuffer); //dynamic or constant
            }
        }
    }

    private void readTextUTF8(int token) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadUTF8None(idx,0, rbB, rbMask, textHeap, reader, rbRingBuffer);
                } else {
                    // tail
                    genReadUTF8Tail(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    
                    genReadTextConstant(constInit, textHeap.initLength(constId), rbB, rbMask, rbRingBuffer);
                } else {
                    // delta
                    genReadUTF8Delta(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadUTF8Copy(idx,0, rbB, rbMask, reader, textHeap, rbRingBuffer);
            } else {
                // default
                int initId = FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                
                genReadUTF8Default(idx, initIdx, textHeap.initLength(initId), 0, rbB, rbMask, textHeap, reader, rbRingBuffer);
            }
        }
    }

    private void readTextASCIIOptional(int token) {
        int idx = token & MAX_TEXT_INSTANCE_MASK;
        if (0 == (token & ((4 | 2 | 1) << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                // none
                genReadASCIINone(idx, rbB, rbMask, reader, textHeap, rbRingBuffer);
            } else {
                // tail
                genReadASCIITailOptional(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIIDeltaOptional(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                } else {
                    int constId = (token & MAX_TEXT_INSTANCE_MASK) | FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    
                    //TODO: redo text to avoid copy and have usage counter in text heap.
                    int constValue = token & MAX_TEXT_INSTANCE_MASK; //TODO: A, is this real? how do we know where to copy from ?
                    genReadTextConstantOptional(constInit, constValue, textHeap.initLength(constId), textHeap.initLength(constValue), rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIICopyOptional(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                } else {
                    // for ASCII we don't need special behavior for optional
                    int initId = FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK|idx;
                    int initIdx = textHeap.initStartOffset(initId) |FASTReaderScriptPlayerDispatch.INIT_VALUE_MASK;
                    genReadASCIIDefault(idx, initIdx, textHeap.initLength(initId), rbB, rbMask, reader, textHeap, rbRingBuffer);
                }
            }
        }
    }

}
