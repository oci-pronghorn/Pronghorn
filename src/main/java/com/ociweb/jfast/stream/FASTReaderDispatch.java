//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;

//May drop interface if this causes a performance problem from virtual table
public class FASTReaderDispatch {

    final PrimitiveReader reader;
    protected final TextHeap textHeap;
    protected final ByteHeap byteHeap;

    private int readFromIdx = -1;

    // This is the GLOBAL dictionary
    // When unspecified in the template GLOBAL is the default so these are used.
    protected final int MAX_INT_INSTANCE_MASK;
    protected final int[] rIntDictionary;
    protected final int[] rIntInit;

    protected final int MAX_LONG_INSTANCE_MASK;
    protected final long[] rLongDictionary;
    protected final long[] rLongInit;

    protected final int MAX_TEXT_INSTANCE_MASK;
    protected final FieldReaderBytes readerBytes;

    protected final int nonTemplatePMapSize;
    protected final int[][] dictionaryMembers;

    protected final DictionaryFactory dictionaryFactory;

    protected DispatchObserver observer;

    // constant fields are always the same or missing but never anything else.
    // manditory constant does not use pmap and has constant injected at
    // destnation never xmit
    // optional constant does use the pmap 1 (use initial const) 0 (not present)
    //
    // default fields can be the default or overridden this one time with a new
    // value.

    protected final int maxNestedSeqDepth;
    protected final int[] sequenceCountStack;

    int sequenceCountStackHead = -1;
    boolean doSequence; //NOTE: return value from closeGroup
    int jumpSequence; // Only needs to be set when returning true.



    int activeScriptCursor;
    int activeScriptLimit;

    int[] fullScript;

    protected final FASTRingBuffer rbRingBuffer;
    protected final int rbMask;
    protected final int[] rbB;
    public static final int INIT_VALUE_MASK = 0x80000000;

    public FASTReaderDispatch(PrimitiveReader reader, DictionaryFactory dcr, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript,
            int maxNestedGroupDepth, int primaryRingBits, int textRingBits) {
        this.reader = reader;
        this.dictionaryFactory = dcr;
        this.nonTemplatePMapSize = nonTemplatePMapSize;
        this.dictionaryMembers = dictionaryMembers;

        this.textHeap = dcr.charDictionary(maxTextLen, charGap);
        this.byteHeap = dcr.byteDictionary(maxVectorLen, bytesGap);

        this.maxNestedSeqDepth = maxNestedGroupDepth;
        this.sequenceCountStack = new int[maxNestedSeqDepth];

        this.fullScript = fullScript;

        this.rIntDictionary = dcr.integerDictionary();
        this.rIntInit = dcr.integerDictionary();
        assert (rIntDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rIntDictionary.length));
        assert (rIntDictionary.length == rIntInit.length);
        this.MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length - 1));

        this.rLongDictionary = dcr.longDictionary();
        this.rLongInit = dcr.longDictionary();
        assert (rLongDictionary.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(rLongDictionary.length));
        assert (rLongDictionary.length == rLongInit.length);

        this.MAX_LONG_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rLongDictionary.length - 1));

        assert(null==textHeap || textHeap.itemCount()<TokenBuilder.MAX_INSTANCE);
        assert(null==textHeap || TokenBuilder.isPowerOfTwo(textHeap.itemCount()));
        this.MAX_TEXT_INSTANCE_MASK = (null==textHeap)?TokenBuilder.MAX_INSTANCE:Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount()-1));

        this.readerBytes = new FieldReaderBytes(reader, byteHeap);

        this.rbRingBuffer = new FASTRingBuffer((byte) primaryRingBits,
                                               (byte) textRingBits, 
                                                null==textHeap? null : textHeap.rawInitAccess(),
                                                null==byteHeap? null : byteHeap.rawInitAccess()
                                                );
        rbMask = rbRingBuffer.mask;
        rbB = rbRingBuffer.buffer;
        
    }

    public FASTRingBuffer ringBuffer() {
        return rbRingBuffer;
    }

    public void reset() {

        // clear all previous values to un-set
        dictionaryFactory.reset(rIntDictionary);
        dictionaryFactory.reset(rLongDictionary);
        if (null != textHeap) {
            textHeap.reset();
        }
        if (null!=byteHeap) {
            byteHeap.reset();
        }
        sequenceCountStackHead = -1;

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
                } else {
                    // 01???
                    if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                        dispatchReadByTokenForText(token);
                    } else {
                        // 011??
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            // 0110? Decimal and DecimalOptional
                            readDecimalExponent(token);
                            
                            //TODO A, Pass the optional bit flag on to Mantissa for var bit optionals
                            
                            token = fullScript[++activeScriptCursor]; //pull second token
                            readDecimalMantissa(token);
                        } else {
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
                        readLength(token,activeScriptCursor + (TokenBuilder.MAX_INSTANCE & fullScript[1+activeScriptCursor]) + 1);

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

    //TODO: generator must track previous read from for text etc
    //TODO: generator must track if previous is not used then do not write to dictionary.
    
    
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

    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }

    protected boolean gatherReadData(PrimitiveReader reader, int cursor) {

        int token = fullScript[cursor];

        if (null != observer) {
            String value = "";
            // totalRead is bytes loaded from stream.

            long absPos = reader.totalRead() - reader.bytesReadyToParse();
            observer.tokenItem(absPos, token, cursor, value);
        }

        return true;
    }

    protected boolean gatherReadData(PrimitiveReader reader, int cursor, String value) {

        int token = fullScript[cursor];

        if (null != observer) {
            // totalRead is bytes loaded from stream.

            long absPos = reader.totalRead() - reader.bytesReadyToParse();
            observer.tokenItem(absPos, token, cursor, value);
        }

        return true;
    }

    protected boolean gatherReadData(PrimitiveReader reader, String msg) {

        if (null != observer) {
            long absPos = reader.totalRead() - reader.bytesReadyToParse();
            observer.tokenItem(absPos, -1, activeScriptCursor, msg);
        }

        return true;
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
                            genReadDictionaryBytesReset(idx, readerBytes);
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
                readLongUnsigned(token);
            } else {
                // 00101 LongUnsignedOptional
                readLongUnsignedOptional(token);
            }
        } else {
            // 0011?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00110 LongSigned
                readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK);
            } else {
                // 00111 LongSignedOptional
                readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK);
            }
        }
    }

    private void dispatchReadByTokenForInteger(int token) {
        // 000??
        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
            // 0000?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00000 IntegerUnsigned
                readIntegerUnsigned(token);
            } else {
                // 00001 IntegerUnsignedOptional
                readIntegerUnsignedOptional(token);
            }
        } else {
            // 0001?
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                // 00010 IntegerSigned
                readIntegerSigned(token, rIntDictionary, MAX_INT_INSTANCE_MASK);
            } else {
                // 00011 IntegerSignedOptional
                readIntegerSignedOptional(token, rIntDictionary, MAX_INT_INSTANCE_MASK);
            }
        }
    }

    public long readLong(int token) {

        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readLongUnsigned(token);
            } else {
                readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readLongUnsignedOptional(token);
            } else {
                readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK);
            }
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peekLong(rbB, rbRingBuffer.addPos-2, rbMask);
    }

    private void readLongSignedOptional(int token, long[] rLongDictionary, int instanceMask) {

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

    private void readLongSigned(int token, long[] rLongDictionary, int instanceMask) {

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

    private void readLongUnsignedOptional(int token) {

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

    private void readLongUnsigned(int token) {

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
                readIntegerUnsigned(token);
            } else {
                readIntegerSigned(token, rIntDictionary, MAX_INT_INSTANCE_MASK);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                readIntegerUnsignedOptional(token);
            } else {
                readIntegerSignedOptional(token, rIntDictionary, MAX_INT_INSTANCE_MASK);
            }
        }
        //NOTE: for testing we need to check what was written
        return FASTRingBuffer.peek(rbB, rbRingBuffer.addPos-1, rbMask);
    }

    private void readIntegerSignedOptional(int token, int[] rIntDictionary, int instanceMask) {

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

    private void readIntegerSigned(int token, int[] rIntDictionary, int instanceMask) {

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

    private void readIntegerUnsignedOptional(int token) {

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

    private void readIntegerUnsigned(int token) {

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

    private void readLength(int token, int jumpToTarget) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                int target = token & MAX_INT_INSTANCE_MASK;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadLength(target, jumpToTarget, rbB, rbMask, rbRingBuffer, rIntDictionary, reader);
                } else {
                    // delta
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                    genReadLengthDelta(target, source, jumpToTarget, rIntDictionary, rbB, rbMask, rbRingBuffer, reader);
                }
            } else {
                // constant
                // always return this required value.
                int constDefault = rIntDictionary[token & MAX_INT_INSTANCE_MASK];
                genReadLengthConstant(constDefault, jumpToTarget, rbB, rbMask, rbRingBuffer);
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthCopy(target, source, jumpToTarget, rIntDictionary, rbB, rbMask, rbRingBuffer, reader);
                } else {
                    // increment
                    int target = token & MAX_INT_INSTANCE_MASK;
                    int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;

                    genReadLengthIncrement(target, source, jumpToTarget, rIntDictionary, rbB, rbMask, rbRingBuffer, reader);
                }
            } else {
                // default
                int target = token & MAX_INT_INSTANCE_MASK;
                int source = readFromIdx >= 0 ? readFromIdx & MAX_INT_INSTANCE_MASK : target;
                int constDefault = rIntDictionary[source];

                genReadLengthDefault(constDefault, jumpToTarget, rbB, reader, rbMask, rbRingBuffer);
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
                    int idx = token & readerBytes.INSTANCE_MASK;
                    genReadBytesNone(idx, rbB, rbMask, byteHeap, readerBytes, rbRingBuffer);
                } else {
                    // tail
                    int idx = token & readerBytes.INSTANCE_MASK;
                    genReadBytesTail(idx, rbB, rbMask, byteHeap, readerBytes, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int id =  token & readerBytes.INSTANCE_MASK;
                    int idLength = byteHeap.length(id);
                    genReadBytesConstant(id, idLength, rbB, rbMask, rbRingBuffer);
                } else {
                    // delta
                    int idx = token & readerBytes.INSTANCE_MASK;
                    genReadBytesDelta(idx, rbB, rbMask, byteHeap, readerBytes, rbRingBuffer);
                }
            }
        } else {
            int idx = token & readerBytes.INSTANCE_MASK;
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genReadBytesCopy(idx, 0, rbB, rbMask, byteHeap, reader, readerBytes, rbRingBuffer);
            } else {
                // default
                int initId = FASTReaderDispatch.INIT_VALUE_MASK | idx;
                int idLength = byteHeap.length(initId);
                int initIdx = byteHeap.initStartOffset(initId);
                
                genReadBytesDefault(idx,initIdx, idLength, 0, rbB, rbMask, byteHeap, reader, readerBytes, rbRingBuffer);
            }
        }
    }



    private void readByteArrayOptional(int token) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            int idx = token & readerBytes.INSTANCE_MASK;
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genReadBytesNoneOptional(idx, rbB, rbMask, byteHeap, readerBytes, rbRingBuffer);
                } else {
                    // tail
                    genReadBytesTailOptional(idx, rbB, rbMask, byteHeap, readerBytes, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId =    idx | FASTReaderDispatch.INIT_VALUE_MASK;
                    int constInitLen = byteHeap.length(constId);
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderDispatch.INIT_VALUE_MASK;
                    
                    int constValue =    idx;
                    int constValueLen = byteHeap.length(constValue);
                    
                    genReadBytesConstantOptional(constInit, constInitLen, constValue, constValueLen, rbB, rbMask, reader, rbRingBuffer);
                } else {
                    // delta
                    genReadBytesDeltaOptional(idx, rbB, rbMask, byteHeap, readerBytes, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & readerBytes.INSTANCE_MASK;
                genReadBytesCopy(idx, 1, rbB, rbMask, byteHeap, reader, readerBytes, rbRingBuffer);
            } else {
                // default
                int constValue =    token & readerBytes.INSTANCE_MASK;
                int initId = FASTReaderDispatch.INIT_VALUE_MASK | constValue;
                
                int constValueLen = byteHeap.length(initId);
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderDispatch.INIT_VALUE_MASK;
                
                genReadBytesDefault(constValue,initIdx, constValueLen,1, rbB, rbMask, byteHeap, reader, readerBytes, rbRingBuffer);
            }
        }
    }



    public void openGroup(int token, int pmapSize) {

        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        if (pmapSize > 0) {
            reader.openPMap(pmapSize);
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
            genReadSequenceClose(backvalue);
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
            readIntegerSigned(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK);
        } else {
            // 00011 IntegerSignedOptional
            readIntegerSignedOptional(expoToken, rIntDictionary, MAX_INT_INSTANCE_MASK);
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
            readLongSigned(token, rLongDictionary, MAX_LONG_INSTANCE_MASK);
        } else {
            // optional
            readLongSignedOptional(token, rLongDictionary, MAX_LONG_INSTANCE_MASK);
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
                    int constId = (idx) | FASTReaderDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderDispatch.INIT_VALUE_MASK;
                    
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
                int initId = FASTReaderDispatch.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderDispatch.INIT_VALUE_MASK;
                
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
                    genReadASCIITail(idx, fromIdx, rbB, rbMask, textHeap, reader, rbRingBuffer);//always dynamic
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    int constId = idx | FASTReaderDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderDispatch.INIT_VALUE_MASK;
                    
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
                int initId = FASTReaderDispatch.INIT_VALUE_MASK|idx;
                int initIdx = textHeap.initStartOffset(initId) |FASTReaderDispatch.INIT_VALUE_MASK;
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
                    int constId = idx | FASTReaderDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderDispatch.INIT_VALUE_MASK;
                    
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
                int initId = FASTReaderDispatch.INIT_VALUE_MASK | idx;
                int initIdx = textHeap.initStartOffset(initId)|FASTReaderDispatch.INIT_VALUE_MASK;
                
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
                    genReadASCIIDeltaOptional(readFromIdx, idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                } else {
                    int constId = (token & MAX_TEXT_INSTANCE_MASK) | FASTReaderDispatch.INIT_VALUE_MASK;
                    int constInit = textHeap.initStartOffset(constId)| FASTReaderDispatch.INIT_VALUE_MASK;
                    
                    int constValue = token & MAX_TEXT_INSTANCE_MASK; //TODO: A, is this real? how do we know where to copy from ?
                    genReadTextConstantOptional(constInit, constValue, textHeap.initLength(constId), textHeap.initLength(constValue), rbB, rbMask, reader, rbRingBuffer);
                }
            } else {
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    genReadASCIICopyOptional(idx, rbB, rbMask, textHeap, reader, rbRingBuffer);
                } else {
                    // for ASCII we don't need special behavior for optional
                    int initId = FASTReaderDispatch.INIT_VALUE_MASK|idx;
                    int initIdx = textHeap.initStartOffset(initId) |FASTReaderDispatch.INIT_VALUE_MASK;
                    genReadASCIIDefault(idx, initIdx, textHeap.initLength(initId), rbB, rbMask, reader, textHeap, rbRingBuffer);
                }
            }
        }
    }

    //////////////////////////////////
    //Code above this point is copied for generator
    //////////////////////////////////
    
    // ////////////////////////////////////////////////////////////
    // DO NOT REMOVE/MODIFY CONSTANT
    public static final String START_HERE = "Code Generator Scripts Start Here";

    
    // ////////////////////////////////////////////////////////////


    protected void genReadSequenceClose(int backvalue) {
        if (sequenceCountStackHead <= 0) {
            // no sequence to worry about or not the right time
            doSequence = false;
        } else {
        
            // each sequence will need to repeat the pmap but we only need to push
            // and pop the stack when the sequence is first encountered.
            // if count is zero we can pop it off but not until then.
        
            if (--sequenceCountStack[sequenceCountStackHead] < 1) {
                // this group is a sequence so pop it off the stack.
                --sequenceCountStackHead;
                // finished this sequence so leave pointer where it is
                jumpSequence = 0;
            } else {
                // do this sequence again so move pointer back
                jumpSequence = backvalue;
            }
            doSequence = true;
        }
    }
    
    protected void genReadGroupPMapOpen(int nonTemplatePMapSize, PrimitiveReader reader) {
        reader.openPMap(nonTemplatePMapSize);
    }

    protected void genReadGroupClose(PrimitiveReader reader) {
        reader.closePMap();
    }
    
    
    //length methods
    protected boolean genReadLengthDefault(int constDefault,  int jumpToTarget, int[] rbB, PrimitiveReader reader, int rbMask, FASTRingBuffer rbRingBuffer) {
        
        int length;
        int value = length = reader.readIntegerUnsignedDefault(constDefault);
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = length;
            return false;
       }
    }

    //TODO: C, once this all works find a better way to inline it with only 1 conditional.
    protected boolean genReadLengthIncrement(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int length;
        int value = length = reader.readIntegerUnsignedIncrement(target, source, rIntDictionary);
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = length;
            return false;
       }
    }

    protected boolean genReadLengthCopy(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int length;
        int value = length = reader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = length;
            return false;
       }
    }

    protected boolean genReadLengthConstant(int constDefault,  int jumpToTarget, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
        if (constDefault == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = constDefault;
            return false;
       }
    }

    protected boolean genReadLengthDelta(int target, int source,  int jumpToTarget, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int length;
        int value = length = (rIntDictionary[target] = (int) (rIntDictionary[source] + reader.readLongSigned()));
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = length;
            return false;
       }
    }

    protected boolean genReadLength(int target,  int jumpToTarget, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, int[] rIntDictionary, PrimitiveReader reader) {
        int length;
        int value = rIntDictionary[target] = length = reader.readIntegerUnsigned();
        rbB[rbMask & rbRingBuffer.addPos++] = value;
        if (length == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            activeScriptCursor = jumpToTarget;
            return true;
        } else {
            // jumpSequence = 0;
            sequenceCountStack[++sequenceCountStackHead] = length;
            return false;
       }
    }
    
    //TODO: A, needs support for messageRef where we can inject template in another and return to the previouslocation. Needs STACK in dispatch!
    //TODO: Z, can we send catalog in-band as a byteArray to push dynamic changes,  Need a unit test for this.
    //TODO: A, for code generation is it better to have shared block? or independent blocks?
    //TODO: B, set the default template for the case when it is undefined in catalog.
    //TODO: C, Must add unit test for message length field start-of-frame testing, FrameLength bytes to read before decoding, is before pmap/templateId
    //TODO: D, perhaps frame support is related to buffer size in primtive write so the right number of bits can be set.
    
    
    // int methods

    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefaultOptional(constDefault, constAbsent, reader);
    }

    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedIncrementOptional(target, source, rIntDictionary, constAbsent);
    }

    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int xi1;
        rbB[rbMask & rbRingBuffer.addPos++] = (0 == (xi1 = reader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader)) ? constAbsent : xi1 - 1);
    }

    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedConstantOptional(constAbsent, constConst);
    }

    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        // Delta opp never uses PMAP
        long value = reader.readLongSigned();
        int result;
        if (0 == value) {
            rIntDictionary[target] = 0;// set to absent
            result = constAbsent;
        } else {
            result = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
        
        }
        rbB[rbMask & rbRingBuffer.addPos++] = result;
    }

    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int xi1;
        rbB[rbMask & rbRingBuffer.addPos++] = 0 == (xi1 = reader.readIntegerUnsigned()) ? constAbsent : xi1 - 1;
    }

    protected void genReadIntegerUnsignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedDefault(constDefault);
    }

    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedIncrement(target, source, rIntDictionary);
    }

    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
    }

    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = (int) (rIntDictionary[source] + reader.readLongSigned()));
    }

    protected void genReadIntegerUnsigned(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = reader.readIntegerUnsigned();
    }

    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDefault(constDefault);
    }

    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedIncrement(target, source, rIntDictionary);
    }

    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedCopy(target, source, rIntDictionary);
    }

    protected void genReadIntegerConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
    }

    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDelta(target, source, rIntDictionary);
    }

    protected void genReadIntegerSignedNone(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = reader.readIntegerSigned();
    }

    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDefaultOptional(constDefault, constAbsent);
    }

    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedIncrementOptional(target, source, rIntDictionary, constAbsent);
    }

    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int xi1;
        rbB[rbMask & rbRingBuffer.addPos++] = (0 == (xi1 = reader.readIntegerSignedCopy(target, source, rIntDictionary)) ? constAbsent : (xi1 > 0 ? xi1 - 1 : xi1));
    }

    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedConstantOptional(constAbsent, constConst);
    }

    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedDeltaOptional(target, source, rIntDictionary, constAbsent);
    }

    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = reader.readIntegerSignedOptional(constAbsent);
    }

    // long methods

    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongUnsignedDefault(constDefault);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongUnsignedIncrement(idx, source, rLongDictionary);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongUnsignedCopy(idx, source, rLongDictionary);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        long tmpLng=constDefault;
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + reader.readLongSigned()));
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=rLongDictionary[idx] = reader.readLongUnsigned();
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongUnsignedDefaultOptional(constDefault, constAbsent);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongUnsignedIncrementOptional(idx, source, rLongDictionary, constAbsent);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long xl1;
        long tmpLng=(0 == (xl1 = reader.readLongUnsignedCopy(idx, source, rLongDictionary)) ? constAbsent : xl1 - 1);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            //TODO: X, rewrite so long values are pre shifted and masked for these constants.
        long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongUnsignedDeltaOptional(idx, source, rLongDictionary, constAbsent);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long value = reader.readLongUnsigned();
        long tmpLng=value == 0 ? constAbsent : value - 1;
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongSignedDefault(constDefault);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongSignedIncrement(idx, source, rLongDictionary);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongSignedCopy(idx, source, rLongDictionary);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        long tmpLng=constDefault;
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + reader.readLongSigned()));
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=rLongDictionary[idx] = reader.readLongSigned();
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongSignedDefaultOptional(constDefault, constAbsent);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) { //TODO: anything at the end is ignored and can be injected values.
        long tmpLng=reader.readLongSignedIncrementOptional(idx, source, rLongDictionary, constAbsent);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long xl1;
        long tmpLng=(0 == (xl1 = reader.readLongSignedCopy(idx, source, rLongDictionary)) ? constAbsent : xl1 - 1);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            //TODO: A, if long const and default values are sent as int tuples then the shift mask logic can be skipped!!!
        long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=reader.readLongSignedDeltaOptional(idx, source, rLongDictionary, constAbsent);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long value = reader.readLongSigned();
        long tmpLng=value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    // text methods.

    
    protected void genReadUTF8None(int idx, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, reader.readIntegerUnsigned() - optOff);
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8TailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = reader.readIntegerUnsigned();
        if (trim == 0) {
            textHeap.setNull(idx);
        } else {
            int utfLength = reader.readIntegerUnsigned();
            int t = trim - 1;
            StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, t);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8DeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = reader.readIntegerSigned();
        if (0 == trim) {
            textHeap.setNull(idx);
        } else {
            StaticGlue.allocateAndDeltaUTF8(idx, textHeap, reader, trim>0?trim-1:trim);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadUTF8Delta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.allocateAndDeltaUTF8(idx, textHeap, reader, reader.readIntegerSigned());
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIITail(int idx, int fromIdx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.readASCIITail(idx, textHeap, reader, reader.readIntegerUnsigned());
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constIdx;
        rbB[rbMask & rbRingBuffer.addPos++] = constLen;
    }

    protected void genReadASCIIDelta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = reader.readIntegerSigned();
        if (trim >=0) {
            StaticGlue.readASCIITail(idx, textHeap, reader, trim); 
        } else {
            StaticGlue.readASCIIHead(idx, trim, readFromIdx, textHeap, reader);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadASCIICopy(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
         int len;
        if (PrimitiveReader.popPMapBit(reader)!=0) {
            byte val;
            int tmp;
            if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                len=StaticGlue.readASCIIToHeapValue(val, tmp, idx, textHeap, reader);
            } else {
                len=StaticGlue.readASCIIToHeapNone(idx, val, textHeap, reader);
            }
        } else {
            len = textHeap.valueLength(idx);
        }
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIICopyOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int len;
       if (PrimitiveReader.popPMapBit(reader)!=0) {
           byte val;
           int tmp;
           if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
               len=StaticGlue.readASCIIToHeapValue(val, tmp, idx, textHeap, reader);
           } else {
               len=StaticGlue.readASCIIToHeapNone(idx, val, textHeap, reader);
           }
       } else {
           len = textHeap.valueLength(idx);
       }
       rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
       rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8Tail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = reader.readIntegerSigned();
        int utfLength = reader.readIntegerUnsigned();
        
        StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, trim);
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }


    protected void genReadUTF8Copy(int idx, int optOff, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, reader.readIntegerUnsigned() - optOff);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8Default(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            rbB[rbMask & rbRingBuffer.addPos++] = defIdx;
            rbB[rbMask & rbRingBuffer.addPos++] = defLen;
        } else {
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, reader.readIntegerUnsigned() - optOff);
            int len = textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        }
    }

    protected void genReadASCIINone(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        byte val;
        int tmp;
        if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
            tmp=StaticGlue.readASCIIToHeapValue(val, tmp, idx, textHeap, reader);
        } else {
            tmp=StaticGlue.readASCIIToHeapNone(idx, val, textHeap, reader);
        }
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, tmp, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = tmp;
    }

    protected void genReadASCIITailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int tail = reader.readIntegerUnsigned();
        if (0 == tail) {
            textHeap.setNull(idx);
        } else {
           StaticGlue.readASCIITail(idx, textHeap, reader, tail-1);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIIDeltaOptional(int fromIdx, int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        //TODO: B, extract the constant length from here.
        int optionalTrim = reader.readIntegerSigned();
        int tempId = (0 == optionalTrim ? 
                         textHeap.initStartOffset( FASTReaderDispatch.INIT_VALUE_MASK | idx) |FASTReaderDispatch.INIT_VALUE_MASK : 
                         (optionalTrim > 0 ? StaticGlue.readASCIITail(idx, textHeap, reader, optionalTrim - 1) :
                                             StaticGlue.readASCIIHead(idx, optionalTrim, fromIdx, textHeap, reader)));
        int len = textHeap.valueLength(tempId);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(tempId, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0 != PrimitiveReader.popPMapBit(reader) ) {
            rbB[rbMask & rbRingBuffer.addPos++] = constInit;
            rbB[rbMask & rbRingBuffer.addPos++] = constInitLen;
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = constValue;
            rbB[rbMask & rbRingBuffer.addPos++] = constValueLen;
        }
    }



    protected void genReadASCIIDefault(int idx, int defIdx, int defLen, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            rbB[rbMask & rbRingBuffer.addPos++] = defIdx;
            rbB[rbMask & rbRingBuffer.addPos++] = defLen;
        } else {
            byte val;
            int tmp;
            if (0 != (tmp = 0x7F & (val = reader.readTextASCIIByte()))) {
                tmp=StaticGlue.readASCIIToHeapValue(val, tmp, idx, textHeap, reader);
            } else {
                tmp=StaticGlue.readASCIIToHeapNone(idx, val, textHeap, reader);
            }
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, tmp, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = tmp;
        }
    }
    
    //byte methods
    
    protected void genReadBytesConstant(int constIdx, int constLen, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constIdx;
        rbB[rbMask & rbRingBuffer.addPos++] = constLen;
    }

    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        
        if (0 != PrimitiveReader.popPMapBit(reader) ) {
            rbB[rbMask & rbRingBuffer.addPos++] = constInit;
            rbB[rbMask & rbRingBuffer.addPos++] = constInitLen;
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = constValue;
            rbB[rbMask & rbRingBuffer.addPos++] = constValueLen;
        }
    }

    protected void genReadBytesDefault(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            rbB[rbMask & rbRingBuffer.addPos++] = defIdx;
            rbB[rbMask & rbRingBuffer.addPos++] = defLen;
        } else {
            readerBytes.readBytesData(idx, optOff);
            int len = byteHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        }
    }

    protected void genReadBytesCopy(int idx, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            readerBytes.readBytesData(idx,optOff);
        }
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesDeltaOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        readerBytes.readBytesDeltaOptional2(idx);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesTailOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        readerBytes.readBytesTailOptional2(idx);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;

    }

    protected void genReadBytesDelta(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        readerBytes.readBytesDelta2(idx);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    
    protected void genReadBytesTail(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        readerBytes.readBytesTail2(idx);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    
    protected void genReadBytesNoneOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        readerBytes.readBytesData(idx, 1);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesNone(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FieldReaderBytes readerBytes, FASTRingBuffer rbRingBuffer) {
        readerBytes.readBytesData(idx,0);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    // dictionary reset

    protected void genReadDictionaryBytesReset(int idx, FieldReaderBytes readerBytes) {
        readerBytes.reset(idx);
    }

    protected void genReadDictionaryTextReset(int idx, TextHeap textHeap) {
        textHeap.reset(idx);
    }

    protected void genReadDictionaryLongReset(int idx, long[] rLongDictionary, long[] rLongInit) {
        rLongDictionary[idx] = rLongInit[idx];
    }

    protected void genReadDictionaryIntegerReset(int idx, int[] rIntDictionary, int[] rIntInit) {
        rIntDictionary[idx] = rIntInit[idx];
    }

    //TODO: C, Need a way to stream to disk over gaps of time. Write FAST to a file and Write series of Dictionaries to another, this set is valid for 1 catalog.
    
    
}
