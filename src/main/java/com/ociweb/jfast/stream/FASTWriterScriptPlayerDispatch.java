//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public final class FASTWriterScriptPlayerDispatch { //TODO: B, should this extend a class with the gens then super. can be used. with writer above that.

    private int templateStackHead = 0;
    private final int[] templateStack;

    private final PrimitiveWriter writer;
    private final int instanceBytesMask;
    
    public final int[] intValues;
    private final int[] intInit;
    public final int intInstanceMask;
    
    public final long[] longValues;
    private final long[] longInit;
    public final int longInstanceMask;
    
    final int nonTemplatePMapSize;

    private int readFromIdx = -1; //TODO: A, Add WiterDispatch support for reading values from previous dictionary location.

    private final DictionaryFactory dictionaryFactory;
    private final FASTRingBuffer queue;
    private final int[][] dictionaryMembers;

    private final int[] sequenceCountStack;
    private int sequenceCountStackHead = -1;
    private boolean isFirstSequenceItem = false;
    private boolean isSkippedSequence = false;
    private DispatchObserver observer;
    int activeScriptCursor;
    int activeScriptLimit;
    final int[] fullScript;
    
    private TextHeap textHeap;
    private ByteHeap byteHeap;

    private RingCharSequence ringCharSequence = new RingCharSequence();
    public static final int INIT_VALUE_MASK = 0x80000000;
    public final int TEXT_INSTANCE_MASK;

    public FASTWriterScriptPlayerDispatch(PrimitiveWriter writer, DictionaryFactory dcr, int maxTemplates, int maxCharSize,
            int maxBytesSize, int gapChars, int gapBytes, FASTRingBuffer queue, int nonTemplatePMapSize,
            int[][] dictionaryMembers, int[] fullScript, int maxNestedGroupDepth) {

        this.fullScript = fullScript;
        this.writer = writer;
        this.dictionaryFactory = dcr;
        this.nonTemplatePMapSize = nonTemplatePMapSize;

        this.sequenceCountStack = new int[maxNestedGroupDepth];

        this.intValues = dcr.integerDictionary();
        this.intInit = dcr.integerDictionary();
        assert (intValues.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(intValues.length));
        this.intInstanceMask = Math.min(TokenBuilder.MAX_INSTANCE, (intValues.length - 1));
        
        this.longValues = dcr.longDictionary();
        this.longInit = dcr.longDictionary();
        assert (longValues.length < TokenBuilder.MAX_INSTANCE);
        assert (TokenBuilder.isPowerOfTwo(longValues.length));
        this.longInstanceMask = Math.min(TokenBuilder.MAX_INSTANCE, (longValues.length - 1));
        
        this.textHeap = dcr.charDictionary(maxCharSize, gapChars);
        this.byteHeap = dcr.byteDictionary(maxBytesSize, gapBytes);

        this.TEXT_INSTANCE_MASK = null == textHeap ? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (textHeap.itemCount() - 1));
        this.instanceBytesMask = null==byteHeap? 0 : Math.min(TokenBuilder.MAX_INSTANCE, (byteHeap.itemCount()-1));

        this.templateStack = new int[maxTemplates];
        this.queue = queue;
        this.dictionaryMembers = dictionaryMembers;
    }

    public void setDispatchObserver(DispatchObserver observer) {
        this.observer = observer;
    }

    /**
     * Write null value, must only be used if the field id is one of optional
     * type.
     */
    public void write(int token) {

        // only optional field types can use this method.
        assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))); 
       // TODO: T, in testing assert(failOnBadArg())

        // select on type, each dictionary will need to remember the null was
        // written
        if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
            // int long
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // int
                int idx = token & intInstanceMask;
                
                FASTWriterScriptPlayerDispatch.writeNullInt(token, writer, intValues, idx);
            } else {
                // long
                int idx = token & longInstanceMask;
                
                FASTWriterScriptPlayerDispatch.writeNullLong(token, idx, writer, longValues);
            }
        } else {
            // text decimal bytes
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // text
                int idx = token & TEXT_INSTANCE_MASK;
                
                FASTWriterScriptPlayerDispatch.writeNullText(token, idx, writer, textHeap);
            } else {
                // decimal bytes
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // decimal
                    int idx = token & intInstanceMask;
                    
                    //TODO: A, must implement null for decimals
                    FASTWriterScriptPlayerDispatch.writeNullInt(token, writer,intValues, idx); 

                    int idx1 = token & longInstanceMask;
                    
                    FASTWriterScriptPlayerDispatch.writeNullLong(token, idx1, writer, longValues);
                } else {
                    // byte
                    FASTWriterScriptPlayerDispatch.writeNullBytes(token, writer, byteHeap, instanceBytesMask);
                }
            }
        }

    }

    /**
     * Method for writing signed unsigned and/or optional longs. To write the
     * "null" or absence of a value use void write(int id)
     */
    public void writeLong(int token, long value) {

        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                acceptLongUnsigned(token, value);
            } else {
                acceptLongSigned(token, value);
            }
        } else {
            if (value == TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG) {
                write(token);
            } else {
                // optional
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    acceptLongUnsignedOptional(token, value);
                } else {
                    acceptLongSignedOptional(token, value);
                }
            }
        }
    }

    //TODO: C, should not be public
    public void acceptLongSignedOptional(int token, long value) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongSignedOptional(value, writer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    long delta = value - longValues[idx];
                    
                    genWriteLongSignedDeltaOptional(value, idx, delta, writer);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";
                
                genWriteLongSignedConstantOptional(writer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & longInstanceMask;
                    
                    genWriteLongSignedCopyOptional(value, idx, writer);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    genWriteLongSignedIncrementOptional(value, idx, writer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongSignedDefaultOptional(value, constDefault, writer);
            }
        }
    }



    public void acceptLongSigned(int token, long value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongSignedNone(value, idx, writer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    genWriteLongSignedDelta(value, idx, writer);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & longInstanceMask;
                    
                    genWriteLongSignedCopy(value, idx, writer);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    
                    genWriteLongSignedIncrement(value, idx, writer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongSignedDefault(value, constDefault, writer);
            }
        }

    }



    private void acceptLongUnsignedOptional(int token, long value) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongUnsignedNoneOptional(value, writer);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedDeltaOptional(value, idx, writer);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";
                genWriteLongUnsignedConstantOptional(writer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedCopyOptional(value, idx, writer);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedIncrementOptional(value, idx, writer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefaultOptional(value, constDefault, writer);
            }
        }
    }



    private void acceptLongUnsigned(int token, long value) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongUnsignedNone(value, idx, writer);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedDelta(value, idx, writer);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedCopy(value, idx, writer);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedIncrement(value, idx, writer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefault(value, constDefault, writer);
            }
        }
    }


    /**
     * Method for writing signed unsigned and/or optional integers. To write the
     * "null" or absence of an integer use void write(int id)
     */
    public void writeInteger(int token, int value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                acceptIntegerUnsigned(token, value);
            } else {
                acceptIntegerSigned(token, value);
            }
        } else {
            if (value == TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT) {
                write(token);
            } else {
                // optional
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    acceptIntegerUnsignedOptional(token, value);
                } else {
                    acceptIntegerSignedOptional(token, value);
                }
            }
        }
    }

    public void acceptIntegerSigned(int token, int value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedNone(value, idx, writer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedDelta(value, idx, writer);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value "
                        + intValues[token & intInstanceMask]
                        + " from the template may be sent";
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedCopy(value, idx, writer);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedIncrement(value, idx, writer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerSignedDefault(value, idx, constDefault, writer);
            }
        }
    }


    private void acceptIntegerUnsigned(int token, int value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & intInstanceMask;

                    genWriteIntegerUnsignedNone(value, idx, writer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = (token & intInstanceMask);

                    genWriteIntegerUnsignedDelta(value, idx, writer);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value from the template may be sent";
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;
                    genWriteIntegerUnsignedCopy(value, idx, writer);
                } else {
                    // increment
                    int idx = token & intInstanceMask;
                    genWriteIntegerUnsignedIncrement(value, idx, writer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerUnsignedDefault(value, constDefault, writer);
            }
        }
    }


    public void acceptIntegerSignedOptional(int token, int value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteIntegerSignedNoneOptional(value, writer);
                } else {
                    // delta
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedDeltaOptional(value, idx, writer);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value from the template may be sent";
                genWriteIntegerSignedConstantOptional(writer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedCopyOptional(value, idx, writer);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedIncrementOptional(value, idx, writer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerSignedDefaultOptional(value, idx, constDefault, writer);
            }
        }
    }


    private void acceptIntegerUnsignedOptional(int token, int value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteIntegerUnsignedNoneOptional(value, writer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & intInstanceMask;

                    genWriteIntegerUnsignedDeltaOptional(value, idx, writer);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value from the template may be sent";
                genWriteIntegerUnsignedConstantOptional(writer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;
                    genWriteIntegerUnsignedCopyOptional(value, idx, writer);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    genWriteIntegerUnsignedIncrementOptional(value, idx, writer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerUnsignedDefaultOptional(value, constDefault, writer);
            }
        }
    }


    public void write(int token, byte[] value, int offset, int length) {

        assert (0 != (token & (2 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            acceptByteArray(token, value, offset, length);
        } else {
            acceptByteArrayOptional(token, value, offset, length);
        }
    }

    private void acceptByteArrayOptional(int token, byte[] value, int offset, int length) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNoneOptional(value, offset, length);
                } else {
                    // tail
                    genWriteBytesTailOptional(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteBytesConstantOptional(token);
                } else {
                    // delta
                    genWriteBytesDeltaOptional(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopyOptional(token, value, offset, length);
            } else {
                // default
                genWriteBytesDefaultOptional(token, value, offset, length);
            }
        }
    }


    private void acceptByteArray(int token, byte[] value, int offset, int length) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNone(value, offset, length);
                } else {
                    // tail
                    genWriteBytesTail(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteBytesConstant(token);
                } else {
                    // delta
                    genWriteBytesDelta(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopy(token, value, offset, length);
            } else {
                // default
                genWriteBytesDefault(token, value, offset, length);
            }
        }
    }


    // TODO: Z, add writeDup(int id) for repeating the last value sent,
    // this can avoid string check for copy operation if its already known that
    // we are sending the same value.

    public void write(int token, ByteBuffer buffer) {

        assert (0 != (token & (2 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            acceptByteBuffer(token, buffer);
        } else {
            acceptByteBufferOptional(token, buffer);
        }
    }

    private void acceptByteBufferOptional(int token, ByteBuffer value) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriterBytesNoneOptional(value);
                } else {
                    // tail
                    genWriterBytesTailOptional(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    genWriteBytesConstantOptional(token);
                } else {
                    // delta
                    genWriterBytesDeltaOptional(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriterBytesCopyOptional(token, value);
            } else {
                // default
                genWriterBytesDefaultOptional(token, value);
            }
        }
    }



    private void acceptByteBuffer(int token, ByteBuffer value) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNone(value);
                } else {
                    // tail
                    genWriteBytesTail(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteBytesConstant2(token);
                } else {
                    // delta
                    genWriteBytesDelta(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopy(token, value);
            } else {
                // default
                genWriteBytesDefault(token, value);
            }
        }
    }



    public void write(int token, CharSequence value) {

        assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii
                acceptCharSequenceASCII(token, value);
            } else {
                // utf8
                acceptCharSequenceUTF8(token, value);
            }
        } else {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii optional
                acceptCharSequenceASCIIOptional(token, value);
            } else {
                // utf8 optional
                acceptCharSequenceUTF8Optional(token, value);
            }
        }
    }

    private void acceptCharSequenceUTF8Optional(int token, CharSequence value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteUTFTextNoneOptional(value, writer);
                } else {
                    // tail
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteUTFTextTailOptional(value, idx, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteUTFTextConstantOptional(writer);
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteUTFTextDeltaOptional(value, idx, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteUTFTextCopyOptional(value, idx, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteUTFTextDefaultOptional(value, idx, writer, textHeap);
            }
        }
    }



    private void acceptCharSequenceUTF8(int token, CharSequence value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteUTFTextNone(value, writer);
                } else {
                    // tail
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    
                    genWriteUTFTextTail(value, idx, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteUTFTextDelta(value, idx, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                // System.err.println("AA");
                genWriteUTFTextCopy(value, idx, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteUTFTextDefault(value, idx, writer, textHeap);
            }
        }

    }



    private void acceptCharSequenceASCIIOptional(int token, CharSequence value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_None)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    if (null == value) {
                        genWriteNull(writer);
                    } else {
                        genWriteTextNone(value, writer);
                    }
                } else {
                    // tail
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Tail)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextTailOptional(value, idx, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    genWriteTextConstantOptional(token);
                } else {
                    // delta
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Delta)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextDeltaOptional(value, idx, writer, textHeap);

                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Copy)) : "Found "
                        + TokenBuilder.tokenToString(token);
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextCopyOptional(value, idx, writer, textHeap);

            } else {
                // default
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Default)) : "Found "
                        + TokenBuilder.tokenToString(token);
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextDefaultOptional(value, idx, writer, textHeap);

            }
        }

    }


    private void acceptCharSequenceASCII(int token, CharSequence value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextNone(value, writer);
                } else {
                    // tail
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextTail(value, idx, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextDelta(value, idx, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextCopy(value, idx, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextDefault(value, idx, writer, textHeap);
            }
        }

    }



    public void write(int token, char[] value, int offset, int length) {

        assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii
                acceptCharArrayASCII(token, value, offset, length);
            } else {
                // utf8
                acceptCharArrayUTF8(token, value, offset, length);
            }
        } else {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii optional
                acceptCharArrayASCIIOptional(token, value, offset, length);
            } else {
                // utf8 optional
                acceptCharArrayUTF8Optional(token, value, offset, length);
            }
        }
    }

    private void acceptCharArrayUTF8Optional(int token, char[] value, int offset, int length) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextUTFNoneOptional(value, offset, length, writer);

                } else {
                    // tail
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextUTFTailOptional(value, offset, length, idx, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextUTFConstantOptional(token, writer);
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextUTFDeltaOptional(value, offset, length, idx, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextUTFCopyOptional(value, offset, length, idx, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextUTFDefaultOptional(value, offset, length, idx, writer, textHeap);
            }
        }

    }



    private void acceptCharArrayUTF8(int token, char[] value, int offset, int length) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextUTFNone(value, offset, length, writer);

                } else {
                    // tail
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextUTFTail(value, offset, length, idx, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextUTFDelta(value, offset, length, idx, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextUTFCopy(value, offset, length, idx, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                int constId = idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK;
                
                genWriteTextUTFDefault(value, offset, length, constId, writer, textHeap);
            }
        }

    }


    private void acceptCharArrayASCIIOptional(int token, char[] value, int offset, int length) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextNoneOptional(value, offset, length);
                } else {
                    // tail
                    genWriteTextTailOptional2(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextConstantOptional(token);
                } else {
                    // delta
                    genWriteTextDeltaOptional2(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextCopyOptional(value, offset, length, idx, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                int constId = idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK;
                
                genWriteTextDefaultOptional(value, offset, length, constId, writer, textHeap);
            }
        }

    }



    private void acceptCharArrayASCII(int token, char[] value, int offset, int length) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextNone(value, offset, length);
                } else {
                    // tail
                    genWriteTextTail2(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    genWriteTextDelta2(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextCopy2(token, value, offset, length);
            } else {
                // default
                genWriteTextDefault2(token, value, offset, length);
            }
        }
    }



    public void openGroup(int token, int pmapSize) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 == (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            genWriteOpenGroup(pmapSize, writer);
        }

    }

    public void openGroup(int token, int templateId, int pmapSize) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        if (pmapSize > 0) {
            genWriteOpenTemplatePMap(templateId, pmapSize, writer);
        } else {
            genWriteOpenTemplate(templateId);
        }
    }

    // must happen just before Group so the Group in question must always have
    // an outer group.
    private void pushTemplate(int templateId) {
        int top = templateStack[templateStackHead];
        if (top == templateId) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(templateId);
            top = templateId;
        }

        templateStack[templateStackHead++] = top;
    }

    public void closeGroup(int token) {
        assert (token < 0);
        assert (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            if (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) {
                genWriteCloseTemplatePMap(writer);
            } else {
                genWriteClosePMap(writer);
            }
        } else {
            if (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) {
                genWriteCloseTemplate();
            }
        }
    }


    public void flush() {
        PrimitiveWriter.flush(writer);
    }

    public void reset() {

        dictionaryFactory.reset(intValues);
        dictionaryFactory.reset(longValues);
        dictionaryFactory.reset(textHeap);
        dictionaryFactory.reset(byteHeap);
        templateStackHead = 0;
        sequenceCountStackHead = 0; 
    }

    public boolean isFirstSequenceItem() {
        return isFirstSequenceItem;
    }

    public boolean isSkippedSequence() {
        return isSkippedSequence;
    }

    // long fieldCount = 0;

    public boolean dispatchWriteByToken(int fieldPos) {

        int token = fullScript[activeScriptCursor];

        assert (gatherWriteData(writer, token, activeScriptCursor, fieldPos, queue));

        if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
            // 0????
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 00???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    writeInteger(token, FASTRingBufferReader.readInt(queue, fieldPos));
                } else {
                    writeLong(token, FASTRingBufferReader.readLong(queue, fieldPos));
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    int length = FASTRingBufferReader.readTextLength(queue, fieldPos);
                    if (length < 0) {
                        write(token);
                    } else {
                        char[] buffer = queue.readRingCharBuffer(fieldPos);
                        write(token, ringCharSequence.set(buffer, queue.readRingCharPos(fieldPos), queue.readRingCharMask(), length));
                    }
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        
                        
                        int exponent = FASTRingBufferReader.readInt(queue, fieldPos);
                        long mantissa = FASTRingBufferReader.readLong(queue, fieldPos + 1);
                        
                        int expoToken = token&
                                ((TokenBuilder.MASK_TYPE<<TokenBuilder.SHIFT_TYPE)| 
                                        (TokenBuilder.MASK_ABSENT<<TokenBuilder.SHIFT_ABSENT)|
                                        (TokenBuilder.MAX_INSTANCE));
                        expoToken |= (token>>TokenBuilder.SHIFT_OPER_DECIMAL_EX)&(TokenBuilder.MASK_OPER<<TokenBuilder.SHIFT_OPER);
                        expoToken |= 0x80000000;
                        
                        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                            
                            
                            acceptIntegerSigned(expoToken, exponent);
                            
                            //NOTE: moving forward one to get second token for decimals
                            token = fullScript[++activeScriptCursor];
                            
                            acceptLongSigned(token, mantissa);
                        } else {
                            
                            //TODO: A, need null decimal implementation.
                            
                            if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT==exponent) {
                            	int idx = expoToken & intInstanceMask; 
                                
                                FASTWriterScriptPlayerDispatch.writeNullInt(expoToken, writer, intValues, idx); //needed for decimal.
                            } else {
                            	acceptIntegerSignedOptional(expoToken, exponent);
                            }
                            
                            //NOTE: moving forward one to get second token for decimals
                            token = fullScript[++activeScriptCursor];
                            
                            if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG==mantissa) {
                            	int idx = token & longInstanceMask; 
                                
                                FASTWriterScriptPlayerDispatch.writeNullLong(token, idx, writer, longValues); 
                            } else {
                            	acceptLongSignedOptional(token, mantissa);
                            }
                        }
                    } else {
                        // //0111? ByteArray
                        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                            // 01110 ByteArray
                            // queue.selectByteSequence(fieldPos);
                            // write(token,queue); TODO: B, copy the text
                            // implementation
                        } else {
                            // 01111 ByteArrayOptional
                            // queue.selectByteSequence(fieldPos);
                            // write(token,queue); TODO: B, copy the text
                            // implementation
                        }
                    }
                }
            }
        } else {
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 10???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    // 100??
                    // Group Type, no others defined so no need to keep checking
                    if (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER))) {

                        isSkippedSequence = false;
                        isFirstSequenceItem = false;
                        // this is NOT a message/template so the non-template
                        // pmapSize is used.
                        // System.err.println("open group:"+TokenBuilder.tokenToString(token));
                        openGroup(token, nonTemplatePMapSize);

                    } else {
                        // System.err.println("close group:"+TokenBuilder.tokenToString(token));
                        closeGroup(token);// closing this seq causing throw!!
                        if (0 != (token & (OperatorMask.Group_Bit_Seq << TokenBuilder.SHIFT_OPER))) {
                            // must always pop because open will always push
                            if (0 == --sequenceCountStack[sequenceCountStackHead]) {
                                sequenceCountStackHead--;// pop sequence off
                                                         // because they have
                                                         // all been used.
                                return false;// this sequence is done.
                            } else {
                                return true;// true if this sequence must be
                                            // visited again.
                            }
                        }
                    }

                } else {
                    // 101??
                    // Length Type, no others defined so no need to keep
                    // checking
                    // Only happens once before a node sequence so push it on
                    // the count stack
                    int length = FASTRingBufferReader.readInt(queue, fieldPos);
                    writeInteger(token, length);

                    if (length == 0) {
                        isFirstSequenceItem = false;
                        isSkippedSequence = true;
                    } else {
                        isFirstSequenceItem = true;
                        isSkippedSequence = false;
                        sequenceCountStack[++sequenceCountStackHead] = length;
                    }
                    return true;
                }
            } else {
                // 11???
                // Dictionary Type, no others defined so no need to keep
                // checking
                if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                    // reset the values
                    int dictionary = TokenBuilder.MAX_INSTANCE & token;

                    int[] members = dictionaryMembers[dictionary];
                    // System.err.println(members.length+" "+Arrays.toString(members));

                    int m = 0;
                    int limit = members.length;
                    if (limit > 0) {
                        int idx = members[m++];
                        while (m < limit) {
                            assert (idx < 0);

                            if (0 == (idx & 8)) {
                                if (0 == (idx & 4)) {
                                    // integer
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        genWriteDictionaryIntegerReset(idx);
                                    }
                                } else {
                                    // long
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        genWriteDictionaryLongReset(idx);
                                    }
                                }
                            } else {
                                if (0 == (idx & 4)) {
                                    // text
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        if (null!=textHeap) {
                                            genWriteDictionaryTextReset(idx);
                                        }
                                    }
                                } else {
                                    if (0 == (idx & 2)) {
                                        // decimal
                                        throw new UnsupportedOperationException("Implemented as int and long reset");
                                    } else {
                                        // bytes
                                        while (m < limit && (idx = members[m++]) >= 0) {
                                            if (null!=byteHeap) {
                                                genWriteDictionaryBytesReset(idx);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    // use last value from this location
                    readFromIdx = TokenBuilder.MAX_INSTANCE & token;
                }

            }

        }
        return false;
    }



    private boolean gatherWriteData(PrimitiveWriter writer, int token, int cursor, int fieldPos, FASTRingBuffer queue) {

        if (null != observer) {

            String value = "";
            int type = TokenBuilder.extractType(token);
            if (type == TypeMask.GroupLength || type == TypeMask.IntegerSigned
                    || type == TypeMask.IntegerSignedOptional || type == TypeMask.IntegerUnsigned
                    || type == TypeMask.IntegerUnsignedOptional) {

                value = "<" + FASTRingBufferReader.readInt(queue, fieldPos) + ">";

            } else if (type == TypeMask.Decimal || type == TypeMask.DecimalOptional) {

                value = "<e:" + FASTRingBufferReader.readInt(queue, fieldPos) + "m:" + FASTRingBufferReader.readLong(queue, fieldPos + 1) + ">";

            } else if (type == TypeMask.TextASCII || type == TypeMask.TextASCIIOptional || type == TypeMask.TextUTF8
                    || type == TypeMask.TextUTF8Optional) {
                value = "<len:" + FASTRingBufferReader.readTextLength(queue, fieldPos) + ">";
            }

            // TotalWritten is updated each time the pump pulls more bytes to
            // write.

            long absPos = PrimitiveWriter.totalWritten(writer) + PrimitiveWriter.bytesReadyToWrite(writer);
            // TODO: Z, this position is never right because it is changed by
            // the pmap length which gets trimmed.

            observer.tokenItem(absPos, token, cursor, value);
        }

        return true;
    }

    public void openMessage(int pmapMaxSize, int templateId) {

        genWriteOpenMessage(pmapMaxSize, templateId, writer);

    }


    public void writePreamble(byte[] preambleData) {
        genWritePreamble(preambleData, writer);
    }
    
    ///////////////////////
    
    
    protected void genWriteOpenMessage(int pmapMaxSize, int templateId, PrimitiveWriter writer) {
        writer.openPMap(pmapMaxSize);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        writer.closePMap();// TODO: A, this needs to be close but not sure this
        // is the right location.
        writer.writeIntegerUnsigned(templateId);
    }

    protected void genWritePreamble(byte[] preambleData, PrimitiveWriter writer) {
        writer.writeByteArrayData(preambleData, 0, preambleData.length);
    }
    
    protected void genWriteUTFTextDefaultOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeNull();
            }
        } else {
            if (textHeap.equals(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK, value)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeIntegerUnsigned(value.length() + 1);
                writer.writeTextUTF(value);
            }
        }
    }

    protected void genWriteUTFTextCopyOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(value.length() + 1);
            writer.writeTextUTF(value);
            textHeap.set(idx, value, 0, value.length());
        }
    }

    protected void genWriteUTFTextDeltaOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value);
        int tailCount = textHeap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount; //+1 for optional
            writer.writeIntegerSigned(trimTail >= 0 ? trimTail + 1 : trimTail);
            int length = (value.length() - headCount);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTFAfter(headCount, value);
            textHeap.appendTail(idx, trimTail, headCount, value);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 1 : -trimHead);
            int valueSend = value.length() - tailCount;
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTFBefore(value, valueSend);
            textHeap.appendHead(idx, trimHead, value, valueSend);
        }
    }
    
    protected void genWriteUTFTextConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteUTFTextTailOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail + 1);// plus 1 for optional
        int length = (value.length() - headCount);
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTFAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteUTFTextNoneOptional(CharSequence value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value.length() + 1);
        writer.writeTextUTF(value);
    }
    
    protected void genWriteUTFTextDefault(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK, value)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(value.length());
            writer.writeTextUTF(value);
        }
    }

    protected void genWriteUTFTextCopy(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(value.length());
            writer.writeTextUTF(value);
            textHeap.set(idx, value, 0, value.length());
        }
    }

    protected void genWriteUTFTextDelta(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value);
        int tailCount = textHeap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            writer.writeIntegerSigned(trimTail);
            writer.writeIntegerUnsigned(value.length() - headCount);
            writer.writeTextUTFAfter(headCount, value);
            textHeap.appendTail(idx, trimTail, headCount, value);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 0 : -trimHead);
            int valueSend = value.length() - tailCount;
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTFBefore(value, valueSend);
            textHeap.appendHead(idx, trimHead, value, valueSend);
        }
    }

    protected void genWriteUTFTextTail(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail);
        int length = (value.length() - headCount);
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTFAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteUTFTextNone(CharSequence value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value.length());
        writer.writeTextUTF(value);
    }

    protected void genWriteTextDefaultOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK)) {
                writer.writePMapBit((byte) 0, writer);
            } else {
                writer.writePMapBit((byte) 1, writer);
                writer.writeNull();
            }
        } else {
            if (textHeap.equals(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK, value)) {
                writer.writePMapBit((byte) 0, writer);
            } else {
                writer.writePMapBit((byte) 1, writer);
                writer.writeTextASCII(value);
            }
        }
    }

    protected void genWriteTextCopyOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(idx)) {
                writer.writePMapBit((byte) 0, writer);
            } else {
                writer.writePMapBit((byte) 1, writer);
                writer.writeNull();
            }
        } else {
            if (textHeap.equals(idx, value)) {
                writer.writePMapBit((byte) 0, writer);
            } else {
                writer.writePMapBit((byte) 1, writer);
                writer.writeTextASCII(value);
                textHeap.set(idx, value, 0, value.length());
            }
        }
    }

    protected void genWriteTextDeltaOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            writer.writeIntegerSigned(0);
        } else {
            // count matching front or back chars
            int headCount = textHeap.countHeadMatch(idx, value);
            int tailCount = textHeap.countTailMatch(idx, value);
            if (headCount > tailCount) {
                int trimTail = textHeap.length(idx) - headCount;
                assert (trimTail >= 0);
                writer.writeIntegerSigned(trimTail + 1);// must add one because this
                                                        // is optional
                writer.writeTextASCIIAfter(headCount, value);
                textHeap.appendTail(idx, trimTail, headCount, value);
            } else {
                int trimHead = textHeap.length(idx) - tailCount;
                writer.writeIntegerSigned(0 == trimHead ? 1 : -trimHead);
                
                int sentLen = value.length() - tailCount;
                writer.writeTextASCIIBefore(value, sentLen);
                textHeap.appendHead(idx, trimHead, value, sentLen);
            }
        }
    }

    protected void genWriteTextTailOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail + 1);
        writer.writeTextASCIIAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteNull(PrimitiveWriter writer) {
        writer.writeNull();
    }
    
    protected void genWriteTextDefault(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK, value)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value);
        }
    }

    protected void genWriteTextCopy(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            // System.err.println("char seq length:"+value.length());
            writer.writeTextASCII(value);
            textHeap.set(idx, value, 0, value.length());
        }
    }

    protected void genWriteTextDelta(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value);
        int tailCount = textHeap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            if (trimTail < 0) {
                throw new UnsupportedOperationException(trimTail + "");
            }
            writer.writeIntegerSigned(trimTail);
            writer.writeTextASCIIAfter(headCount, value);
            textHeap.appendTail(idx, trimTail, headCount, value);
        } else {
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 0 : -trimHead);
            
            int sentLen = value.length() - tailCount;
            writer.writeTextASCIIBefore(value, sentLen);
            textHeap.appendHead(idx, trimHead, value, sentLen);
        }
    }
    
    protected void genWriteTextTail(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail);
        writer.writeTextASCIIAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteTextNone(CharSequence value, PrimitiveWriter writer) {
        writer.writeTextASCII(value);
    }
    
    protected void genWriteTextUTFDefaultOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length + 1);
            writer.writeTextUTF(value, offset, length);
        }
    }

    protected void genWriteTextUTFCopyOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length + 1);
            writer.writeTextUTF(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    protected void genWriteTextUTFDeltaOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            int valueSend = length - headCount;
            int startAfter = offset + headCount;
            textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
            writer.writeIntegerUnsigned(trimTail + 1);
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTF(value, startAfter, valueSend);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(trimHead == 0 ? 1 : -trimHead);
            int len = length - tailCount;
            writer.writeIntegerUnsigned(len);
            writer.writeTextUTF(value, offset, len);
            textHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextUTFConstantOptional(int token, PrimitiveWriter writer) {
        writer.writePMapBit((byte) 1, writer);
    }

    protected void genWriteTextUTFTailOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount;
        int valueSend = length - headCount;
        int startAfter = offset + headCount;
        textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        
        writer.writeIntegerUnsigned(trimTail + 1);
        writer.writeIntegerUnsigned(valueSend);
        writer.writeTextUTF(value, startAfter, valueSend);
    }

    protected void genWriteTextUTFNoneOptional(char[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(length + 1);
        writer.writeTextUTF(value, offset, length);
    }

    protected void genWriteTextUTFDefault(char[] value, int offset, int length, int constId, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(constId, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTF(value, offset, length);
        }
    }

    protected void genWriteTextUTFCopy(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTF(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    protected void genWriteTextUTFDelta(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            int valueSend = length - headCount;
            int startAfter = offset + headCount + headCount;
            textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
            
            writer.writeIntegerUnsigned(trimTail + 0);
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTF(value, startAfter, valueSend);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(trimHead == 0 ? 0 : -trimHead);
            
            int len = length - tailCount;
            writer.writeIntegerUnsigned(len);
            writer.writeTextUTF(value, offset, len);
            
            textHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextUTFTail(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount;
        int valueSend = length - headCount;
        int startAfter = offset + headCount;
        textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        
        writer.writeIntegerUnsigned(trimTail + 0);
        writer.writeIntegerUnsigned(valueSend);
        writer.writeTextUTF(value, startAfter, valueSend);
    }

    
    protected void genWriteTextUTFNone(char[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTF(value, offset, length);
    }
    
    protected void genWriteTextDefaultOptional(char[] value, int offset, int length, int constId, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(constId, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
        }
    }

    protected void genWriteTextCopyOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    private void genWriteTextDeltaOptional2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            writer.writeIntegerSigned(trimTail + 1); // cut off these from tail,
                                                     // also add 1 because this
                                                     // is optional
        
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            writer.writeTextASCII(value, valueStart, valueSend);
            textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 1 : -trimHead);
        
            int len = length - tailCount;
            writer.writeTextASCII(value, offset, len);
            textHeap.appendHead(idx, trimHead, value, offset, len);
        
        }
    }
    
    private void genWriteTextConstantOptional(int token) {
        writer.writePMapBit((byte) 1, writer);
        // the writeNull will take care of the rest.
    }

    private void genWriteTextTailOptional2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount; // head count is total that
                                                     // match from head.
        
        writer.writeIntegerUnsigned(trimTail + 1); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        writer.writeTextASCII(value, valueStart, valueSend);
        textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
    }

    private void genWriteTextNoneOptional(char[] value, int offset, int length) {
        writer.writeTextASCII(value, offset, length);
    }
    
    private void genWriteTextDefault2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        if (textHeap.equals(idx | FASTWriterScriptPlayerDispatch.INIT_VALUE_MASK, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
        }
    }

    private void genWriteTextCopy2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        if (textHeap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0, writer);
        } else {
            writer.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    private void genWriteTextDelta2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            writer.writeIntegerSigned(trimTail); // cut off these from tail
        
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            writer.writeTextASCII(value, valueStart, valueSend);
            textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerUnsigned(-trimHead);
        
            int len = length - tailCount;
            writer.writeTextASCII(value, offset, len);
        
            textHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    private void genWriteTextTail2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount; // head count is total that
                                                     // match from head.
        writer.writeIntegerUnsigned(trimTail); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        writer.writeTextASCII(value, valueStart, valueSend);
        textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
    }

    private void genWriteTextNone(char[] value, int offset, int length) {
        writer.writeTextASCII(value, offset, length);
    }
    
    private void genWriterBytesDefaultOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value)) {
        	writer.writePMapBit((byte)0, writer); 
        	value.position(value.limit());//skip over the data just like we wrote it.
        } else {
        	writer.writePMapBit((byte)1, writer);
        	int len = value.remaining();
        	if (len<0) {
        		len = 0;
        	}
        	writer.writeIntegerUnsigned(len+1);
        	writer.writeByteArrayData(value);
        }
    }

    private void genWriterBytesCopyOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx, value)) {
        	writer.writePMapBit((byte)0, writer);
        	value.position(value.limit());//skip over the data just like we wrote it.
        } 
        else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(value.remaining()+1);
        	byteHeap.set(idx, value);//position is NOT modified
        	writer.writeByteArrayData(value); //this moves the position in value
        }
    }

    private void genWriterBytesDeltaOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value);
        int tailCount = byteHeap.countTailMatch(idx, value);
        if (headCount>tailCount) {
        	writeBytesTail(idx, headCount, value, 1); //does not modify position
        } else {
        	writeBytesHead(idx, tailCount, value, 1); //does not modify position
        }
        value.position(value.limit());//skip over the data just like we wrote it.
    }
    
    //TODO: B, will be static
    private void writeBytesTail(int idx, int headCount, ByteBuffer value, final int optional) {
        
    
        
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+optional : trimTail);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        writer.writeIntegerUnsigned(valueSend);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        
    }
    
    //TODO: B,  will be static
    private void writeBytesHead(int idx, int tailCount, ByteBuffer value, int opt) {
        
        //replace head, tail matches to tailCount
        int trimHead = byteHeap.length(idx)-tailCount;
        writer.writeIntegerSigned(trimHead==0? opt: -trimHead); 
        
        int len = value.remaining() - tailCount;
        int offset = value.position();
        writer.writeIntegerUnsigned(len);
        writer.writeByteArrayData(value, offset, len);
        byteHeap.appendHead(idx, trimHead, value, offset, len);
    }

    private void genWriterBytesTailOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        int headCount = byteHeap.countHeadMatch(idx, value);
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
        	throw new ArrayIndexOutOfBoundsException();
        }
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+1 : trimTail);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
        		
        writer.writeIntegerUnsigned(valueSend);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    private void genWriterBytesNoneOptional(ByteBuffer value) {
        writer.writeIntegerUnsigned(value.remaining()+1);
        writer.writeByteArrayData(value); //this moves the position in value
    }

    private void genWriteBytesDefault(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value)) {
        	writer.writePMapBit((byte)0, writer);
        	value.position(value.limit());//skip over the data just like we wrote it.
        } else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(value.remaining());
        	writer.writeByteArrayData(value); //this moves the position in value
        }
    }

    private void genWriteBytesCopy(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        //System.err.println("AA");
        if (byteHeap.equals(idx, value)) {
        	writer.writePMapBit((byte)0, writer);
        	value.position(value.limit());//skip over the data just like we wrote it.
        } else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(value.remaining());
        	byteHeap.set(idx, value);//position is NOT modified
        	writer.writeByteArrayData(value); //this moves the position in value
        }
    }

    private void genWriteBytesDelta(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value);
        int tailCount = byteHeap.countTailMatch(idx, value);
        if (headCount>tailCount) {
        	int trimTail = byteHeap.length(idx)-headCount;
            if (trimTail<0) {
            	throw new ArrayIndexOutOfBoundsException();
            }
            writer.writeIntegerUnsigned(trimTail>=0? trimTail+0 : trimTail);
            
            int valueSend = value.remaining()-headCount;
            int startAfter = value.position()+headCount;
            		
            writer.writeIntegerUnsigned(valueSend);
            //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
            byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
            writer.writeByteArrayData(value, startAfter, valueSend); //does not modify position
        } else {
        	//replace head, tail matches to tailCount
            int trimHead = byteHeap.length(idx)-tailCount;
            writer.writeIntegerSigned(trimHead==0? 0: -trimHead); 
            
            int len = value.remaining() - tailCount;
            int offset = value.position();
            writer.writeIntegerUnsigned(len);
            writer.writeByteArrayData(value, offset, len);
            byteHeap.appendHead(idx, trimHead, value, offset, len); //does not modify position
        }
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    private void genWriteBytesConstant2(int token) {
    }

    private void genWriteBytesTail(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        int headCount = byteHeap.countHeadMatch(idx, value);
        		
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
        	throw new ArrayIndexOutOfBoundsException();
        }
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+0 : trimTail);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
        		
        writer.writeIntegerUnsigned(valueSend);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    private void genWriteBytesNone(ByteBuffer value) {
        writer.writeIntegerUnsigned(value.remaining());
        writer.writeByteArrayData(value); //this moves the position in value
    }
    
    private void genWriteBytesDefault(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
        	writer.writePMapBit((byte)0, writer);
        } else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(length);
        	writer.writeByteArrayData(value,offset,length);
        }
    }

    private void genWriteBytesCopy(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx, value, offset, length)) {
        	writer.writePMapBit((byte)0, writer);
        }
        else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(length);
        	writer.writeByteArrayData(value,offset,length);
        	byteHeap.set(idx, value, offset, length);
        }
    }

    private void genWriteBytesDelta(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = byteHeap.countTailMatch(idx, value, offset+length, length);
        if (headCount>tailCount) {
        	writeBytesTail(idx, headCount, value, offset+headCount, length, 0);
        } else {
        	writeBytesHead(idx, tailCount, value, offset, length, 0);
        }
    }

    private void genWriteBytesConstant(int token) {
    }

    private void genWriteBytesTail(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        
        int trimTail = byteHeap.length(idx)-headCount;
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        writer.writeIntegerUnsigned(valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
    }

    private void genWriteBytesNone(byte[] value, int offset, int length) {
        writer.writeIntegerUnsigned(length);
        writer.writeByteArrayData(value,offset,length);
    }
    
    private void writeBytesHead(int idx, int tailCount, byte[] value, int offset, int length, int opt) {
        
        //replace head, tail matches to tailCount
        int trimHead = byteHeap.length(idx)-tailCount;
        writer.writeIntegerSigned(trimHead==0? opt: -trimHead); 
        
        int len = length - tailCount;
        writer.writeIntegerUnsigned(len);
        writer.writeByteArrayData(value, offset, len);
        
        byteHeap.appendHead(idx, trimHead, value, offset, len);
    }
    
   private void writeBytesTail(int idx, int headCount, byte[] value, int offset, int length, final int optional) {
        int trimTail = byteHeap.length(idx)-headCount;
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+optional: trimTail);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        writer.writeIntegerUnsigned(valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
    }
    
    private void genWriteBytesDefaultOptional(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
        	writer.writePMapBit((byte)0, writer);
        } else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(length+1);
        	writer.writeByteArrayData(value,offset,length);
        }
    }

    private void genWriteBytesCopyOptional(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx, value, offset, length)) {
        	writer.writePMapBit((byte)0, writer);
        } else {
        	writer.writePMapBit((byte)1, writer);
        	writer.writeIntegerUnsigned(length+1);
        	writer.writeByteArrayData(value,offset,length);
        	byteHeap.set(idx, value, offset, length);
        }
    }

    private void genWriteBytesDeltaOptional(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = byteHeap.countTailMatch(idx, value, offset+length, length);
        if (headCount>tailCount) {
        	int trimTail = byteHeap.length(idx)-headCount;
            writer.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail);
            
            int valueSend = length-headCount;
            int startAfter = offset+headCount;
            
            writer.writeIntegerUnsigned(valueSend);
            writer.writeByteArrayData(value, startAfter, valueSend);
            byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        } else {
        	//replace head, tail matches to tailCount
            int trimHead = byteHeap.length(idx)-tailCount;
            writer.writeIntegerSigned(trimHead==0? 1: -trimHead); 
            
            int len = length - tailCount;
            writer.writeIntegerUnsigned(len);
            writer.writeByteArrayData(value, offset, len);
            
            byteHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    private void genWriteBytesConstantOptional(int token) {
        writer.writePMapBit((byte)1, writer);
        //the writeNull will take care of the rest.
    }

    private void genWriteBytesTailOptional(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = byteHeap.length(idx)-headCount;
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        writer.writeIntegerUnsigned(valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
    }

    private void genWriteBytesNoneOptional(byte[] value, int offset, int length) {
        writer.writeIntegerUnsigned(length+1);
        writer.writeByteArrayData(value,offset,length);
    }
    
    private void genWriteIntegerSignedDefault(int value, int idx, int constDefault, PrimitiveWriter writer) {
        writer.writeIntegerSignedDefault(value, constDefault);
    }

    private void genWriteIntegerSignedIncrement(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerSignedIncrement(value, idx, intValues);
    }

    private void genWriteIntegerSignedCopy(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerSignedCopy(value, idx, intValues);
    }

    private void genWriteIntegerSignedDelta(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerSignedDelta(value, idx, intValues);
    }

    private void genWriteIntegerSignedNone(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerSigned(intValues[idx] = value);
    }
    
    private void genWriteIntegerUnsignedDefault(int value, int constDefault, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedDefault(value, constDefault);
    }

    private void genWriteIntegerUnsignedIncrement(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedIncrement(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedCopy(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedCopy(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedDelta(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedDelta(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedNone(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(intValues[idx] = value);
    }

    private void genWriteIntegerSignedDefaultOptional(int value, int idx, int constDefault, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;// room for null
        }
        writer.writeIntegerSignedDefaultOptional(value, constDefault);
    }

    private void genWriteIntegerSignedIncrementOptional(int value, int idx, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;
        }
        int last = intValues[idx];
        intValues[idx] = value;
        writer.writeIntegerSignedIncrementOptional(value, last);
    }

    private void genWriteIntegerSignedCopyOptional(int value, int idx, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;
        }
        
        writer.writeIntegerSignedCopyOptional(value, idx, intValues);
    }

    private void genWriteIntegerSignedConstantOptional(PrimitiveWriter writer) {
        writer.writePMapBit((byte) 1, writer);
    }

    private void genWriteIntegerSignedDeltaOptional(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerSignedDeltaOptional(value, idx, intValues);
    }

    private void genWriteIntegerSignedNoneOptional(int value, PrimitiveWriter writer) {
        writer.writeIntegerSignedOptional(value);
    }

    private void genWriteIntegerUnsignedCopyOptional(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedCopyOptional(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedDefaultOptional(int value, int constDefault, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedDefaultOptional(value, constDefault);
    }

    private void genWriteIntegerUnsignedIncrementOptional(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedIncrementOptional(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedConstantOptional(PrimitiveWriter writer) {
        writer.writePMapBit((byte) 1, writer);
    }

    private void genWriteIntegerUnsignedDeltaOptional(int value, int idx, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedDeltaOptional(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedNoneOptional(int value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value + 1);
    }

    private void genWriteLongUnsignedDefault(long value, long constDefault, PrimitiveWriter writer) {
        writer.writeLongUnsignedDefault(value, constDefault);
    }

    private void genWriteLongUnsignedIncrement(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongUnsignedIncrement(value, idx, longValues);
    }

    private void genWriteLongUnsignedCopy(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongUnsignedCopy(value, idx, longValues);
    }

    private void genWriteLongUnsignedDelta(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongSigned(value - longValues[idx]);
        longValues[idx] = value;
    }

    private void genWriteLongUnsignedNone(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongUnsigned(longValues[idx] = value);
    }
    
    private void genWriteLongUnsignedDefaultOptional(long value, long constDefault, PrimitiveWriter writer) {
        writer.writneLongUnsignedDefaultOptional(value, constDefault);
    }

    private void genWriteLongUnsignedIncrementOptional(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongUnsignedIncrementOptional(value, idx, longValues);
    }

    private void genWriteLongUnsignedCopyOptional(long value, int idx, PrimitiveWriter writer) {
        value++;// zero is held for null
        
        writer.writeLongUnsignedCopyOptional(value, idx, longValues);
    }

    private void genWriteLongUnsignedConstantOptional(PrimitiveWriter writer) {
        writer.writePMapBit((byte) 1, writer);
    }

    private void genWriteLongUnsignedNoneOptional(long value, PrimitiveWriter writer) {
        writer.writeLongUnsigned(value + 1);
    }

    private void genWriteLongUnsignedDeltaOptional(long value, int idx, PrimitiveWriter writer) {
        long delta = value - longValues[idx];
        writer.writeLongSigned(delta>=0 ? 1+delta : delta);
        longValues[idx] = value;
    }
    
    private void genWriteLongSignedDefault(long value, long constDefault, PrimitiveWriter writer) {
        writer.writeLongSignedDefault(value, constDefault);
    }

    private void genWriteLongSignedIncrement(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongSignedIncrement(value,  longValues[idx]);
        longValues[idx] = value;
    }

    private void genWriteLongSignedCopy(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongSignedCopy(value, idx, longValues);
    }

    private void genWriteLongSignedNone(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongSigned(longValues[idx] = value);
    }

    private void genWriteLongSignedDelta(long value, int idx, PrimitiveWriter writer) {
        writer.writeLongSigned(value - longValues[idx]);
        longValues[idx] = value;
    }
    
    private void genWriteLongSignedOptional(long value, PrimitiveWriter writer) {
        writer.writeLongSignedOptional(value);
    }

    private void genWriteLongSignedDeltaOptional(long value, int idx, long delta, PrimitiveWriter writer) {
        writer.writeLongSigned(((delta + (delta >>> 63)) + 1));
        longValues[idx] = value;
    }

    private void genWriteLongSignedConstantOptional(PrimitiveWriter writer) {
        writer.writePMapBit((byte) 1, writer);
    }
    
    //TODO: A, Add gen copy methods and insert them before teh gen write/read calls.

    private void genWriteLongSignedCopyOptional(long value, int idx, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;
        }
        writer.writeLongSignedCopy(value, idx, longValues);
    }

    private void genWriteLongSignedIncrementOptional(long value, int idx, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;
        }
        writer.writeLongSignedIncrementOptional(value, longValues[idx]);
        longValues[idx] = value;
    }

    private void genWriteLongSignedDefaultOptional(long value, long constDefault, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;// room for null
        }
        writer.writeLongSignedDefault(value, constDefault);
    }

    public static void writeNullInt(int token, PrimitiveWriter writer, int[] dictionary, int idx) {
        //TODO: A, must have genWrite methods for these.
        
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta (both do not use pmap)
                dictionary[idx] = 0;
                writer.writeNull(); // no pmap, yes change to last
                                             // value
            } else {
                // Copy and Increment
    
                if (dictionary[idx] == 0) { // stored value was null;
                    writer.writePMapBit((byte) 0, writer);
                } else {
                    dictionary[idx] = 0;
                    writer.writePMapBit((byte) 1, writer);
                    writer.writeNull();
                } // yes pmap, yes change to last value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                // const optional
                writer.writePMapBit((byte) 0, writer); // pmap only
            } else {
                // default
    
                if (dictionary[idx] == 0) { // stored value was null;
                    writer.writePMapBit((byte) 0, writer);
                } else {
                    writer.writePMapBit((byte) 1, writer);
                    writer.writeNull();
                } // yes pmap, no change to last value
            }
        }
    }

    public static void writeNullLong(int token, int idx, PrimitiveWriter writer, long[] dictionary) {
      //TODO: A, must have genWrite methods for these.
        
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta (both do not use pmap)
                dictionary[idx] = 0;
                writer.writeNull(); // no pmap, yes change to last value
            } else {
                // Copy and Increment
    
                if (dictionary[idx] == 0) { // stored value was null;
                    writer.writePMapBit((byte) 0, writer);
                } else {
                    dictionary[idx] = 0;
                    writer.writePMapBit((byte) 1, writer);
                    writer.writeNull();
                } // yes pmap, yes change to last value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                    assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                    writer.writePMapBit((byte) 0, writer); // pmap only
            } else {
                // default
                if (dictionary[idx] == 0) { // stored value
                                                              // was null;
                    writer.writePMapBit((byte) 0, writer);
                } else {
                    writer.writePMapBit((byte) 1, writer);
                    writer.writeNull();
                } // primitiveWriter pmap, no change to last value
            }
        }
    }

    public static void writeNullText(int token, int idx, PrimitiveWriter writer, TextHeap textHeap) {
      //TODO: A, must have genWrite methods for these.
        
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta and Tail
                writer.writeNull();
                textHeap.setNull(idx); // no pmap, yes change to last value
            } else {
                // Copy and Increment
                
                if (textHeap.isNull(idx)) { // stored value was null;
                    writer.writePMapBit((byte) 0, writer);
                } else {
                    writer.writePMapBit((byte) 1, writer);
                    writer.writeNull();
                    textHeap.setNull(idx);
                } // yes pmap, yes change to last
                                              // value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                writer.writePMapBit((byte) 0, writer); // pmap only
            } else {
                // default
                if (textHeap.isNull(idx)) { // stored value was null;
                    writer.writePMapBit((byte) 0, writer);
                } else {
                    writer.writePMapBit((byte) 1, writer);
                    writer.writeNull();
                } // yes pmap, no change to last value
            }
        }
    }
    
    protected void genWriteDictionaryBytesReset(int idx) {
        byteHeap.setNull(idx);
    }

    protected void genWriteDictionaryTextReset(int idx) {
        textHeap.reset(idx);
    }

    protected void genWriteDictionaryLongReset(int idx) {
        longValues[idx] = longInit[idx];
    }

    protected void genWriteDictionaryIntegerReset(int idx) {
        intValues[idx] = intInit[idx];
    }
    
    protected void genWriteClosePMap(PrimitiveWriter writer) {
        writer.closePMap();
    }

    protected void genWriteCloseTemplatePMap(PrimitiveWriter writer) {
        writer.closePMap();
        // must always pop because open will always push
        templateStackHead--;
    }

    protected void genWriteCloseTemplate() {
        // must always pop because open will always push
        templateStackHead--;
    }
    
    protected void genWriteOpenTemplate(int templateId) {
        // done here for safety to ensure it is always done at group open.
        pushTemplate(templateId);
    }

    protected void genWriteOpenTemplatePMap(int templateId, int pmapSize, PrimitiveWriter writer) {
        writer.openPMap(pmapSize);
        // done here for safety to ensure it is always done at group open.
        pushTemplate(templateId);
    }
    
    protected void genWriteOpenGroup(int pmapSize, PrimitiveWriter writer) {
        writer.openPMap(pmapSize);
    }

    public static void writeNullBytes(int token, PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask) {
    	
    	if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
    		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
    			//None and Delta and Tail
    			writer.writeNull();
                byteHeap.setNull(token & instanceMask);              //no pmap, yes change to last value
    		} else {
    			//Copy and Increment
    			int idx = token & instanceMask;
                
                if (byteHeap.isNull(idx)) { //stored value was null;
                	writer.writePMapBit((byte)0, writer);
                } else {
                	writer.writePMapBit((byte)1, writer);
                	writer.writeNull();
                	byteHeap.setNull(idx);
                }  //yes pmap, yes change to last value	
    		}
    	} else {
    		if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
    			if (0==(token&(1<<TokenBuilder.SHIFT_TYPE))) {
    				//const
    				writer.writeNull();                 //no pmap,  no change to last value  
    			} else {
    				//const optional
    				writer.writePMapBit((byte)0, writer);       //pmap only
    			}			
    		} else {	
    			//default
    			if (byteHeap.isNull(token & instanceMask)) { //stored value was null;
                	writer.writePMapBit((byte)0, writer);
                } else {
                	writer.writePMapBit((byte)1, writer);
                	writer.writeNull();
                }  //yes pmap,  no change to last value
    		}	
    	}
    	
    }
}
