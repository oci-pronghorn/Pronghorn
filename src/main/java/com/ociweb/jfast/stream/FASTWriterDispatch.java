//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldWriterBytes;
import com.ociweb.jfast.field.FieldWriterText;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public final class FASTWriterDispatch {

    private int templateStackHead = 0;
    private final int[] templateStack;

    private final PrimitiveWriter writer;

    private final FieldWriterText writerChar;
    private final FieldWriterBytes writerBytes;

    public final int[] intValues;
    private final int[] intInit;
    public final int intInstanceMask;
    
    public final long[] longValues;
    private final long[] longInit;
    public final int longInstanceMask;
    
    final int nonTemplatePMapSize;

    private int readFromIdx = -1;

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

    public FASTWriterDispatch(PrimitiveWriter writer, DictionaryFactory dcr, int maxTemplates, int maxCharSize,
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

        this.writerChar = new FieldWriterText(writer, textHeap);
        this.writerBytes = new FieldWriterBytes(writer, byteHeap);

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
                
                FASTWriterDispatch.writeNullInt(token, writer, intValues, idx);
            } else {
                // long
                int idx = token & longInstanceMask;
                
                FASTWriterDispatch.writeNullLong(token, idx, writer, longValues);
            }
        } else {
            // text decimal bytes
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // text
                int idx = token & writerChar.INSTANCE_MASK;
                
                FASTWriterDispatch.writeNullText(token, idx, writer, textHeap);
            } else {
                // decimal bytes
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // decimal
                    int idx = token & intInstanceMask;
                    
                    //TODO: A, must implement null for decimals
                    FASTWriterDispatch.writeNullInt(token, writer,intValues, idx); 

                    int idx1 = token & longInstanceMask;
                    
                    FASTWriterDispatch.writeNullLong(token, idx1, writer, longValues);
                } else {
                    // byte
                    writerBytes.writeNull(token);
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

    private void acceptLongSignedOptional(int token, long value) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongSignedOptional(value);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    long delta = value - longValues[idx];
                    
                    genWriteLongSignedDeltaOptional(value, idx, delta);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";
                
                genWriteLongSignedConstantOptional();
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & longInstanceMask;
                    
                    genWriteLongSignedCopyOptional(value, idx);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    genWriteLongSignedIncrementOptional(value, idx);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongSignedDefaultOptional(value, constDefault);
            }
        }
    }



    private void acceptLongSigned(int token, long value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongSignedNone(value, idx);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    genWriteLongSignedDelta(value, idx);
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
                    
                    genWriteLongSignedCopy(value, idx);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    
                    genWriteLongSignedIncrement(value, idx);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongSignedDefault(value, constDefault);
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
                    genWriteLongUnsignedNoneOptional(value);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedDeltaOptional(value, idx);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";
                genWriteLongUnsignedConstantOptional();
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedCopyOptional(value, idx);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedIncrementOptional(value, idx);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefaultOptional(value, constDefault);
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

                    genWriteLongUnsignedNone(value, idx);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedDelta(value, idx);
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
                    
                    genWriteLongUnsignedCopy(value, idx);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
                    
                    genWriteLongUnsignedIncrement(value, idx);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefault(value, constDefault);
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

    private void acceptIntegerSigned(int token, int value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedNone(value, idx);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedDelta(value, idx);
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

                    genWriteIntegerSignedCopy(value, idx);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedIncrement(value, idx);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerSignedDefault(value, idx, constDefault);
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

                    genWriteIntegerUnsignedNone(value, idx);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = (token & intInstanceMask);

                    genWriteIntegerUnsignedDelta(value, idx);
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
                    genWriteIntegerUnsignedCopy(value, idx);
                } else {
                    // increment
                    int idx = token & intInstanceMask;
                    genWriteIntegerUnsignedIncrement(value, idx);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerUnsignedDefault(value, constDefault);
            }
        }
    }


    private void acceptIntegerSignedOptional(int token, int value) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteIntegerSignedNoneOptional(value);
                } else {
                    // delta
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedDeltaOptional(value, idx);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value from the template may be sent";
                genWriteIntegerSignedConstantOptional();
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedCopyOptional(value, idx);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedIncrementOptional(value, idx);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerSignedDefaultOptional(value, idx, constDefault);
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
                    genWriteIntegerUnsignedNoneOptional(value);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & intInstanceMask;

                    genWriteIntegerUnsignedDeltaOptional(value, idx);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value from the template may be sent";
                genWriteIntegerUnsignedConstantOptional();
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;
                    genWriteIntegerUnsignedCopyOptional(value, idx);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    genWriteIntegerUnsignedIncrementOptional(value, idx);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerUnsignedDefaultOptional(value, constDefault);
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
                    genWriteUTFTextNoneOptional(value);
                } else {
                    // tail
                    genWriteUTFTextTailOptional(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteUTFTextConstantOptional(token);
                } else {
                    // delta
                    genWriteUTFTextDeltaOptional(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteUTFTextCopyOptional(token, value);
            } else {
                // default
                genWriteUTFTextDefaultOptional(token, value);
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
                    genWriteUTFTextNone(value);
                } else {
                    // tail
                    genWriteUTFTextTail(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteUTFTextConstant(token);
                } else {
                    // delta
                    genWriteUTFTextDelta(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteUTFTextCopy(token, value);
            } else {
                // default
                genWriteUTFTextDefault(token, value);
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
                        genWriteNull();
                    } else {
                        genWriteTextNone(value);
                    }
                } else {
                    // tail
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Tail)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    genWriteTextTailOptional(token, value);
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
                    genWriteTextDeltaOptional(token, value);

                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Copy)) : "Found "
                        + TokenBuilder.tokenToString(token);
                genWriteTextCopyOptional(token, value);

            } else {
                // default
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Default)) : "Found "
                        + TokenBuilder.tokenToString(token);
                genWriteTextDefaultOptional(token, value);

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
                    genWriteTextNone(value);
                } else {
                    // tail
                    genWriteTextTail(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextConstant2(token);
                } else {
                    // delta
                    genWriteTextDelta(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextCopy(token, value);
            } else {
                // default
                genWriteTextDefault(token, value);
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
                    genWriteTextUTFNoneOptional(value, offset, length);

                } else {
                    // tail
                    genWriteTextUTFTailOptional(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextUTFConstantOptional(token);
                } else {
                    // delta
                    genWriteTextUTFDeltaOptional(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextUTFCopyOptional(token, value, offset, length);
            } else {
                // default
                genWriteTextUTFDefaultOptional(token, value, offset, length);
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
                    genWriteTextUTFNone(value, offset, length);

                } else {
                    // tail
                    genWriteTextUTFTail(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextUTFConstant(token);
                } else {
                    // delta
                    genWriteTextUTFDelta(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextUTFCopy(token, value, offset, length);
            } else {
                // default
                genWriteTextUTFDefault(token, value, offset, length);
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
                    genWriteTextTailOptional(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextConstantOptional(token);
                } else {
                    // delta
                    genWriteTextDeltaOptional(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextCopyOptional(token, value, offset, length);
            } else {
                // default
                genWriteTextDefaultOptional(token, value, offset, length);
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
                    genWriteTextTail(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextConstant(token);
                } else {
                    // delta
                    genWriteTextDelta(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextCopy(token, value, offset, length);
            } else {
                // default
                genWriteTextDefault(token, value, offset, length);
            }
        }
    }



    public void openGroup(int token, int pmapSize) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 == (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            writer.openPMap(pmapSize);
        }

    }

    public void openGroup(int token, int templateId, int pmapSize) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        if (pmapSize > 0) {
            writer.openPMap(pmapSize);
        }
        // done here for safety to ensure it is always done at group open.
        pushTemplate(templateId);
    }

    // must happen just before Group so the Group in question must always have
    // an outer group.
    private void pushTemplate(int templateId) {
        int top = templateStack[templateStackHead];
        if (top == templateId) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(templateId);
            top = templateId;
        }

        templateStack[templateStackHead++] = top;
    }

    public void closeGroup(int token) {
        assert (token < 0);
        assert (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            writer.closePMap();
        }

        if (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) {
            // must always pop because open will always push
            templateStackHead--;
        }

    }

    public void flush() {
        writer.flush();
    }

    public void reset() {

        dictionaryFactory.reset(intValues);
        dictionaryFactory.reset(longValues);
        dictionaryFactory.reset(textHeap);
        writerBytes.reset(dictionaryFactory);
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
                        long mantissa = FASTRingBufferReader.readLong(queue, fieldPos + 1);//TODO: A, writer must break these into two
                        
                        
                        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                            writeExponent(token, exponent);
                            
                            //NOTE: moving forward one to get second token for decimals
                            token = fullScript[++activeScriptCursor];
                            
                            writeMantissa(token, mantissa);
                        } else {
                            if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT==exponent) {
                            	int idx = token & intInstanceMask; 
                                
                                FASTWriterDispatch.writeNullInt(token, writer, intValues, idx); //needed for decimal.
                            } else {
                            	writeExponentOptional(token, exponent);
                            }
                            
                            //NOTE: moving forward one to get second token for decimals
                            token = fullScript[++activeScriptCursor];
                            
                            if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG==mantissa) {
                            	int idx = token & longInstanceMask; 
                                
                                FASTWriterDispatch.writeNullLong(token, idx, writer, longValues); 
                            } else {
                            	writeMantissaOptional(token, mantissa);
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
                                        intValues[idx] = intInit[idx];
                                    }
                                } else {
                                    // long
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        longValues[idx] = longInit[idx];
                                    }
                                }
                            } else {
                                if (0 == (idx & 4)) {
                                    // text
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        if (null!=textHeap) {
                                            textHeap.reset(idx);
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
                                                byteHeap.setNull(idx);
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

            long absPos = writer.totalWritten() + writer.bytesReadyToWrite();
            // TODO: Z, this position is never right because it is changed by
            // the pmap length which gets trimmed.

            observer.tokenItem(absPos, token, cursor, value);
        }

        return true;
    }

    public void dispatchPreable(byte[] preambleData) {
        writer.writeByteArrayData(preambleData, 0, preambleData.length);
    }

    public void openMessage(int pmapMaxSize, int templateId) {

        writer.openPMap(pmapMaxSize);
        writer.writePMapBit((byte) 1);
        writer.closePMap();// TODO: A, this needs to be close but not sure this
                           // is the right location.
        writer.writeIntegerUnsigned(templateId);

    }

    public void writeMantissaOptional(int token, long mantissa) {
    
        // oppMaint
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;
    
                    writer.writeLongSigned(longValues[idx] = 1 + mantissa);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
    
                    long delta = mantissa - longValues[idx];
                    writer.writeLongSigned(((delta + (delta >>> 63)) + 1));
                    longValues[idx] = mantissa;
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == mantissa) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }
    
        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & longInstanceMask;
    
                    if (mantissa >= 0) {
                        mantissa++;
                    }
                    writer.writeLongSignedCopy2(mantissa, idx, longValues);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
    
                    if (mantissa >= 0) {
                        mantissa++;
                    }
                    writer.writeLongSignedIncrementOptional2(mantissa,
                            longValues[idx]);
                    longValues[idx] = mantissa;
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
    
                if (mantissa >= 0) {
                    mantissa++;// room for null
                }
                genWriteLongSignedDefault(mantissa, constDefault);
            }
        }
    }

    public void writeExponentOptional(int token, int exponent) {
        // oppExp
        if (0 == (token & (1 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
            // none, constant, delta
            if (0 == (token & (2 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                // none, delta
                if (0 == (token & (4 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                    // none
                    int idx = token & intInstanceMask; 
    
                    writer.writeIntegerSigned(intValues[idx] = exponent >= 0 ? 1 + exponent
                            : exponent);
                } else {
                    // delta
                    int idx = token & intInstanceMask;
    
                    writer.writeIntegerSignedDeltaOptional(exponent, idx, intValues);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == exponent) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }
    
        } else {
            // copy, default, increment
            if (0 == (token & (2 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                // copy, increment
                if (0 == (token & (4 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                    // copy
                    int idx = token & intInstanceMask;
    
                    writer.writeIntegerSignedCopyOptional(exponent, idx, intValues);
                } else {
                    // increment
                    int idx = token & intInstanceMask;
    
                    writer.writeIntegerSignedIncrementOptional(exponent, idx, intValues);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];
    
                writer.writeIntegerSignedDefaultOptional(exponent, idx, constDefault);
            }
        }
    }

    public void writeMantissa(int token, long mantissa) {
        // oppMaint
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask; 
    
                    genWriteLongSignedNone(mantissa, idx);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & longInstanceMask;
    
                    writer.writeLongSigned(mantissa - longValues[idx]);
                    longValues[idx] = mantissa;
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == mantissa) : "Only the constant value from the template may be sent";
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
    
                    writer.writeLongSignedCopy2(mantissa, idx, longValues);
                } else {
                    // increment
                    int idx = token & longInstanceMask;
    
                    genWriteLongSignedIncrement(mantissa, idx);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
    
                genWriteLongSignedDefault(mantissa, constDefault);
            }
        }
    }

    public void writeExponent(int token, int exponent) {
        // oppExp
        if (0 == (token & (1 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
            // none, constant, delta
            if (0 == (token & (2 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                // none, delta
                if (0 == (token & (4 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                    // none
                    int idx = token & intInstanceMask;
    
                    writer.writeIntegerSigned(intValues[idx] = exponent);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & intInstanceMask;
    
                    genWriteIntegerSignedDelta(exponent, idx);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == exponent) : "Only the constant value "
                        + intValues[token & intInstanceMask]
                        + " from the template may be sent";
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }
    
        } else {
            // copy, default, increment
            if (0 == (token & (2 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                // copy, increment
                if (0 == (token & (4 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                    // copy
                    int idx = token & intInstanceMask;
    
                    writer.writeIntegerSignedCopy(exponent, idx, intValues);
                } else {
                    // increment
                    int idx = token & intInstanceMask;
    
                    writer.writeIntegerSignedIncrement(exponent, idx, intValues);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];
    
                writer.writeIntegerSignedDefault(exponent, idx, constDefault);
            }
        }
    }
    
    private void genWriteUTFTextDefaultOptional(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (null == value) {
            if (writerChar.heap.isNull(idx | FieldWriterText.INIT_VALUE_MASK)) {
                writer.writePMapBit((byte) 0);
            } else {
                writer.writePMapBit((byte) 1);
                writer.writeNull();
            }
        } else {
            if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value)) {
                writer.writePMapBit((byte) 0);
            } else {
                writer.writePMapBit((byte) 1);
                writer.writeIntegerUnsigned(value.length() + 1);
                writer.writeTextUTF(value);
            }
        }
    }

    private void genWriteUTFTextCopyOptional(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx, value)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(value.length() + 1);
            writer.writeTextUTF(value);
            writerChar.heap.set(idx, value, 0, value.length());
        }
    }

    private void genWriteUTFTextDeltaOptional(int token, CharSequence value) {
        writerChar.writeUTF8DeltaOptional(token, value);
    }

    private void genWriteUTFTextConstantOptional(int token) {
        writerChar.writeUTF8ConstantOptional(token);
    }

    private void genWriteUTFTextTailOptional(int token, CharSequence value) {
        writerChar.writeUTF8TailOptional(token, value);
    }

    private void genWriteUTFTextNoneOptional(CharSequence value) {
        writer.writeIntegerUnsigned(value.length() + 1);
        writer.writeTextUTF(value);
    }
    
    private void genWriteUTFTextDefault(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(value.length());
            writer.writeTextUTF(value);
        }
    }

    private void genWriteUTFTextCopy(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        // System.err.println("AA");
        if (writerChar.heap.equals(idx, value)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(value.length());
            writer.writeTextUTF(value);
            writerChar.heap.set(idx, value, 0, value.length());
        }
    }

    private void genWriteUTFTextDelta(int token, CharSequence value) {
        writerChar.writeUTF8Delta(token, value);
    }

    private void genWriteUTFTextConstant(int token) {
    }

    private void genWriteUTFTextTail(int token, CharSequence value) {
        writerChar.writeUTF8Tail(token, value);
    }

    private void genWriteUTFTextNone(CharSequence value) {
        writer.writeIntegerUnsigned(value.length());
        writer.writeTextUTF(value);
    }

    private void genWriteTextDefaultOptional(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (null == value) {
            if (writerChar.heap.isNull(idx | FieldWriterText.INIT_VALUE_MASK)) {
                writer.writePMapBit((byte) 0);
            } else {
                writer.writePMapBit((byte) 1);
                writer.writeNull();
            }
        } else {
            if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value)) {
                writer.writePMapBit((byte) 0);
            } else {
                writer.writePMapBit((byte) 1);
                writer.writeTextASCII(value);
            }
        }
    }

    private void genWriteTextCopyOptional(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (null == value) {
            if (writerChar.heap.isNull(idx)) {
                writer.writePMapBit((byte) 0);
            } else {
                writer.writePMapBit((byte) 1);
                writer.writeNull();
            }
        } else {
            if (writerChar.heap.equals(idx, value)) {
                writer.writePMapBit((byte) 0);
            } else {
                writer.writePMapBit((byte) 1);
                writer.writeTextASCII(value);
                writerChar.heap.set(idx, value, 0, value.length());
            }
        }
    }

    private void genWriteTextDeltaOptional(int token, CharSequence value) {
        writerChar.writeASCIIDeltaOptional(token, value);
    }

    private void genWriteTextTailOptional(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        int headCount = writerChar.heap.countHeadMatch(idx, value);
        int trimTail = writerChar.heap.length(idx) - headCount;
        
        writer.writeIntegerUnsigned(trimTail + 1);
        writeASCIITail(idx, headCount, value, trimTail);
    }

    private void writeASCIITail(int idx, int headCount, CharSequence value, int trimTail) {
        writer.writeTextASCIIAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }
    
    private void genWriteNull() {
        writer.writeNull();
    }
    
    private void genWriteTextDefault(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeTextASCII(value);
        }
    }

    private void genWriteTextCopy(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx, value)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            // System.err.println("char seq length:"+value.length());
            writer.writeTextASCII(value);
            writerChar.heap.set(idx, value, 0, value.length());
        }
    }

    private void genWriteTextDelta(int token, CharSequence value) {
        writerChar.writeASCIIDelta(token, value);
    }

    private void genWriteTextConstant2(int token) {
    }

    private void genWriteTextTail(int token, CharSequence value) {
        int idx = token & writerChar.INSTANCE_MASK;
        int headCount = writerChar.heap.countHeadMatch(idx, value);
        int trimTail = writerChar.heap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail);
        writeASCIITail(idx, headCount, value, trimTail);
    }

    private void genWriteTextNone(CharSequence value) {
        writer.writeTextASCII(value);
    }
    
    private void genWriteTextUTFDefaultOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(length + 1);
            writer.writeTextUTF(value, offset, length);
        }
    }

    private void genWriteTextUTFCopyOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(length + 1);
            writer.writeTextUTF(value, offset, length);
            writerChar.heap.set(idx, value, offset, length);
        }
    }

    private void genWriteTextUTFDeltaOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = writerChar.heap.countHeadMatch(idx, value, offset, length);
        int tailCount = writerChar.heap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            writeUTF8Tail(idx, headCount, value, offset, length, 1);
        } else {
            writeUTF8Head(idx, tailCount, value, offset, length, 1);
        }
    }

    private void genWriteTextUTFConstantOptional(int token) {
        writerChar.writeUTF8ConstantOptional(token);
    }

    private void genWriteTextUTFTailOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        writeUTF8Tail(idx, writerChar.heap.countHeadMatch(idx, value, offset, length), value, offset, length, 1);
    }

    private void genWriteTextUTFNoneOptional(char[] value, int offset, int length) {
        writer.writeIntegerUnsigned(length + 1);
        writer.writeTextUTF(value, offset, length);
    }

    private void genWriteTextUTFDefault(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTF(value, offset, length);
        }
    }

    private void genWriteTextUTFCopy(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTF(value, offset, length);
            writerChar.heap.set(idx, value, offset, length);
        }
    }

    private void genWriteTextUTFDelta(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = writerChar.heap.countHeadMatch(idx, value, offset, length);
        int tailCount = writerChar.heap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            writeUTF8Tail(idx, headCount, value, offset + headCount, length, 0);
        } else {
            writeUTF8Head(idx, tailCount, value, offset, length, 0);
        }
    }

    private void genWriteTextUTFConstant(int token) {
    }

    private void genWriteTextUTFTail(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        writeUTF8Tail(idx, writerChar.heap.countHeadMatch(idx, value, offset, length), value, offset, length, 0);
    }

    
    private void writeUTF8Tail(int idx, int headCount, char[] value, int offset, int length, final int optional) {
        int trimTail = textHeap.length(idx) - headCount;
        int valueSend = length - headCount;
        int startAfter = offset + headCount;
        textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        
        writer.writeIntegerUnsigned(trimTail + optional);
        writer.writeIntegerUnsigned(valueSend);
        writer.writeTextUTF(value, startAfter, valueSend);
        
    }

    private void writeUTF8Head(int idx, int tailCount, char[] value, int offset, int length, int opt) {

        // replace head, tail matches to tailCount
        int trimHead = textHeap.length(idx) - tailCount;
        writer.writeIntegerSigned(trimHead == 0 ? opt : -trimHead);

        int len = length - tailCount;
        writer.writeIntegerUnsigned(len);
        writer.writeTextUTF(value, offset, len);

        textHeap.appendHead(idx, trimHead, value, offset, len);
    }
    
    private void genWriteTextUTFNone(char[] value, int offset, int length) {
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTF(value, offset, length);
    }
    
    private void genWriteTextDefaultOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeTextASCII(value, offset, length);
        }
    }

    private void genWriteTextCopyOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeTextASCII(value, offset, length);
            writerChar.heap.set(idx, value, offset, length);
        }
    }

    private void genWriteTextDeltaOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = writerChar.heap.countHeadMatch(idx, value, offset, length);
        int tailCount = writerChar.heap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = writerChar.heap.length(idx) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            writer.writeIntegerSigned(trimTail + 1); // cut off these from tail,
                                                     // also add 1 because this
                                                     // is optional
        
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            writeASCIITail(idx, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = writerChar.heap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 1 : -trimHead);
        
            int len = length - tailCount;
            writer.writeTextASCII(value, offset, len);
            textHeap.appendHead(idx, trimHead, value, offset, len);
        
        }
    }
    
    private void writeASCIITail(int idx, int trimTail, char[] value, int valueStart, int valueSend) {
        writer.writeTextASCII(value, valueStart, valueSend);
        textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
    }

    private void genWriteTextConstantOptional(int token) {
        writer.writePMapBit((byte) 1);
        // the writeNull will take care of the rest.
    }

    private void genWriteTextTailOptional(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        int headCount = writerChar.heap.countHeadMatch(idx, value, offset, length);
        int trimTail = writerChar.heap.length(idx) - headCount; // head count is total that
                                                     // match from head.
        
        writer.writeIntegerUnsigned(trimTail + 1); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        writeASCIITail(idx, trimTail, value, valueStart, valueSend);
    }

    private void genWriteTextNoneOptional(char[] value, int offset, int length) {
        writer.writeTextASCII(value, offset, length);
    }
    
    private void genWriteTextDefault(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx | FieldWriterText.INIT_VALUE_MASK, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeTextASCII(value, offset, length);
        }
    }

    private void genWriteTextCopy(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        if (writerChar.heap.equals(idx, value, offset, length)) {
            writer.writePMapBit((byte) 0);
        } else {
            writer.writePMapBit((byte) 1);
            writer.writeTextASCII(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    private void genWriteTextDelta(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = writerChar.heap.countHeadMatch(idx, value, offset, length);
        int tailCount = writerChar.heap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = writerChar.heap.length(idx) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            writer.writeIntegerSigned(trimTail); // cut off these from tail
        
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            writeASCIITail(idx, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = writerChar.heap.length(idx) - tailCount;
            writer.writeIntegerUnsigned(-trimHead);
        
            int len = length - tailCount;
            writer.writeTextASCII(value, offset, len);
        
            writerChar.heap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    private void genWriteTextConstant(int token) {
    }

    private void genWriteTextTail(int token, char[] value, int offset, int length) {
        int idx = token & writerChar.INSTANCE_MASK;
        int headCount = writerChar.heap.countHeadMatch(idx, value, offset, length);
        int trimTail = writerChar.heap.length(idx) - headCount; // head count is total that
                                                     // match from head.
        writer.writeIntegerUnsigned(trimTail); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        writeASCIITail(idx, trimTail, value, valueStart, valueSend);
    }

    private void genWriteTextNone(char[] value, int offset, int length) {
        writer.writeTextASCII(value, offset, length);
    }
    
    private void genWriterBytesDefaultOptional(int token, ByteBuffer value) {
        writerBytes.writeBytesDefaultOptional(token, value);
    }

    private void genWriterBytesCopyOptional(int token, ByteBuffer value) {
        writerBytes.writeBytesCopyOptional(token, value);
    }

    private void genWriterBytesDeltaOptional(int token, ByteBuffer value) {
        writerBytes.writeBytesDeltaOptional(token, value);
    }

    private void genWriterBytesTailOptional(int token, ByteBuffer value) {
        writerBytes.writeBytesTailOptional(token, value);
    }

    private void genWriterBytesNoneOptional(ByteBuffer value) {
        writerBytes.writeBytesOptional(value);
    }

    private void genWriteBytesDefault(int token, ByteBuffer value) {
        writerBytes.writeBytesDefault(token, value);
    }

    private void genWriteBytesCopy(int token, ByteBuffer value) {
        writerBytes.writeBytesCopy(token, value);
    }

    private void genWriteBytesDelta(int token, ByteBuffer value) {
        writerBytes.writeBytesDelta(token, value);
    }

    private void genWriteBytesConstant2(int token) {
        writerBytes.writeBytesConstant(token);
    }

    private void genWriteBytesTail(int token, ByteBuffer value) {
        writerBytes.writeBytesTail(token, value);
    }

    private void genWriteBytesNone(ByteBuffer value) {
        writerBytes.writeBytes(value);
    }
    
    private void genWriteBytesDefault(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesDefault(token, value, offset, length);
    }

    private void genWriteBytesCopy(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesCopy(token, value, offset, length);
    }

    private void genWriteBytesDelta(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesDelta(token, value, offset, length);
    }

    private void genWriteBytesConstant(int token) {
        writerBytes.writeBytesConstant(token);
    }

    private void genWriteBytesTail(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesTail(token, value, offset, length);
    }

    private void genWriteBytesNone(byte[] value, int offset, int length) {
        writerBytes.writeBytes(value, offset, length);
    }
    
    private void genWriteBytesDefaultOptional(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesDefaultOptional(token, value, offset, length);
    }

    private void genWriteBytesCopyOptional(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesCopyOptional(token, value, offset, length);
    }

    private void genWriteBytesDeltaOptional(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesDeltaOptional(token, value, offset, length);
    }

    private void genWriteBytesConstantOptional(int token) {
        writerBytes.writeBytesConstantOptional(token);
    }

    private void genWriteBytesTailOptional(int token, byte[] value, int offset, int length) {
        writerBytes.writeBytesTailOptional(token, value, offset, length);
    }

    private void genWriteBytesNoneOptional(byte[] value, int offset, int length) {
        writerBytes.writeBytesOptional(value, offset, length);
    }
    
    private void genWriteIntegerSignedDefault(int value, int idx, int constDefault) {
        writer.writeIntegerSignedDefault(value, idx, constDefault);
    }

    private void genWriteIntegerSignedIncrement(int value, int idx) {
        writer.writeIntegerSignedIncrement(value, idx, intValues);
    }

    private void genWriteIntegerSignedCopy(int value, int idx) {
        writer.writeIntegerSignedCopy(value, idx, intValues);
    }

    private void genWriteIntegerSignedDelta(int value, int idx) {
        writer.writeIntegerSignedDelta(value, idx, intValues);
    }

    private void genWriteIntegerSignedNone(int value, int idx) {
        writer.writeIntegerSigned(intValues[idx] = value);
    }
    
    private void genWriteIntegerUnsignedDefault(int value, int constDefault) {
        writer.writeIntegerUnsignedDefault(value, constDefault);
    }

    private void genWriteIntegerUnsignedIncrement(int value, int idx) {
        writer.writeIntegerUnsignedIncrement(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedCopy(int value, int idx) {
        writer.writeIntegerUnsignedCopy(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedDelta(int value, int idx) {
        writer.writeIntegerUnsignedDelta(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedNone(int value, int idx) {
        writer.writeIntegerUnsigned(intValues[idx] = value);
    }

    private void genWriteIntegerSignedDefaultOptional(int value, int idx, int constDefault) {
        writer.writeIntegerSignedDefaultOptional(value, idx, constDefault);
    }

    private void genWriteIntegerSignedIncrementOptional(int value, int idx) {
        writer.writeIntegerSignedIncrementOptional(value, idx, intValues);
    }

    private void genWriteIntegerSignedCopyOptional(int value, int idx) {
        writer.writeIntegerSignedCopyOptional(value, idx, intValues);
    }

    private void genWriteIntegerSignedConstantOptional() {
        writer.writePMapBit((byte) 1);
    }

    private void genWriteIntegerSignedDeltaOptional(int value, int idx) {
        writer.writeIntegerSignedDeltaOptional(value, idx, intValues);
    }

    private void genWriteIntegerSignedNoneOptional(int value) {
        writer.writeIntegerSignedOptional(value);
    }

    private void genWriteIntegerUnsignedCopyOptional(int value, int idx) {
        writer.writeIntegerUnsignedCopyOptional(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedDefaultOptional(int value, int constDefault) {
        writer.writeIntegerUnsignedDefaultOptional(value, constDefault);
    }

    private void genWriteIntegerUnsignedIncrementOptional(int value, int idx) {
        writer.writeIntegerUnsignedIncrementOptional(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedConstantOptional() {
        writer.writePMapBit((byte) 1);
    }

    private void genWriteIntegerUnsignedDeltaOptional(int value, int idx) {
        writer.writeIntegerUnsignedDeltaOptional(value, idx, intValues);
    }

    private void genWriteIntegerUnsignedNoneOptional(int value) {
        writer.writeIntegerUnsigned(value + 1);
    }

    private void genWriteLongUnsignedDefault(long value, long constDefault) {
        writer.writeLongUnsignedDefault2(value, constDefault);
    }

    private void genWriteLongUnsignedIncrement(long value, int idx) {
        writer.writeLongUnsignedIncrement2(value, idx, longValues);
    }

    private void genWriteLongUnsignedCopy(long value, int idx) {
        writer.writeLongUnsignedCopy(value, idx, longValues);
    }

    private void genWriteLongUnsignedDelta(long value, int idx) {
        writer.writeLongSigned(value - longValues[idx]);
        longValues[idx] = value;
    }

    private void genWriteLongUnsignedNone(long value, int idx) {
        writer.writeLongUnsigned(longValues[idx] = value);
    }
    
    private void genWriteLongUnsignedDefaultOptional(long value, long constDefault) {
        writer.writneLongUnsignedDefaultOptional2(value, constDefault);
    }

    private void genWriteLongUnsignedIncrementOptional(long value, int idx) {
        writer.writeLongUnsignedIncrementOptional2(value, idx, longValues);
    }

    private void genWriteLongUnsignedCopyOptional(long value, int idx) {
        writer.writeLongUnsignedCopyOptional(value, idx, longValues);
    }

    private void genWriteLongUnsignedConstantOptional() {
        writer.writePMapBit((byte) 1);
    }

    private void genWriteLongUnsignedNoneOptional(long value) {
        writer.writeLongUnsigned(value + 1);
    }

    private void genWriteLongUnsignedDeltaOptional(long value, int idx) {
        long delta = value - longValues[idx];
        writer.writeLongSigned(delta>=0 ? 1+delta : delta);
        longValues[idx] = value;
    }
    
    private void genWriteLongSignedDefault(long value, long constDefault) {
        writer.writeLongSignedDefault2(value, constDefault);
    }

    private void genWriteLongSignedIncrement(long value, int idx) {
        writer.writeLongSignedIncrement2(value,  longValues[idx]);
        longValues[idx] = value;
    }

    private void genWriteLongSignedCopy(long value, int idx) {
        writer.writeLongSignedCopy2(value, idx, longValues);
    }

    private void genWriteLongSignedNone(long value, int idx) {
        writer.writeLongSigned(longValues[idx] = value);
    }

    private void genWriteLongSignedDelta(long value, int idx) {
        writer.writeLongSigned(value - longValues[idx]);
        longValues[idx] = value;
    }
    
    private void genWriteLongSignedOptional(long value) {
        writer.writeLongSignedOptional(value);
    }

    private void genWriteLongSignedDeltaOptional(long value, int idx, long delta) {
        writer.writeLongSigned(((delta + (delta >>> 63)) + 1));
        longValues[idx] = value;
    }

    private void genWriteLongSignedConstantOptional() {
        writer.writePMapBit((byte) 1);
    }

    private void genWriteLongSignedCopyOptional(long value, int idx) {
        if (value >= 0) {
            value++;
        }
        writer.writeLongSignedCopy2(value, idx, longValues);
    }

    private void genWriteLongSignedIncrementOptional(long value, int idx) {
        if (value >= 0) {
            value++;
        }
        writer.writeLongSignedIncrementOptional2(value, longValues[idx]);
        longValues[idx] = value;
    }

    private void genWriteLongSignedDefaultOptional(long value, long constDefault) {
        if (value >= 0) {
            value++;// room for null
        }
        writer.writeLongSignedDefault2(value, constDefault);
    }

    public static void writeNullInt(int token, PrimitiveWriter primitiveWriter, int[] dictionary, int idx) {
        //TODO: must have genWrite methods for these.
        
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta (both do not use pmap)
                dictionary[idx] = 0;
                primitiveWriter.writeNull(); // no pmap, yes change to last
                                             // value
            } else {
                // Copy and Increment
    
                if (dictionary[idx] == 0) { // stored value was null;
                    primitiveWriter.writePMapBit((byte) 0);
                } else {
                    dictionary[idx] = 0;
                    primitiveWriter.writePMapBit((byte) 1);
                    primitiveWriter.writeNull();
                } // yes pmap, yes change to last value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                // const optional
                primitiveWriter.writePMapBit((byte) 0); // pmap only
            } else {
                // default
    
                if (dictionary[idx] == 0) { // stored value was null;
                    primitiveWriter.writePMapBit((byte) 0);
                } else {
                    primitiveWriter.writePMapBit((byte) 1);
                    primitiveWriter.writeNull();
                } // yes pmap, no change to last value
            }
        }
    }

    public static void writeNullLong(int token, int idx, PrimitiveWriter primitiveWriter, long[] dictionary) {
      //TODO: must have genWrite methods for these.
        
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta (both do not use pmap)
                dictionary[idx] = 0;
                primitiveWriter.writeNull(); // no pmap, yes change to last value
            } else {
                // Copy and Increment
    
                if (dictionary[idx] == 0) { // stored value was null;
                    primitiveWriter.writePMapBit((byte) 0);
                } else {
                    dictionary[idx] = 0;
                    primitiveWriter.writePMapBit((byte) 1);
                    primitiveWriter.writeNull();
                } // yes pmap, yes change to last value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                    assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                    primitiveWriter.writePMapBit((byte) 0); // pmap only
            } else {
                // default
                if (dictionary[idx] == 0) { // stored value
                                                              // was null;
                    primitiveWriter.writePMapBit((byte) 0);
                } else {
                    primitiveWriter.writePMapBit((byte) 1);
                    primitiveWriter.writeNull();
                } // primitiveWriter pmap, no change to last value
            }
        }
    }

    public static void writeNullText(int token, int idx, PrimitiveWriter primitiveWriter, TextHeap textHeap) {
      //TODO: must have genWrite methods for these.
        
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta and Tail
                primitiveWriter.writeNull();
                textHeap.setNull(idx); // no pmap, yes change to last value
            } else {
                // Copy and Increment
                
                if (textHeap.isNull(idx)) { // stored value was null;
                    primitiveWriter.writePMapBit((byte) 0);
                } else {
                    primitiveWriter.writePMapBit((byte) 1);
                    primitiveWriter.writeNull();
                    textHeap.setNull(idx);
                } // yes pmap, yes change to last
                                              // value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                primitiveWriter.writePMapBit((byte) 0); // pmap only
            } else {
                // default
                if (textHeap.isNull(idx)) { // stored value was null;
                    primitiveWriter.writePMapBit((byte) 0);
                } else {
                    primitiveWriter.writePMapBit((byte) 1);
                    primitiveWriter.writeNull();
                } // yes pmap, no change to last value
            }
        }
    }
}
