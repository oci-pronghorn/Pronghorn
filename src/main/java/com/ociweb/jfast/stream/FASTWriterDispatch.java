//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldWriterBytes;
import com.ociweb.jfast.field.FieldWriterLong;
import com.ociweb.jfast.field.FieldWriterText;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.StaticGlue;
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

    private final FieldWriterLong writerLong;
    private final FieldWriterText writerChar;
    private final FieldWriterBytes writerBytes;

    public final int[] exponentValues;
    public final int[] exponentInit;
    public final long[] mantissaValues;
    public final long[] mantissaInit;
    public final int mantissaMask;
    public final int exponentMask;
    
    private final int[] intValues;
    private final int[] intInit;
    private final int intInstanceMask;
    
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
    
    private TextHeap charDictionary;
    private ByteHeap byteDictionary;

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
        
        this.writerLong = new FieldWriterLong(writer, dcr.longDictionary(), dcr.longDictionary());

        this.exponentInit = dcr.decimalExponentDictionary();
        this.exponentValues = dcr.decimalExponentDictionary();
        this.mantissaInit = dcr.decimalMantissaDictionary();
        this.mantissaValues = dcr.decimalMantissaDictionary();
        this.mantissaMask= Math.min(TokenBuilder.MAX_INSTANCE, (mantissaValues.length - 1));
        this.exponentMask= Math.min(TokenBuilder.MAX_INSTANCE, (exponentValues.length - 1));
        
        
        this.charDictionary = dcr.charDictionary(maxCharSize, gapChars);
        this.byteDictionary = dcr.byteDictionary(maxBytesSize, gapBytes);

        this.writerChar = new FieldWriterText(writer, charDictionary);
        this.writerBytes = new FieldWriterBytes(writer, byteDictionary);

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
        assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))); // TODO: T, in
                                                                // testing
                                                                // assert(failOnBadArg())

        // select on type, each dictionary will need to remember the null was
        // written
        if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
            // int long
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // int
                int idx = token & intInstanceMask;
                
                StaticGlue.writeNull2(token, writer, intValues, idx);
            } else {
                // long
                int idx = token & writerLong.INSTANCE_MASK;
                
                StaticGlue.writeNull2(token, idx, writer, writerLong.dictionary);
            }
        } else {
            // text decimal bytes
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // text
                writerChar.writeNull(token);
            } else {
                // decimal bytes
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // decimal
                    int idx = token & exponentMask;
                    
                    StaticGlue.writeNull2(token, writer,exponentValues, idx); // TODO:
                                                                                                 // A,
                                                                                                 // must
                                                                                                 // implement
                                                                                                 // null
                                                                                                 // for
                                                                                                 // decimals,
                                                                                                 // this
                                                                                                 // is
                                                                                                 // not
                                                                                                 // done
                                                                                                 // yet
                    int idx1 = token & mantissaMask;
                    
                    StaticGlue.writeNull2(token, idx1, writer, mantissaValues);
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
                    writer.writeLongSignedOptional(value);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    long delta = value - writerLong.dictionary[idx];
                    writer.writeLongSigned(((delta + (delta >>> 63)) + 1));
                    writerLong.dictionary[idx] = value;
                }
            } else {
                // constant
                assert (writerLong.dictionary[token & writerLong.INSTANCE_MASK] == value) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    if (value >= 0) {
                        value++;
                    }
                    writer.writeLongSignedCopy2(value, idx, writerLong.dictionary);
                } else {
                    // increment
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    if (value >= 0) {
                        value++;
                    }
                    writer.writeLongSignedIncrementOptional2(value, writerLong.dictionary[idx]);
                    writerLong.dictionary[idx] = value;
                }
            } else {
                // default
                int idx = token & writerLong.INSTANCE_MASK;
                long constDefault = writerLong.dictionary[idx];
                
                if (value >= 0) {
                    value++;// room for null
                }
                writer.writeLongSignedDefault2(value, constDefault);
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
                    int idx = token & writerLong.INSTANCE_MASK;

                    writer.writeLongSigned(writerLong.dictionary[idx] = value);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    writer.writeLongSigned(value - writerLong.dictionary[idx]);
                    writerLong.dictionary[idx] = value;
                }
            } else {
                // constant
                assert (writerLong.dictionary[token & writerLong.INSTANCE_MASK] == value) : "Only the constant value from the template may be sent";
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
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    writer.writeLongSignedCopy2(value, idx, writerLong.dictionary);
                } else {
                    // increment
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    
                    writer.writeLongSignedIncrement2(value,  writerLong.dictionary[idx]);
                    writerLong.dictionary[idx] = value;
                }
            } else {
                // default
                int idx = token & writerLong.INSTANCE_MASK;
                long constDefault = writerLong.dictionary[idx];
                
                writer.writeLongSignedDefault2(value, constDefault);
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
                    writer.writeLongUnsigned(value + 1);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    long delta = value - writerLong.dictionary[idx];
                    writer.writeLongSigned(delta>=0 ? 1+delta : delta);
                    writerLong.dictionary[idx] = value;
                }
            } else {
                // constant
                assert (writerLong.dictionary[token & writerLong.INSTANCE_MASK] == value) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    writer.writeLongUnsignedCopyOptional(value, idx, writerLong.dictionary);
                } else {
                    // increment
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    writer.writeLongUnsignedIncrementOptional2(value, idx, writerLong.dictionary);
                }
            } else {
                // default
                int idx = token & writerLong.INSTANCE_MASK;
                long constDefault = writerLong.dictionary[idx];
                
                writer.writneLongUnsignedDefaultOptional2(value, constDefault);
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
                    int idx = token & writerLong.INSTANCE_MASK;

                    writer.writeLongUnsigned(writerLong.dictionary[idx] = value);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    writer.writeLongSigned(value - writerLong.dictionary[idx]);
                    writerLong.dictionary[idx] = value;
                }
            } else {
                // constant
                assert (writerLong.dictionary[token & writerLong.INSTANCE_MASK] == value) : "Only the constant value from the template may be sent";
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
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    writer.writeLongUnsignedCopy(value, idx, writerLong.dictionary);
                } else {
                    // increment
                    int idx = token & writerLong.INSTANCE_MASK;
                    
                    writer.writeLongUnsignedIncrement2(value, idx, writerLong.dictionary);
                }
            } else {
                // default
                int idx = token & writerLong.INSTANCE_MASK;
                long constDefault = writerLong.dictionary[idx];
                
                writer.writeLongUnsignedDefault2(value, constDefault);
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

                    writer.writeIntegerSigned(intValues[idx] = value);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & intInstanceMask;

                    writer.writeIntegerSignedDelta(value, idx, intValues);
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

                    writer.writeIntegerSignedCopy(value, idx, intValues);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    writer.writeIntegerSignedIncrement(value, idx, intValues);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                writer.writeIntegerSignedDefault(value, idx, constDefault);
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

                    writer.writeIntegerUnsigned(intValues[idx] = value);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = (token & intInstanceMask);

                    writer.writeIntegerUnsignedDelta(value, idx, intValues);
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
                    writer.writeIntegerUnsignedCopy(value, idx, intValues);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    writer.writeIntegerUnsignedIncrement(value, idx, intValues);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                writer.writeIntegerUnsignedDefault(value, constDefault);
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
                    writer.writeIntegerSignedOptional(value);
                } else {
                    // delta
                    int idx = token & intInstanceMask;

                    writer.writeIntegerSignedDeltaOptional(value, idx, intValues);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;

                    writer.writeIntegerSignedCopyOptional(value, idx, intValues);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    writer.writeIntegerSignedIncrementOptional(value, idx, intValues);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                writer.writeIntegerSignedDefaultOptional(value, idx, constDefault);
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
                    writer.writeIntegerUnsigned(value + 1);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & intInstanceMask;

                    writer.writeIntegerUnsignedDeltaOptional(value, idx, intValues);
                }
            } else {
                // constant
                assert (intValues[token & intInstanceMask] == value) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & intInstanceMask;
                    writer.writeIntegerUnsignedCopyOptional(value, idx, intValues);
                } else {
                    // increment
                    int idx = token & intInstanceMask;

                    writer.writeIntegerUnsignedIncrementOptional(value, idx, intValues);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                writer.writeIntegerUnsignedDefaultOptional(value, constDefault);
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
                    writerBytes.writeBytesOptional(value, offset, length);
                } else {
                    // tail
                    writerBytes.writeBytesTailOptional(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerBytes.writeBytesConstantOptional(token);
                } else {
                    // delta
                    writerBytes.writeBytesDeltaOptional(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerBytes.writeBytesCopyOptional(token, value, offset, length);
            } else {
                // default
                writerBytes.writeBytesDefaultOptional(token, value, offset, length);
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
                    writerBytes.writeBytes(value, offset, length);
                } else {
                    // tail
                    writerBytes.writeBytesTail(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerBytes.writeBytesConstant(token);
                } else {
                    // delta
                    writerBytes.writeBytesDelta(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerBytes.writeBytesCopy(token, value, offset, length);
            } else {
                // default
                writerBytes.writeBytesDefault(token, value, offset, length);
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
                    writerBytes.writeBytesOptional(value);
                } else {
                    // tail
                    writerBytes.writeBytesTailOptional(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerBytes.writeBytesConstantOptional(token);
                } else {
                    // delta
                    writerBytes.writeBytesDeltaOptional(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerBytes.writeBytesCopyOptional(token, value);
            } else {
                // default
                writerBytes.writeBytesDefaultOptional(token, value);
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
                    writerBytes.writeBytes(value);
                } else {
                    // tail
                    writerBytes.writeBytesTail(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerBytes.writeBytesConstant(token);
                } else {
                    // delta
                    writerBytes.writeBytesDelta(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerBytes.writeBytesCopy(token, value);
            } else {
                // default
                writerBytes.writeBytesDefault(token, value);
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
                    writerChar.writeUTF8Optional(value);
                } else {
                    // tail
                    writerChar.writeUTF8TailOptional(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerChar.writeUTF8ConstantOptional(token);
                } else {
                    // delta
                    writerChar.writeUTF8DeltaOptional(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerChar.writeUTF8CopyOptional(token, value);
            } else {
                // default
                writerChar.writeUTF8DefaultOptional(token, value);
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
                    writerChar.writeUTF8(value);
                } else {
                    // tail
                    writerChar.writeUTF8Tail(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerChar.writeUTF8Constant(token);
                } else {
                    // delta
                    writerChar.writeUTF8Delta(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerChar.writeUTF8Copy(token, value);
            } else {
                // default
                writerChar.writeUTF8Default(token, value);
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
                    writerChar.writeASCIITextOptional(value);
                } else {
                    // tail
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Tail)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    writerChar.writeASCIITailOptional(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    writerChar.writeASCIIConstantOptional(token);
                } else {
                    // delta
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Delta)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    writerChar.writeASCIIDeltaOptional(token, value);

                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Copy)) : "Found "
                        + TokenBuilder.tokenToString(token);
                writerChar.writeASCIICopyOptional(token, value);

            } else {
                // default
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Default)) : "Found "
                        + TokenBuilder.tokenToString(token);
                writerChar.writeASCIIDefaultOptional(token, value);

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
                    writerChar.writeASCII(value);
                } else {
                    // tail
                    writerChar.writeASCIITail(token, value);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerChar.writeASCIIConstant(token);
                } else {
                    // delta
                    writerChar.writeASCIIDelta(token, value);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerChar.writeASCIICopy(token, value);
            } else {
                // default
                writerChar.writeASCIIDefault(token, value);
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
                    writerChar.writeUTF8Optional(value, offset, length);

                } else {
                    // tail
                    writerChar.writeUTF8TailOptional(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerChar.writeUTF8ConstantOptional(token);
                } else {
                    // delta
                    writerChar.writeUTF8DeltaOptional(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerChar.writeUTF8CopyOptional(token, value, offset, length);
            } else {
                // default
                writerChar.writeUTF8DefaultOptional(token, value, offset, length);
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
                    writerChar.writeUTF8(value, offset, length);

                } else {
                    // tail
                    writerChar.writeUTF8Tail(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerChar.writeUTF8Constant(token);
                } else {
                    // delta
                    writerChar.writeUTF8Delta(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerChar.writeUTF8Copy(token, value, offset, length);
            } else {
                // default
                writerChar.writeUTF8Default(token, value, offset, length);
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
                    writerChar.writeASCIITextOptional(value, offset, length);
                } else {
                    // tail
                    writerChar.writeASCIITailOptional(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerChar.writeASCIIConstantOptional(token);
                } else {
                    // delta
                    writerChar.writeASCIIDeltaOptional(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerChar.writeASCIICopyOptional(token, value, offset, length);
            } else {
                // default
                writerChar.writeASCIIDefaultOptional(token, value, offset, length);
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
                    writerChar.writeASCIIText(token, value, offset, length);
                } else {
                    // tail
                    writerChar.writeASCIITail(token, value, offset, length);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    writerChar.writeASCIIConstant(token);
                } else {
                    // delta
                    writerChar.writeASCIIDelta(token, value, offset, length);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                writerChar.writeASCIICopy(token, value, offset, length);
            } else {
                // default
                writerChar.writeASCIIDefault(token, value, offset, length);
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
        dictionaryFactory.reset(writerLong.dictionary);
        dictionaryFactory.reset(exponentValues, mantissaValues);
        dictionaryFactory.reset(charDictionary);
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
                    writeInteger(token, queue.readInteger(fieldPos));
                } else {
                    writeLong(token, queue.readLong(fieldPos));
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    char[] buffer = queue.readRingCharBuffer(fieldPos);
                    int length = queue.readCharsLength(fieldPos);
                    if (length < 0) {
                        write(token);
                    } else {
                        write(token,
                                charSequence(buffer, queue.readRingCharPos(fieldPos), queue.readRingCharMask(), length));
                    }
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        

                        
                        int exponent = queue.readInteger(fieldPos);
                        long mantissa = queue.readLong(fieldPos + 1);//TODO: A, writer must break these into two
                        
                        
                        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                            writeExponent(token, exponent);
                            
                            //NOTE: moving forward one to get second token for decimals
                            token = fullScript[++activeScriptCursor];
                            
                            writeMantissa(token, mantissa);
                        } else {
                            if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT==exponent) {
                            	int idx = token & exponentMask;
                                
                                StaticGlue.writeNull2(token, writer, exponentValues, idx);
                            } else {
                            	writeExponentOptional(token, exponent);
                            }
                            
                            //NOTE: moving forward one to get second token for decimals
                            token = fullScript[++activeScriptCursor];
                            
                            if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG==mantissa) {
                            	int idx = token & mantissaMask;
                                
                                StaticGlue.writeNull2(token, idx, writer, mantissaValues);
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
                    int length = queue.readInteger(fieldPos);
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
                                        writerLong.dictionary[idx] = writerLong.init[idx];
                                    }
                                }
                            } else {
                                if (0 == (idx & 4)) {
                                    // text
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        if (null!=charDictionary) {
                                            charDictionary.reset(idx);
                                        }
                                    }
                                } else {
                                    if (0 == (idx & 2)) {
                                        // decimal
                                        while (m < limit && (idx = members[m++]) >= 0) {
                                            exponentValues[idx] = exponentInit[idx];
                                            mantissaValues[idx] = mantissaInit[idx];
                                        }
                                    } else {
                                        // bytes
                                        while (m < limit && (idx = members[m++]) >= 0) {
                                            if (null!=byteDictionary) {
                                                byteDictionary.setNull(idx);
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

    private CharSequence charSequence(char[] buffer, int pos, int mask, int length) {
        return ringCharSequence.set(buffer, pos, mask, length);
    }

    private boolean gatherWriteData(PrimitiveWriter writer, int token, int cursor, int fieldPos, FASTRingBuffer queue) {

        if (null != observer) {

            String value = "";
            int type = TokenBuilder.extractType(token);
            if (type == TypeMask.GroupLength || type == TypeMask.IntegerSigned
                    || type == TypeMask.IntegerSignedOptional || type == TypeMask.IntegerUnsigned
                    || type == TypeMask.IntegerUnsignedOptional) {

                value = "<" + queue.readInteger(fieldPos) + ">";

            } else if (type == TypeMask.Decimal || type == TypeMask.DecimalOptional) {

                value = "<e:" + queue.readInteger(fieldPos) + "m:" + queue.readLong(fieldPos + 1) + ">";

            } else if (type == TypeMask.TextASCII || type == TypeMask.TextASCIIOptional || type == TypeMask.TextUTF8
                    || type == TypeMask.TextUTF8Optional) {
                value = "<len:" + queue.readCharsLength(fieldPos) + ">";
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
                    int idx = token & mantissaMask;
    
                    writer.writeLongSigned(mantissaValues[idx] = 1 + mantissa);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & mantissaMask;
    
                    long delta = mantissa - mantissaValues[idx];
                    writer.writeLongSigned(((delta + (delta >>> 63)) + 1));
                    mantissaValues[idx] = mantissa;
                }
            } else {
                // constant
                assert (mantissaValues[token & mantissaMask] == mantissa) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }
    
        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    int idx = token & mantissaMask;
    
                    if (mantissa >= 0) {
                        mantissa++;
                    }
                    writer.writeLongSignedCopy2(mantissa, idx, mantissaValues);
                } else {
                    // increment
                    int idx = token & mantissaMask;
    
                    if (mantissa >= 0) {
                        mantissa++;
                    }
                    writer.writeLongSignedIncrementOptional2(mantissa,
                            mantissaValues[idx]);
                    mantissaValues[idx] = mantissa;
                }
            } else {
                // default
                int idx = token & mantissaMask;
                long constDefault = mantissaValues[idx];
    
                if (mantissa >= 0) {
                    mantissa++;// room for null
                }
                writer.writeLongSignedDefault2(mantissa, constDefault);
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
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSigned(exponentValues[idx] = exponent >= 0 ? 1 + exponent
                            : exponent);
                } else {
                    // delta
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSignedDeltaOptional(exponent, idx, exponentValues);
                }
            } else {
                // constant
                assert (exponentValues[token & exponentMask] == exponent) : "Only the constant value from the template may be sent";
                writer.writePMapBit((byte) 1);
                // the writeNull will take care of the rest.
            }
    
        } else {
            // copy, default, increment
            if (0 == (token & (2 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                // copy, increment
                if (0 == (token & (4 << (TokenBuilder.SHIFT_OPER + TokenBuilder.SHIFT_OPER_DECIMAL_EX)))) {
                    // copy
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSignedCopyOptional(exponent, idx, exponentValues);
                } else {
                    // increment
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSignedIncrementOptional(exponent, idx, exponentValues);
                }
            } else {
                // default
                int idx = token & exponentMask;
                int constDefault = exponentValues[idx];
    
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
                    int idx = token & mantissaMask;
    
                    writer.writeLongSigned(mantissaValues[idx] = mantissa);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & mantissaMask;
    
                    writer.writeLongSigned(mantissa - mantissaValues[idx]);
                    mantissaValues[idx] = mantissa;
                }
            } else {
                // constant
                assert (mantissaValues[token & mantissaMask] == mantissa) : "Only the constant value from the template may be sent";
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
                    int idx = token & mantissaMask;
    
                    writer.writeLongSignedCopy2(mantissa, idx, mantissaValues);
                } else {
                    // increment
                    int idx = token & mantissaMask;
    
                    writer.writeLongSignedIncrement2(mantissa,
                            mantissaValues[idx]);
                    mantissaValues[idx] = mantissa;
                }
            } else {
                // default
                int idx = token & mantissaMask;
                long constDefault = mantissaValues[idx];
    
                writer.writeLongSignedDefault2(mantissa, constDefault);
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
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSigned(exponentValues[idx] = exponent);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSignedDelta(exponent, idx, exponentValues);
                }
            } else {
                // constant
                assert (exponentValues[token & exponentMask] == exponent) : "Only the constant value "
                        + exponentValues[token & exponentMask]
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
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSignedCopy(exponent, idx, exponentValues);
                } else {
                    // increment
                    int idx = token & exponentMask;
    
                    writer.writeIntegerSignedIncrement(exponent, idx, exponentValues);
                }
            } else {
                // default
                int idx = token & exponentMask;
                int constDefault = exponentValues[idx];
    
                writer.writeIntegerSignedDefault(exponent, idx, constDefault);
            }
        }
    }

}
