//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.FASTWriterDispatchTemplates;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public class FASTWriterInterpreterDispatch extends FASTWriterDispatchTemplates implements GeneratorDriving{ 

    public FASTRingBuffer rbRingBufferLocal = new FASTRingBuffer((byte)2,(byte)2,null, null, null);
    
    protected final int[] fieldIdScript;
    protected final String[] fieldNameScript;
    
    public FASTWriterInterpreterDispatch(byte[] catBytes) {
        this(new TemplateCatalogConfig(catBytes));
    }    
    
    public FASTWriterInterpreterDispatch(final TemplateCatalogConfig catalog) {
        super(catalog, catalog.ringBuffers());
        this.fieldIdScript = catalog.fieldIdScript();
        this.fieldNameScript = catalog.fieldNameScript();
    } 
    
    
    //Constructor is very useful for adding new message or dropping messages without any modification of messages.
    public FASTWriterInterpreterDispatch(final TemplateCatalogConfig catalog, RingBuffers buffers) {
        super(catalog, buffers);
        this.fieldIdScript = catalog.fieldIdScript();
        this.fieldNameScript = catalog.fieldNameScript();
    }
    

    public void acceptLongSignedOptional(int token, long valueOfNull, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
        int target = (token & longInstanceMask);
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongSignedOptional(valueOfNull, target,  rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    genWriteLongSignedDeltaOptional(valueOfNull, target, source, rbPos, writer, longValues, rbRingBuffer);
                }
            } else {
                // constant
                genWriteLongSignedConstantOptional(valueOfNull, target, rbPos, writer, rbRingBuffer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                
                // copy, increment
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongSignedCopyOptional(target, source, valueOfNull, rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongSignedIncrementOptional(target, source, rbPos, valueOfNull, writer, longValues, rbRingBuffer);
                }
            } else {
                // default              
                
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];                
                
                genWriteLongSignedDefaultOptional(target, rbPos, valueOfNull, constDefault, writer, rbRingBuffer, longValues);
            }
        }
    }


//
//    public void acceptLongSigned(int token, long value, Object newParam, FASTRingBuffer rbRingBuffer) {
//        
//        ////    //temp solution as the ring buffer is introduce into all the APIs
//        rbRingBuffer.dump();            
//        rbRingBuffer.buffer[rbRingBuffer.mask & rbRingBuffer.workingHeadPos++] = (int) (value >>> 32);
//        rbRingBuffer.buffer[rbRingBuffer.mask & rbRingBuffer.workingHeadPos++] = (int) (value & 0xFFFFFFFF); 
//        FASTRingBuffer.unBlockMessage(rbRingBuffer);
//        int rbPos = 0;
//        
//
//        acceptLongSigned(token, rbPos, rbRingBuffer);
//
//    }

    public void acceptLongSigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongSignedNone(idx, rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    
                    genWriteLongSignedDelta(target, source, rbPos, writer, longValues, rbRingBuffer);
                }
            } else {
                // constant
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                int target = (token & longInstanceMask);
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongSignedCopy(target, source, rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongSignedIncrement(target, source, rbPos, writer, longValues, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongSignedDefault(constDefault, rbPos, writer, rbRingBuffer);
            }
        }
    }



    void acceptLongUnsignedOptional(int token, long valueOfNull, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {

        int target = token & longInstanceMask;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongUnsignedNoneOptional(valueOfNull, target, rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;

                    genWriteLongUnsignedDeltaOptional(valueOfNull, target, source, rbPos, writer, longValues, rbRingBuffer);
                }
            } else {
                // constant
                genWriteLongUnsignedConstantOptional(valueOfNull, target, rbPos, writer, rbRingBuffer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                
                // copy, increment
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongUnsignedCopyOptional(valueOfNull, target, source, rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongUnsignedIncrementOptional(valueOfNull, target, source, rbPos, writer, longValues, rbRingBuffer);
                }
            } else {
                // default               
                
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefaultOptional(valueOfNull, target, constDefault, rbPos, writer, rbRingBuffer, longValues);
            }
        }
    }


    public void acceptLongUnsigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongUnsignedNone(idx, rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    
                    genWriteLongUnsignedDelta(target, source, rbPos, writer, longValues, rbRingBuffer);
                }
            } else {
                // constant
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                int target = (token & longInstanceMask);
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongUnsignedCopy(target, source, rbPos, writer, longValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongUnsignedIncrement(target, source, rbPos, writer, longValues, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefault(constDefault, rbPos, writer, rbRingBuffer);
            }
        }
    }



    public void acceptIntegerSigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedNone(idx, rbPos, writer, intValues, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & intInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    
                    genWriteIntegerSignedDelta(target, source, rbPos, writer, intValues, rbRingBuffer);
                }
            } else {
                // constant
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = (token & intInstanceMask);
                int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteIntegerSignedCopy(target, source, rbPos, writer, intValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteIntegerSignedIncrement(target, source, rbPos, writer, intValues, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerSignedDefault(constDefault, rbPos, writer, rbRingBuffer);
            }
        }
    }

    public void acceptIntegerUnsigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & intInstanceMask;

                    genWriteIntegerUnsignedNone(idx, rbPos, writer, intValues, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & intInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerUnsignedDelta(target, source, rbPos, writer, intValues, rbRingBuffer);
                }
            } else {
                // constant
                // nothing need be sent because constant does not use pmap and
                // the template
                // on the other receiver side will inject this value from the
                // template
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = (token & intInstanceMask);
                int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteIntegerUnsignedCopy(target, source, rbPos, writer, intValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteIntegerUnsignedIncrement(target, source, rbPos, writer, intValues, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerUnsignedDefault(constDefault, rbPos, writer, rbRingBuffer);
            }
        }
    }
 
    public void acceptIntegerSignedOptional(int token, int valueOfNull, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = (token & intInstanceMask);
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteIntegerSignedNoneOptional(target, rbPos, valueOfNull, writer, intValues, rbRingBuffer);
                } else {
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerSignedDeltaOptional(target, source, rbPos, valueOfNull, writer, intValues, rbRingBuffer);
                }
            } else {
                // constant
                genWriteIntegerSignedConstantOptional(valueOfNull, rbPos, writer, rbRingBuffer);
                // the writeNull will take care of the rest.
            }

        } else {
            int target = (token & intInstanceMask);
            int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteIntegerSignedCopyOptional(target, source, rbPos, valueOfNull, writer, intValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteIntegerSignedIncrementOptional(target, source, rbPos, valueOfNull, writer, intValues, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];


                genWriteIntegerSignedDefaultOptional(source, rbPos, constDefault, valueOfNull, writer, rbRingBuffer, intValues);
            }
        }
    }

    public void acceptIntegerUnsignedOptional(int token, int valueOfNull, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = (token & intInstanceMask);
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteIntegerUnsignedNoneOptional(target, valueOfNull, rbPos, writer, rbRingBuffer, intValues);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerUnsignedDeltaOptional(target, source, rbPos, valueOfNull, writer, intValues, rbRingBuffer);
                }
            } else {
                // constant
                genWriteIntegerUnsignedConstantOptional(rbPos, valueOfNull, writer, rbRingBuffer);
                // the writeNull will take care of the rest.
            }

        } else {
            int target = (token & intInstanceMask);
            int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteIntegerUnsignedCopyOptional(target, source, rbPos, valueOfNull, writer, intValues, rbRingBuffer);
                } else {
                    // increment
                    genWriteIntegerUnsignedIncrementOptional(target, source, rbPos, valueOfNull, writer, intValues, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerUnsignedDefaultOptional(source, rbPos, valueOfNull, constDefault, writer, rbRingBuffer, intValues);
            }
        }
    }

    public void acceptByteArrayOptional(int token, PrimitiveWriter writer, LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                int idx = token & instanceBytesMask;
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNoneOptional(idx, rbPos, writer, rbRingBuffer, byteHeap);
                } else {
                    // tail
                    genWriteBytesTailOptional(idx, rbPos, writer, byteHeap, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteBytesConstantOptional(rbPos, writer, rbRingBuffer);
                } else {
                    // delta
                    int idx = token & instanceBytesMask;
                    
                    genWriteBytesDeltaOptional(idx, rbPos, writer, byteHeap, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & instanceBytesMask;
                
                genWriteBytesCopyOptional(idx, rbPos, writer, byteHeap, rbRingBuffer);
            } else {
                // default
                int idx = token & instanceBytesMask;
                idx = idx|INIT_VALUE_MASK;
                genWriteBytesDefaultOptional(idx, rbPos, writer, byteHeap, rbRingBuffer);
            }
        }
    }


    public void acceptByteArray(int token, PrimitiveWriter writer, LocalHeap byteHeap, int rbPos, FASTRingBuffer rbRingBuffer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNone(rbPos, writer, rbRingBuffer);
                } else {
                    // tail
                    int idx = token & instanceBytesMask;
                    genWriteBytesTail(idx, rbPos, writer, byteHeap, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & instanceBytesMask;
                    genWriteBytesDelta(idx, rbPos, writer, byteHeap, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopy(token & instanceBytesMask, rbPos, byteHeap, writer, rbRingBuffer);
            } else {
                // default
                genWriteBytesDefault(token & instanceBytesMask, rbPos, byteHeap, writer, rbRingBuffer);
            }
        }
    }


    public void acceptCharSequenceASCIIOptional(int token, PrimitiveWriter writer, LocalHeap byteHeap, int fieldPos, FASTRingBuffer rbRingBuffer) {

        int idx = token & TEXT_INSTANCE_MASK;
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
                    
                    genWriteTextNoneOptional(fieldPos, writer, rbRingBuffer);

                } else {
                    // tail
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Tail)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    
                    genWriteTextTailOptional(idx, fieldPos, writer, byteHeap, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    genWriteTextConstantOptional(fieldPos, writer, rbRingBuffer);
                } else {
                    // delta
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Delta)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    
                    genWriteTextDeltaOptional(idx, fieldPos, writer, byteHeap, rbRingBuffer);

                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Copy)) : "Found "
                        + TokenBuilder.tokenToString(token);
                
                genWriteTextCopyOptional(idx, fieldPos, writer, byteHeap, rbRingBuffer);

            } else {
                // default
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Default)) : "Found "
                        + TokenBuilder.tokenToString(token);
                
                genWriteTextDefaultOptional(idx, fieldPos, writer, byteHeap, rbRingBuffer);

            }
        }

    }

    public void acceptCharSequenceASCII(int token, PrimitiveWriter writer, LocalHeap byteHeap, int fieldPos, FASTRingBuffer rbRingBuffer) {
       
        int idx = token & TEXT_INSTANCE_MASK;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextNone(fieldPos, writer, rbRingBuffer);
                } else {
                    // tail                    
                    genWriteTextTail(idx, fieldPos, writer, byteHeap, rbRingBuffer);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    genWriteTextDelta(idx, fieldPos, writer, byteHeap, rbRingBuffer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextCopy(idx, fieldPos, writer, byteHeap, rbRingBuffer);
            } else {
                // default
                genWriteTextDefault(idx, fieldPos, writer, byteHeap, rbRingBuffer);
            }
        }

    }


    public void openGroup(int token, int pmapSize, PrimitiveWriter writer) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 == (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            genWriteOpenGroup(pmapSize, writer);
        }

    }
    
    
    public void closeGroup(int token, PrimitiveWriter writer) {
        assert (token < 0);
        assert (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
                genWriteClosePMap(writer);
        }
    }


    public void flush(PrimitiveWriter writer) {
        PrimitiveWriter.flush(writer);
    }

    private boolean dispatchWriteByToken(PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {

        int token = fullScript[activeScriptCursor];
       
        assert (gatherWriteData(writer, token, activeScriptCursor, fieldPos, rbRingBuffer));

        if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
            // 0????
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 00???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    
                    if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                        // the work.
                        // not optional
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            acceptIntegerUnsigned(token, fieldPos, rbRingBuffer, writer);
                        } else {
                            acceptIntegerSigned(token, fieldPos, rbRingBuffer, writer);
                        }
                    } else {

                        // optional
                        //TODO: B, Add lookup for value of absent/null instead of this constant.
                        int valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {                            
                            acceptIntegerUnsignedOptional(token, valueOfNull, fieldPos, rbRingBuffer, writer);                            
                        } else {        
                            acceptIntegerSignedOptional(token, valueOfNull, fieldPos, rbRingBuffer, writer);
                        }

                    }
                    fieldPos+=1;
                } else {
                    assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
                    
                    if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                        // the work.
                        // not optional
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            acceptLongUnsigned(token, fieldPos, rbRingBuffer, writer);
                        } else {
                            acceptLongSigned(token, fieldPos, rbRingBuffer, writer);
                        }
                    } else {
                        //TODO: B, Add lookup for value of absent/null instead of this constant.
                        long valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;

                        // optional
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            acceptLongUnsignedOptional(token, valueOfNull, fieldPos, rbRingBuffer, writer);
                        } else {
                            acceptLongSignedOptional(token, valueOfNull, fieldPos, rbRingBuffer, writer);
                        }

                    }
                    fieldPos+=2;
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    
                    //text is written to the ring buffer encoded as ascii or utf8
                    //this code must write the bytes to the stream,
                    //     ascii bytes need a trailing high bit set
                    //     utf8 bytes need a leading length int
                    
                    
                    assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
                    assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));
                    

                         if (readFromIdx>=0) {
                            int source = token & TEXT_INSTANCE_MASK;
                            int target = readFromIdx & TEXT_INSTANCE_MASK;
                            genWriteCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
                            readFromIdx = -1; //reset for next field where it might be used.
                        }
                        
                        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                            // the work.
                            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                                // ascii
                                acceptCharSequenceASCII(token, writer, byteHeap, fieldPos, rbRingBuffer);
                            } else {
                                // utf8
                                acceptByteArray(token, writer, byteHeap, fieldPos, rbRingBuffer);
                            }
                        } else {
                            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                                // ascii optional
                                acceptCharSequenceASCIIOptional(token, writer, byteHeap, fieldPos, rbRingBuffer);
                            } else {
                                // utf8 optional
                                acceptByteArrayOptional(token, writer, byteHeap, fieldPos, rbRingBuffer);
                            }
                        }

                    fieldPos+=2;
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        
                        int expoToken = token;
                        
                        //at runtime if the value is null for the exponent must not
                        //write the mantissa to the stream.
                        
                        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_TYPE))) {
                            //exponent not optional so mantissa will aLWAYS BE WRITTEN
                            acceptIntegerSigned(expoToken, fieldPos, rbRingBuffer, writer);
                            //Must record the exponent write while we still have the values.
                            assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                            int mantToken = fullScript[++activeScriptCursor];
                            assert(0 == (mantToken & (1 << TokenBuilder.SHIFT_TYPE))) : "Bad template, mantissa can not be optional";
                            acceptLongSigned(mantToken, fieldPos + 1, rbRingBuffer, writer);

                        } else {
                            //exponent is optional so the mantissa may or may not be written.
                            acceptOptionalDecimal(fieldPos, writer, expoToken, fieldPos, rbRingBuffer);
                        }
                                                
                        fieldPos+=3;
                    } else {
                        
                        if (readFromIdx>=0) {
                            int source = token & instanceBytesMask;
                            int target = readFromIdx & instanceBytesMask;
                            genWriteCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
                            readFromIdx = -1; //reset for next field where it might be used.
                        }
                        
                        // //0111? ByteArray
                        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                            // 01110 ByteArray
                            acceptByteArray(token, writer, byteHeap, fieldPos, rbRingBuffer);
                        } else {
                            // 01111 ByteArrayOptional
                            acceptByteArrayOptional(token, writer, byteHeap, fieldPos, rbRingBuffer);
                        }
                        fieldPos+=2;
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

                        boolean isTemplate = (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));
                        if (isTemplate && true) {
                            
                            openMessage(token, templatePMapSize, fieldPos-1, writer, rbRingBuffer);
                                                        
                        } else {
                            
                            // this is NOT a message/template so the non-template
                            // pmapSize is used.
                            openGroup(token, nonTemplatePMapSize, writer);
                            
                        }
                    } else {                        
                        closeGroup(token, writer);
                    }

                } else {
                    // 101??
                    // Length Type, no others defined so no need to keep
                    // checking
                    // Only happens once before a node sequence so push it on
                    // the count stack
                    if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
                        // not optional
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            acceptIntegerUnsigned(token, fieldPos, rbRingBuffer, writer);
                        } else {
                            acceptIntegerSigned(token, fieldPos, rbRingBuffer, writer);
                        }
                    } else {

                        // optional
                        //TODO: B, Add lookup for value of absent/null instead of this constant.
                        int valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                        
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {                                
                            acceptIntegerUnsignedOptional(token, valueOfNull, fieldPos, rbRingBuffer, writer);
                        } else {
                            acceptIntegerSignedOptional(token, valueOfNull, fieldPos, rbRingBuffer, writer);
                        }
                    }
                    fieldPos+=1;
                    assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
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
                                        genWriteDictionaryIntegerReset(idx,intInit[idx], intValues);
                                    }
                                } else {
                                    // long
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        genWriteDictionaryLongReset(idx, longInit[idx], longValues);
                                    }
                                }
                            } else {
                                if (0 == (idx & 4)) {
                                    // text
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        if (null!=byteHeap) {
                                            genWriteDictionaryTextReset(idx, byteHeap);
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
                                                genWriteDictionaryBytesReset(idx, byteHeap);
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
        assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
        return false;
    }


    private void acceptOptionalDecimal(int fieldPos, PrimitiveWriter writer, int expoToken, int rbPos, FASTRingBuffer rbRingBuffer) {
        //TODO: must call specific gen method.
        
        int mantissaToken = fullScript[1+activeScriptCursor];
        int exponentValueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT; //TODO: B, neeed to inject
        
        int exponentTarget = (expoToken & intInstanceMask);
        int exponentSource = readFromIdx > 0 ? readFromIdx & intInstanceMask : exponentTarget;
        
        int mantissaTarget = (mantissaToken & longInstanceMask);
        int mantissaSource = readFromIdx > 0 ? readFromIdx & longInstanceMask : mantissaTarget;
        
  //      System.err.println(TokenBuilder.tokenToString(expoToken)+ " "+TokenBuilder.tokenToString(mantissaToken));
        
        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (expoToken & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (expoToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none

                        if (0 == (mantissaToken & (1 << TokenBuilder.SHIFT_OPER))) {
                            // none, constant, delta
                            if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                                // none, delta
                                if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                    // none none
                                    genWriteDecimalNoneOptionalNone(exponentTarget, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                                } else {
                                    // none delta
                                    genWriteDecimalNoneOptionalDelta(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                                }
                            } else {
                                // none constant
                                genWriteDecimalNoneOptionalConstant(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, this);
                            }

                        } else {
                            // copy, default, increment
                            if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                                // copy, increment
                                if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                    // none copy
                                    genWriteDecimalNoneOptionalCopy(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                                } else {
                                    // none increment
                                    genWriteDecimalNoneOptionalIncrement(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                                }
                            } else {
                                // none default
                                long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                                genWriteDecimalNoneOptionalDefault(exponentTarget, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer, this);
                            }
                        }

                } else {
                    // delta 
                    if (0 == (mantissaToken & (1 << TokenBuilder.SHIFT_OPER))) {
                        // none, constant, delta
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // none, delta
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // delta none
                                genWriteDecimalDeltaOptionalNone(exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            } else {
                                // delta delta
                                genWriteDecimalDeltaOptionalDelta(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            }
                        } else {
                            // delta constant
                            genWriteDecimalDeltaOptionalConstant(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, this);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // delta copy
                                genWriteDecimalDeltaOptionalCopy(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            } else {
                                // delta increment
                                genWriteDecimalDeltaOptionalIncrement(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            }
                        } else {
                            // delta default
                            long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalDeltaOptionalDefault(exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer, this);
                        }
                    }
                    
                }
            } else {
                // constant
                if (0 == (mantissaToken & (1 << TokenBuilder.SHIFT_OPER))) {
                    // none, constant, delta
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // none, delta
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // constant none
                            genWriteDecimalConstantOptionalNone(exponentValueOfNull, mantissaTarget, rbPos, writer, rbRingBuffer, longValues, this);
                        } else {
                            // constant delta
                            genWriteDecimalConstantOptionalDelta(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer, longValues, this);
                        }
                    } else {
                        // constant constant
                        genWriteDecimalConstantOptionalConstant(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer, this);
                    }

                } else {
                    // copy, default, increment
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // copy, increment
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // constant copy
                            genWriteDecimalConstantOptionalCopy(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer, longValues, this);
                        } else {
                            // constant increment
                            genWriteDecimalConstantOptionalIncrement(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer, longValues, this);
                        }
                    } else {
                        // constant default
                        long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                        genWriteDecimalConstantOptionalDefault(exponentValueOfNull, mantissaTarget, mantissaConstDefault, rbPos, writer, rbRingBuffer, this);
                    }
                }
            }

        } else {
            // copy, default, increment
            if (0 == (expoToken & (2 << TokenBuilder.SHIFT_OPER))) {
                // copy, increment
                if (0 == (expoToken & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    if (0 == (mantissaToken & (1 << TokenBuilder.SHIFT_OPER))) {
                        // none, constant, delta
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // none, delta
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // copy none
                                genWriteDecimalCopyOptionalNone(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            } else {
                                // copy delta
                                genWriteDecimalCopyOptionalDelta(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            }
                        } else {
                            // copy constant
                            genWriteDecimalCopyOptionalConstant(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, this);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // copy copy
                                genWriteDecimalCopyOptionalCopy(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            } else {
                                // copy increment
                                genWriteDecimalCopyOptionalIncrement(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            }
                        } else {
                            // copy default
                            long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalCopyOptionalDefault(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer, this);
                        }
                    }
                } else {
                    // increment
                    if (0 == (mantissaToken & (1 << TokenBuilder.SHIFT_OPER))) {
                        // none, constant, delta
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // none, delta
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // increment none
                                genWriteDecimalIncrementOptionalNone(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            } else {
                                // increment delta
                                genWriteDecimalIncrementOptionalDelta(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            }
                        } else {
                            // increment constant
                            genWriteDecimalIncrementOptionalConstant(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, this);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                           // int target = (mantissaToken & longInstanceMask);
                          //  int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // increment copy
                                genWriteDecimalIncrementOptionalCopy(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            } else {
                                // increment increment
                                genWriteDecimalIncrementOptionalIncrement(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues, this);
                            }
                        } else {
                            // increment default
                            long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalIncrementOptionalDefault(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer, this);
                        }
                    }
                }
            } else {
                // default
                int exponentConstDefault = intValues[expoToken & intInstanceMask]; //this is a runtime constant so we look it up here
                
                if (0 == (mantissaToken & (1 << TokenBuilder.SHIFT_OPER))) {
                    // none, constant, delta
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // none, delta
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // default none                            
                            genWriteDecimalDefaultOptionalNone(exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, longValues, intValues, this);
                        } else {
                            
                          // default delta
                            genWriteDecimalDefaultOptionalDelta(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, longValues, intValues, this);
                        }
                    } else {
                        // default constant
                        genWriteDecimalDefaultOptionalConstant(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, intValues, this);
                    }

                } else {
                    // copy, default, increment
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // copy, increment
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // default copy
                            genWriteDecimalDefaultOptionalCopy(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, longValues, this);
                        } else {
                            // default increment
                            genWriteDecimalDefaultOptionalIncrement(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, longValues, this);
                        }
                    } else {
                        // default default
                        long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                        genWriteDecimalDefaultOptionalDefault(exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, mantissaConstDefault, rbPos, writer, rbRingBuffer, intValues, this);
                    }
                }
            }
        }        
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
                value = "<len:" + FASTRingBufferReader.readDataLength(queue, fieldPos) + ">";
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

    
    public void openMessage(int token, int pmapSize, int fieldPos,  PrimitiveWriter writer, FASTRingBuffer queue) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        //add 1 bit to pmap and write the templateId
      //  System.err.println("open msg");
        genWriteOpenTemplatePMap(pmapSize, fieldPos, writer, queue);
        if (0 == (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            //group does not require PMap so we will close our 1 bit PMap now when we use it.
            //NOTE: if this was not done here it would add the full latency of the entire message encode before transmit
            genWriteClosePMap(writer); 
       //     System.err.println("close msg");
        } else {
        //    System.err.println("********* must close later");
        }
    }

    @Override
    public void runBeginMessage() {    
      callBeginMessage(null,null);
    }
    
    public void callBeginMessage(PrimitiveWriter writer, FASTRingBuffer ringBuffer) {
        beginMessage(writer, ringBuffer);
    }
    
    private void beginMessage(PrimitiveWriter writer, FASTRingBuffer ringBuffer) {
        if (preambleData.length != 0) {
            
            genWritePreamble(preambleData, writer, ringBuffer, this); ///TODO: A, must be in generator so gen method is integrated
            
        };

        // template processing (can these be nested?)
      //  int templateId = FASTRingBufferReader.readInt(ringBuffer, idx);
        
        fieldPos++;  
    }
    
    @Override
    public void runFromCursor() {
        fieldPos = 0;
        encode(null,null);
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
    ///////////////////////

   
    @Override
    public int encode(PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        //TODO: B, generated code looks up ring buffer, remove argument
        // rb=RingBuffers.get(ringBuffers,activeScriptCursor);
        
        //TODO: A, the generated code does not call the call begin message logic.
        //TODO: A, the generated code calls unBlockFragment not sure it should!
        
        fieldPos = 0;
        
        //TODO: how will this be set for generated code?
        if (null!=rbRingBuffer) {
            //cursor and limit already set
            setActiveScriptCursor(rbRingBuffer.consumerData.getCursor());        
            setActiveScriptLimit(rbRingBuffer.consumerData.getCursor() + rbRingBuffer.fragmentSteps());
        }
        
        if (null!=rbRingBuffer && rbRingBuffer.consumerData.isNewMessage()) {                
            callBeginMessage(writer, rbRingBuffer);
        }
        
        int stop = activeScriptLimit; 
        while (activeScriptCursor<stop) { 
            dispatchWriteByToken(writer,rbRingBuffer);
            activeScriptCursor++; 
        }
        return activeScriptCursor;
    }

 
}
