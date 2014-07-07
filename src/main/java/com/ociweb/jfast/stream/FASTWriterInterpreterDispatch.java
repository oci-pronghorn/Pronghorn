//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.FASTWriterDispatchTemplates;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public class FASTWriterInterpreterDispatch extends FASTWriterDispatchTemplates implements GeneratorDriving{ 

    public FASTRingBuffer rbRingBufferLocal = new FASTRingBuffer((byte)2,(byte)2,null, null, null);
    
    protected final int[] fieldIdScript;
    protected final String[] fieldNameScript;
    
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
    

    public void acceptLongSignedOptional(int token, long valueOfNull, long value, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
        int target = (token & longInstanceMask);
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongSignedOptional(valueOfNull, target,  writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    genWriteLongSignedDeltaOptional(valueOfNull, target, source, writer, longValues, rbPos, rbRingBuffer);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";
                genWriteLongSignedConstantOptional(valueOfNull, target, writer, rbPos, rbRingBuffer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                
                // copy, increment
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongSignedCopyOptional(valueOfNull, target, source, writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongSignedIncrementOptional(valueOfNull, target, source, writer, longValues, rbPos, rbRingBuffer);
                }
            } else {
                // default              
                
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];                
                
                genWriteLongSignedDefaultOptional(valueOfNull, target, constDefault, writer, rbPos, rbRingBuffer);
            }
        }
    }


//
//    public void acceptLongSigned(int token, long value, Object newParam, FASTRingBuffer rbRingBuffer) {
//        
//        ////    //temp solution as the ring buffer is introduce into all the APIs
//        rbRingBuffer.dump();            
//        rbRingBuffer.buffer[rbRingBuffer.mask & rbRingBuffer.addPos++] = (int) (value >>> 32);
//        rbRingBuffer.buffer[rbRingBuffer.mask & rbRingBuffer.addPos++] = (int) (value & 0xFFFFFFFF); 
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

                    genWriteLongSignedNone(idx, writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    
                    genWriteLongSignedDelta(target, source, writer, longValues, rbPos, rbRingBuffer);
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
                    genWriteLongSignedCopy(target, source, writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongSignedIncrement(target, source, writer, longValues, rbPos, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongSignedDefault(constDefault, writer, rbPos, rbRingBuffer);
            }
        }
    }



    void acceptLongUnsignedOptional(int token, long valueOfNull, long value, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {

        int target = token & longInstanceMask;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongUnsignedNoneOptional(valueOfNull, target, writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;

                    genWriteLongUnsignedDeltaOptional(valueOfNull, target, source, writer, longValues, rbPos, rbRingBuffer);
                }
            } else {
                // constant
                assert (longValues[token & longInstanceMask] == value) : "Only the constant value from the template may be sent";

                genWriteLongUnsignedConstantOptional(valueOfNull, target, writer, rbPos, rbRingBuffer);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                
                // copy, increment
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongUnsignedCopyOptional(valueOfNull, target, source, writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongUnsignedIncrementOptional(valueOfNull, target, source, writer, longValues, rbPos, rbRingBuffer);
                }
            } else {
                // default               
                
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefaultOptional(valueOfNull, target, constDefault, writer, rbPos, rbRingBuffer);
            }
        }
    }


//  public void acceptIntegerSigned(int token, int value, FASTRingBuffer rbRingBuffer, Object newParam, Object thing) {
//
//      
//      
//  //temp solution as the ring buffer is introduce into all the APIs
//  rbRingBuffer.dump();
//  rbRingBuffer.buffer[rbRingBuffer.mask & rbRingBuffer.addPos++] = value;
//  FASTRingBuffer.unBlockMessage(rbRingBuffer);
//  int rbPos = 0;
//      
//      
//      acceptIntegerSigned(token, rbPos, rbRingBuffer);
//  }

//    void acceptLongUnsigned(int token, long value, FASTRingBuffer rbRingBuffer) {
//        
////      //temp solution as the ring buffer is introduce into all the APIs
//      rbRingBuffer.dump();            
//      rbRingBuffer.buffer[rbRingBuffer.mask & rbRingBuffer.addPos++] = (int) (value >>> 32);
//      rbRingBuffer.buffer[rbRingBuffer.mask & rbRingBuffer.addPos++] = (int) (value & 0xFFFFFFFF); 
//      FASTRingBuffer.unBlockMessage(rbRingBuffer);
//      int rbPos = 0;
//        
//        
//        acceptLongUnsigned(token, rbPos, rbRingBuffer);
//        
//    }

    public void acceptLongUnsigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongUnsignedNone(idx, writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    
                    genWriteLongUnsignedDelta(target, source, writer, longValues, rbPos, rbRingBuffer);
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
                    genWriteLongUnsignedCopy(target, source, writer, longValues, rbPos, rbRingBuffer);
                } else {
                    // increment
                    genWriteLongUnsignedIncrement(target, source, writer, longValues, rbPos, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefault(constDefault, writer, rbPos, rbRingBuffer);
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

                    genWriteIntegerSignedNone(idx, writer, intValues, rbPos, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & intInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    
                    genWriteIntegerSignedDelta(target, source, writer, intValues, rbPos, rbRingBuffer);
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
                    genWriteIntegerSignedCopy(target, source, writer, intValues, rbPos, rbRingBuffer);
                } else {
                    // increment
                    genWriteIntegerSignedIncrement(target, source, writer, intValues, rbPos, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerSignedDefault(constDefault, writer, rbPos, rbRingBuffer);
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
                    genWriteIntegerSignedNoneOptional(target, valueOfNull, writer, intValues, rbPos, rbRingBuffer);
                } else {
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerSignedDeltaOptional(target, source, valueOfNull, writer, intValues, rbPos, rbRingBuffer);
                }
            } else {
                // constant
                genWriteIntegerSignedConstantOptional(valueOfNull, writer, rbPos, rbRingBuffer);
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
                    genWriteIntegerSignedCopyOptional(target, source, valueOfNull, writer, intValues, rbPos, rbRingBuffer);
                } else {
                    // increment
                    genWriteIntegerSignedIncrementOptional(target, source, valueOfNull, writer, intValues, rbPos, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];


                genWriteIntegerSignedDefaultOptional(source, constDefault, valueOfNull, writer, rbPos, rbRingBuffer);
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
                    genWriteIntegerUnsignedNoneOptional(target, valueOfNull, writer, rbPos, rbRingBuffer);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerUnsignedDeltaOptional(target, source, valueOfNull, writer, intValues, rbPos, rbRingBuffer);
                }
            } else {
                // constant
                genWriteIntegerUnsignedConstantOptional(valueOfNull, writer, rbPos, rbRingBuffer);
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
                    genWriteIntegerUnsignedCopyOptional(target, source, valueOfNull, writer, intValues, rbPos, rbRingBuffer);
                } else {
                    // increment
                    genWriteIntegerUnsignedIncrementOptional(target, source, valueOfNull, writer, intValues, rbPos, rbRingBuffer);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = intValues[idx];

                genWriteIntegerUnsignedDefaultOptional(source, valueOfNull, constDefault, writer, rbPos, rbRingBuffer);
            }
        }
    }


    public void write(int token, byte[] value, int offset, int length, PrimitiveWriter writer) {

        assert (0 != (token & (2 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));
        
        if (readFromIdx>=0) {
            int source = token & instanceBytesMask;
            int target = readFromIdx & instanceBytesMask;
            genWriteCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
            readFromIdx = -1; //reset for next field where it might be used.
        }

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            acceptByteArray(token, value, offset, length, writer, byteHeap);
        } else {
            acceptByteArrayOptional(token, value, offset, length, writer);
        }
    }

    private void acceptByteArrayOptional(int token, byte[] value, int offset, int length, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNoneOptional(offset, length, value, writer);
                } else {
                    // tail
                    int idx = token & instanceBytesMask;
                    genWriteBytesTailOptional(idx, offset, length, value, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteBytesConstantOptional(writer);
                } else {
                    // delta
                    int idx = token & instanceBytesMask;
                    
                    genWriteBytesDeltaOptional(idx, offset, length, value, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & instanceBytesMask;
                
                genWriteBytesCopyOptional(idx, offset, length, value, writer, byteHeap);
            } else {
                // default
                int idx = token & instanceBytesMask;
                idx = idx|INIT_VALUE_MASK;
                genWriteBytesDefaultOptional(idx, offset, length, value, writer, byteHeap);
            }
        }
    }


    private void acceptByteArray(int token, byte[] value, int offset, int length, PrimitiveWriter writer, LocalHeap byteHeap) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNone(offset, length, value, writer);
                } else {
                    // tail
                    int idx = token & instanceBytesMask;
                    genWriteBytesTail(idx, offset, length, value, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & instanceBytesMask;
                    
                    genWriteBytesDelta(idx, offset, length, value, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopy(token & instanceBytesMask, offset, length, value, byteHeap, writer);
            } else {
                // default
                genWriteBytesDefault(token & instanceBytesMask, offset, length, value, byteHeap, writer);
            }
        }
    }


    // TODO: Z, add writeDup(int id) for repeating the last value sent,
    // this can avoid string check for copy operation if its already known that
    // we are sending the same value.

    public void write(int token, ByteBuffer buffer, PrimitiveWriter writer) {

        assert (0 != (token & (2 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (readFromIdx>=0) {
            int source = token & instanceBytesMask;
            int target = readFromIdx & instanceBytesMask;
            genWriteCopyBytes(source, target, byteHeap); //NOTE: may find better way to suppor this with text, requires research.
            readFromIdx = -1; //reset for next field where it might be used.
        }
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            acceptByteBuffer(token, buffer, writer, byteHeap);
        } else {
            acceptByteBufferOptional(token, buffer, writer, byteHeap);
        }
    }

    private void acceptByteBufferOptional(int token, ByteBuffer value, PrimitiveWriter writer, LocalHeap byteHeap) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriterBytesNoneOptional(value, writer);
                } else {
                    // tail
                    genWriterBytesTailOptional(token & instanceBytesMask, value, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    genWriteBytesConstantOptional(writer);
                } else {
                    // delta
                    genWriterBytesDeltaOptional(token & instanceBytesMask, value, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriterBytesCopyOptional(token & instanceBytesMask, value, writer, byteHeap);
            } else {
                // default
                genWriterBytesDefaultOptional(token & instanceBytesMask, value, writer, byteHeap);
            }
        }
    }



    private void acceptByteBuffer(int token, ByteBuffer value, PrimitiveWriter writer, LocalHeap byteHeap) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNone(value, writer);
                } else {
                    // tail
                    genWriteBytesTail(token & instanceBytesMask, value, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    genWriteBytesDelta(token & instanceBytesMask, value, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopy(token & instanceBytesMask, value, byteHeap, writer);
            } else {
                // default
                genWriteBytesDefault(token & instanceBytesMask, value, writer, byteHeap);
            }
        }
    }



    public void write(int token, CharSequence value, PrimitiveWriter writer) {

        assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        if (readFromIdx>=0) {
            int source = token & TEXT_INSTANCE_MASK;
            int target = readFromIdx & TEXT_INSTANCE_MASK;
            genWriteCopyText(source, target, textHeap); //NOTE: may find better way to suppor this with text, requires research.
            readFromIdx = -1; //reset for next field where it might be used.
        }
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii
                acceptCharSequenceASCII(token, value, writer, textHeap);
            } else {
                // utf8
                acceptCharSequenceUTF8(token, value, writer);
            }
        } else {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii optional
                acceptCharSequenceASCIIOptional(token, value, writer, textHeap);
            } else {
                // utf8 optional
                acceptCharSequenceUTF8Optional(token, value, writer);
            }
        }
    }

    private void acceptCharSequenceUTF8Optional(int token, CharSequence value, PrimitiveWriter writer) {

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
                    
                    genWriteUTFTextTailOptional(idx, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteUTFTextConstantOptional(writer);
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteUTFTextDeltaOptional(idx, value, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteUTFTextCopyOptional(idx, value, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteUTFTextDefaultOptional(idx, value, writer, textHeap);
            }
        }
    }



    private void acceptCharSequenceUTF8(int token, CharSequence value, PrimitiveWriter writer) {

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
                    
                    genWriteUTFTextTail(idx, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteUTFTextDelta(idx, value, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                // System.err.println("AA");
                genWriteUTFTextCopy(idx, value, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteUTFTextDefault(idx, value, writer, textHeap);
            }
        }

    }



    private void acceptCharSequenceASCIIOptional(int token, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {

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
                    
                    genWriteTextTailOptional(idx, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    genWriteTextConstantOptional(writer);
                } else {
                    // delta
                    assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Delta)) : "Found "
                            + TokenBuilder.tokenToString(token);
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextDeltaOptional(idx, value, writer, textHeap);

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
                
                genWriteTextCopyOptional(idx, value, writer, textHeap);

            } else {
                // default
                assert (TokenBuilder.isOpperator(token, OperatorMask.Field_Default)) : "Found "
                        + TokenBuilder.tokenToString(token);
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextDefaultOptional(idx, value, writer, textHeap);

            }
        }

    }


    private void acceptCharSequenceASCII(int token, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {

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
                    
                    genWriteTextTail(idx, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextDelta(idx, value, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextCopy(idx, value, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextDefault(idx, value, writer, textHeap);
            }
        }

    }


    public void acceptCharArrayUTF8Optional(int token, char[] value, int offset, int length, PrimitiveWriter writer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextUTFNoneOptional(offset, length, value, writer);

                } else {
                    // tail
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextUTFTailOptional(idx, offset, length, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextUTFConstantOptional(writer);
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextUTFDeltaOptional(idx, offset, length, value, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextUTFCopyOptional(idx, offset, length, value, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextUTFDefaultOptional(idx, offset, length, value, writer, textHeap);
            }
        }

    }



    public void acceptCharArrayUTF8(int token, char[] value, int offset, int length, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none             
                    
                    
                    genWriteTextUTFNone(offset, length, value, writer);

                } else {
                    // tail
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    //Where to we convert the chars into bytes?
                    
                    genWriteTextUTFTail(idx, offset, length, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & TEXT_INSTANCE_MASK;
                    
                    genWriteTextUTFDelta(idx, offset, length, value, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextUTFCopy(idx, offset, length, value, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                int constId = idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK;
                
                genWriteTextUTFDefault(constId, offset, length, value, writer, textHeap);
            }
        }

    }


    public void acceptCharArrayASCIIOptional(int token, char[] value, int offset, int length, PrimitiveWriter writer) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextNoneOptional(value, offset, length, writer);
                } else {
                    // tail
                    genWriteTextTailOptional2(token & TEXT_INSTANCE_MASK, offset, length, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextConstantOptional(writer);
                } else {
                    // delta
                    genWriteTextDeltaOptional2(token & TEXT_INSTANCE_MASK, offset, length, value, textHeap, writer);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & TEXT_INSTANCE_MASK;
                
                genWriteTextCopyOptional(idx, offset, length, value, writer, textHeap);
            } else {
                // default
                int idx = token & TEXT_INSTANCE_MASK;
                
                int constId = idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK;
                
                genWriteTextDefaultOptional(constId, offset, length, value, writer, textHeap);
            }
        }

    }



    public void acceptCharArrayASCII(int token, char[] value, int offset, int length, PrimitiveWriter writer) {

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteTextNone(value, offset, length, writer);
                } else {
                    // tail
                    genWriteTextTail2(token & TEXT_INSTANCE_MASK, offset, length, value, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    genWriteTextDelta2(token & TEXT_INSTANCE_MASK, offset, length, value, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextCopy2(token & TEXT_INSTANCE_MASK, offset, length, value, textHeap, writer);
            } else {
                // default
                genWriteTextDefault2(token & TEXT_INSTANCE_MASK, offset, length, value, textHeap, writer);
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

    public void reset() {

        dictionaryFactory.reset(intValues);
        dictionaryFactory.reset(longValues);
        dictionaryFactory.reset(textHeap);
        dictionaryFactory.reset(byteHeap);
    }

    
    
    private boolean dispatchWriteByToken(PrimitiveWriter writer) {

        int token = fullScript[activeScriptCursor];

        FASTRingBuffer rbRingBuffer = RingBuffers.get(ringBuffers,activeScriptCursor);
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
                    long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
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
                            acceptLongUnsignedOptional(token, valueOfNull, value, fieldPos, rbRingBuffer, writer);
                        } else {
                            acceptLongSignedOptional(token, valueOfNull, value, fieldPos, rbRingBuffer, writer);
                        }

                    }
                    fieldPos+=2;
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    acceptText(writer, token, rbRingBuffer);
                    fieldPos+=2;
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        
                        int expoToken = token;
                        
                        ///int exponent = FASTRingBufferReader.readInt(ringBuffers[activeScriptCursor], fieldPos);
                        long mantissa = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos + 1);

                        
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
                            acceptOptionalDecimal(fieldPos, writer, expoToken, mantissa, fieldPos, rbRingBuffer);
                        }
                                                
                        fieldPos+=3;
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
                                        if (null!=textHeap) {
                                            genWriteDictionaryTextReset(idx, textHeap);
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


    private void acceptText(PrimitiveWriter writer, int token, FASTRingBuffer rbRingBuffer) {
        assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));

        int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
        if (length < 0) {
            writeNullText(token, token & TEXT_INSTANCE_MASK, writer, textHeap); //TODO: A, must be integrated into the writes.
        } else {
            char[] buffer = rbRingBuffer.readRingCharBuffer(fieldPos);
            CharSequence value = ringCharSequence.set(buffer, rbRingBuffer.readRingCharPos(fieldPos), rbRingBuffer.readRingCharMask(), length);
            
            if (readFromIdx>=0) {
                int source = token & TEXT_INSTANCE_MASK;
                int target = readFromIdx & TEXT_INSTANCE_MASK;
                genWriteCopyText(source, target, textHeap); //NOTE: may find better way to suppor this with text, requires research.
                readFromIdx = -1; //reset for next field where it might be used.
            }
            
            if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                // the work.
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // ascii
                    acceptCharSequenceASCII(token, value, writer, textHeap);
                } else {
                    // utf8
                    acceptCharSequenceUTF8(token, value, writer);
                }
            } else {
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // ascii optional
                    acceptCharSequenceASCIIOptional(token, value, writer, textHeap);
                } else {
                    // utf8 optional
                    acceptCharSequenceUTF8Optional(token, value, writer);
                }
            }
        }
    }

    private void acceptOptionalDecimal(int fieldPos, PrimitiveWriter writer, int expoToken, long mantissa, int rbPos, FASTRingBuffer rbRingBuffer) {
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
                                    genWriteDecimalNoneOptionalNone(exponentTarget, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues);
                                } else {
                                    // none delta
                                    genWriteDecimalNoneOptionalDelta(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                                }
                            } else {
                                // none constant
                                genWriteDecimalNoneOptionalConstant(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            }

                        } else {
                            // copy, default, increment
                            if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                                // copy, increment
                                if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                    // none copy
                                    genWriteDecimalNoneOptionalCopy(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                                } else {
                                    // none increment
                                    genWriteDecimalNoneOptionalIncrement(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                                }
                            } else {
                                // none default
                                long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                                genWriteDecimalNoneOptionalDefault(exponentTarget, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer);
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
                                genWriteDecimalDeltaOptionalNone(exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues);
                            } else {
                                // delta delta
                                genWriteDecimalDeltaOptionalDelta(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            }
                        } else {
                            // delta constant
                            genWriteDecimalDeltaOptionalConstant(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // delta copy
                                genWriteDecimalDeltaOptionalCopy(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            } else {
                                // delta increment
                                genWriteDecimalDeltaOptionalIncrement(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            }
                        } else {
                            // delta default
                            long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalDeltaOptionalDefault(exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer);
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
                            genWriteDecimalConstantOptionalNone(exponentValueOfNull, mantissaTarget, rbPos, writer, rbRingBuffer, longValues);
                        } else {
                            // constant delta
                            genWriteDecimalConstantOptionalDelta(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer);
                        }
                    } else {
                        // constant constant
                        genWriteDecimalConstantOptionalConstant(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer);
                    }

                } else {
                    // copy, default, increment
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // copy, increment
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // constant copy
                            genWriteDecimalConstantOptionalCopy(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer);
                        } else {
                            // constant increment
                            genWriteDecimalConstantOptionalIncrement(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer);
                        }
                    } else {
                        // constant default
                        long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                        genWriteDecimalConstantOptionalDefault(exponentValueOfNull, mantissaTarget, mantissaConstDefault, rbPos, writer, rbRingBuffer);
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
                                genWriteDecimalCopyOptionalNone(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues);
                            } else {
                                // copy delta
                                genWriteDecimalCopyOptionalDelta(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            }
                        } else {
                            // copy constant
                            genWriteDecimalCopyOptionalConstant(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // copy copy
                                genWriteDecimalCopyOptionalCopy(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            } else {
                                // copy increment
                                genWriteDecimalCopyOptionalIncrement(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            }
                        } else {
                            // copy default
                            long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalCopyOptionalDefault(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer);
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
                                genWriteDecimalIncrementOptionalNone(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer, longValues);
                            } else {
                                // increment delta
                                genWriteDecimalIncrementOptionalDelta(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            }
                        } else {
                            // increment constant
                            genWriteDecimalIncrementOptionalConstant(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                           // int target = (mantissaToken & longInstanceMask);
                          //  int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // increment copy
                                genWriteDecimalIncrementOptionalCopy(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            } else {
                                // increment increment
                                genWriteDecimalIncrementOptionalIncrement(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, intValues, rbRingBuffer);
                            }
                        } else {
                            // increment default
                            long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalIncrementOptionalDefault(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, intValues, rbRingBuffer);
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
                            genWriteDecimalDefaultOptionalNone(exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, longValues, intValues);
                        } else {
                            
                          // default delta
                            genWriteDecimalDefaultOptionalDelta(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer);
                        }
                    } else {
                        // default constant
                        genWriteDecimalDefaultOptionalConstant(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer);
                    }

                } else {
                    // copy, default, increment
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // copy, increment
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // default copy
                            genWriteDecimalDefaultOptionalCopy(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer);
                        } else {
                            // default increment
                            genWriteDecimalDefaultOptionalIncrement(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer);
                        }
                    } else {
                        // default default
                        long mantissaConstDefault = longValues[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                        genWriteDecimalDefaultOptionalDefault(exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, mantissaConstDefault, rbPos, writer, rbRingBuffer);
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

    public void writePreamble(byte[] preambleData, PrimitiveWriter writer) {
        genWritePreamble(preambleData, writer);
    }

    public void writeNullText(int token, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta and Tail
                genWriteNullNoPMapText(idx, writer, textHeap); // no pmap, yes change to last value
            } else {
                // Copy and Increment
                genWriteNullCopyIncText(idx, writer, textHeap); // yes pmap, yes change to last value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                genWriteNullPMap(writer);
            } else {
                // default
                genWriteNullDefaultText(idx, writer, textHeap); // yes pmap, no change to last value
            }
        }
    }


    public void writeNullBytes(int token, PrimitiveWriter writer, LocalHeap byteHeap, int instanceMask) {
        
        if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
            if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
                //None and Delta and Tail
                genWriteNullNoPMapBytes(token & instanceMask, writer, byteHeap);              //no pmap, yes change to last value
            } else {
                //Copy and Increment
                int idx = token & instanceMask;
                genWriteNullCopyIncBytes(idx, writer, byteHeap);  //yes pmap, yes change to last value 
            }
        } else {
            if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                genWriteNullPMap(writer);      
            } else {    
                //default
                genWriteNullDefaultBytes(token & instanceMask, writer, byteHeap);  //yes pmap,  no change to last value
            }   
        }
        
    }

    @Override
    public int getActiveScriptCursor() {
        return activeScriptCursor;
    }

    @Override
    public void setActiveScriptCursor(int cursor) {
       activeScriptCursor = cursor;
    }

    @Override
    public void setActiveScriptLimit(int limit) { //TODO: B, find a way to remove this?
        activeScriptLimit = limit;
    }

    @Override
    public void runBeginMessage() {    
      //TODO: A, is this needed for encode?  callBeginMessage(null);
    }

    @Override
    public void runFromCursor() {
        encode(0, null);
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


    public int fieldPos=-1;
    

    //TODO: A, must be abstract in base and created by the compiler.
    void encode(int fieldPos, FASTDynamicWriter fastDynamicWriter) {
        PrimitiveWriter writer = null==fastDynamicWriter? null: fastDynamicWriter.writer;
        int stop = activeScriptLimit;
        this.fieldPos = fieldPos;
        while (activeScriptCursor<stop) { 
            dispatchWriteByToken(writer);
            activeScriptCursor++; 
        }
    }

 
}
