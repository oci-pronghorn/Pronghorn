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
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;

//May drop interface if this causes a performance problem from virtual table 
public final class FASTWriterInterpreterDispatch extends FASTWriterDispatchTemplates { 

    FASTRingBuffer rbRingBufferLocal = new FASTRingBuffer((byte)2,(byte)2,null, 10, null, null);
    
    public FASTWriterInterpreterDispatch(final TemplateCatalogConfig catalog, FASTRingBuffer queue) {
        super(catalog, buildRingBuffers(queue, catalog.fullScript()));
    }
    
    private static FASTRingBuffer[] buildRingBuffers(FASTRingBuffer queue, int[] fullScript) {
        int len = null==fullScript ? 1 : fullScript.length;
        
        FASTRingBuffer[] buffers = new FASTRingBuffer[len];
        int i = len;
        while (--i>=0) {
            buffers[i]=queue;
        }
        return buffers;
    }

    /**
     * Write null value, must only be used if the field id is one of optional
     * type.
     */
    public void write(int token, PrimitiveWriter writer) {

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
                
                //temp solution as the ring buffer is introduce into all the APIs
                FASTRingBuffer.dump(rbRingBufferLocal);
                FASTRingBuffer.addValue(rbRingBufferLocal.buffer, rbRingBufferLocal.mask, rbRingBufferLocal.addPos, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
                FASTRingBuffer.unBlockFragment(rbRingBufferLocal);
                int rbPos = 0;

                // hack until all the classes no longer need this method.
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    acceptIntegerUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                } else {
                    acceptIntegerSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                }
            } else {
                // long
                int idx = token & longInstanceMask;
                
                writeNullLong(token, idx, writer, longValues);
            }
        } else {
            // text decimal bytes
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // text
                int idx = token & TEXT_INSTANCE_MASK;
                
                writeNullText(token, idx, writer, textHeap);
            } else {
                // decimal bytes
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // decimal
                    int idx = token & intInstanceMask;
                    
                    //temp solution as the ring buffer is introduce into all the APIs     
                    FASTRingBuffer.dump(rbRingBufferLocal);
                    FASTRingBuffer.addValue(rbRingBufferLocal.buffer, rbRingBufferLocal.mask, rbRingBufferLocal.addPos, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
                    FASTRingBuffer.unBlockFragment(rbRingBufferLocal);
                    int rbPos = 0;
                                        // hack until all the classes no longer need this method.
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        acceptIntegerUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                    } else {
                        acceptIntegerSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                    } 

                    int idx1 = token & longInstanceMask;
                    
                    writeNullLong(token, idx1, writer, longValues);
                } else {
                    // byte
                    writeNullBytes(token, writer, byteHeap, instanceBytesMask);
                }
            }
        }

    }



    public void acceptLongSignedOptional(int token, long valueOfNull, long value, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
//      ////    //temp solution as the ring buffer is introduce into all the APIs
      FASTRingBuffer.dump(rbRingBuffer);            
      FASTRingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.addPos,(int) (value >>> 32));
      FASTRingBuffer.addValue(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.addPos,(int) (value & 0xFFFFFFFF)); 
      FASTRingBuffer.unBlockFragment(rbRingBuffer);
      int rbPos = 0;
        
        
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
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    
                    genWriteLongSignedDeltaOptional(value, target, source, writer, longValues);
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
                int target = (token & longInstanceMask);
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongSignedCopyOptional(value, target, source, writer, longValues);
                } else {
                    // increment
                    genWriteLongSignedIncrementOptional(value, target, source, writer, longValues);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongSignedDefaultOptional(value, constDefault, writer);
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



    void acceptLongUnsignedOptional(int token, long value, PrimitiveWriter writer) {
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
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    genWriteLongUnsignedDeltaOptional(value, target, source, writer, longValues);
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
                int target = (token & longInstanceMask);
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongUnsignedCopyOptional(value, target, source, writer, longValues);
                } else {
                    // increment
                    genWriteLongUnsignedIncrementOptional(value, target, source, writer, longValues);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = longValues[idx];
                
                genWriteLongUnsignedDefaultOptional(value, constDefault, writer);
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
                    genWriteBytesNoneOptional(value, offset, length, writer);
                } else {
                    // tail
                    int idx = token & instanceBytesMask;
                    genWriteBytesTailOptional(value, offset, length, idx, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteBytesConstantOptional(writer);
                } else {
                    // delta
                    int idx = token & instanceBytesMask;
                    
                    genWriteBytesDeltaOptional(value, offset, length, idx, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                int idx = token & instanceBytesMask;
                
                genWriteBytesCopyOptional(value, offset, length, idx, writer, byteHeap);
            } else {
                // default
                int idx = token & instanceBytesMask;
                idx = idx|INIT_VALUE_MASK;
                genWriteBytesDefaultOptional(value, offset, length, idx, writer, byteHeap);
            }
        }
    }


    private void acceptByteArray(int token, byte[] value, int offset, int length, PrimitiveWriter writer, ByteHeap byteHeap) {
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {// compiler does all
                                                            // the work.
            // none constant delta tail
            if (0 == (token & (6 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // none tail
                if (0 == (token & (8 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteBytesNone(value, offset, length, writer);
                } else {
                    // tail
                    int idx = token & instanceBytesMask;
                    genWriteBytesTail(idx, value, offset, length, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    int idx = token & instanceBytesMask;
                    
                    genWriteBytesDelta(value, offset, length, idx, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopy(token, value, offset, length, byteHeap, writer);
            } else {
                // default
                genWriteBytesDefault(token, value, offset, length, byteHeap, writer);
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

    private void acceptByteBufferOptional(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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
                    genWriterBytesTailOptional(token, value, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    genWriteBytesConstantOptional(writer);
                } else {
                    // delta
                    genWriterBytesDeltaOptional(token, value, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriterBytesCopyOptional(token, value, writer, byteHeap);
            } else {
                // default
                genWriterBytesDefaultOptional(token, value, writer, byteHeap);
            }
        }
    }



    private void acceptByteBuffer(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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
                    genWriteBytesTail(token, value, writer, byteHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    genWriteBytesDelta(token, value, writer, byteHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteBytesCopy(token, value, byteHeap, writer);
            } else {
                // default
                genWriteBytesDefault(token, value, writer, byteHeap);
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
                    
                    genWriteTextTailOptional(value, idx, writer, textHeap);
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



    public void write(int token, char[] value, int offset, int length, PrimitiveWriter writer) {

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
                acceptCharArrayASCII(token, value, offset, length, writer);
            } else {
                // utf8
                acceptCharArrayUTF8(token, value, offset, length, writer);
            }
        } else {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii optional
                acceptCharArrayASCIIOptional(token, value, offset, length, writer);
            } else {
                // utf8 optional
                acceptCharArrayUTF8Optional(token, value, offset, length, writer);
            }
        }
    }

    private void acceptCharArrayUTF8Optional(int token, char[] value, int offset, int length, PrimitiveWriter writer) {

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
                    genWriteTextUTFConstantOptional(writer);
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



    private void acceptCharArrayUTF8(int token, char[] value, int offset, int length, PrimitiveWriter writer) {
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
                int constId = idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK;
                
                genWriteTextUTFDefault(value, offset, length, constId, writer, textHeap);
            }
        }

    }


    private void acceptCharArrayASCIIOptional(int token, char[] value, int offset, int length, PrimitiveWriter writer) {
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
                    genWriteTextTailOptional2(token, value, offset, length, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    genWriteTextConstantOptional(writer);
                } else {
                    // delta
                    genWriteTextDeltaOptional2(token, value, offset, length, textHeap, writer);
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
                
                int constId = idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK;
                
                genWriteTextDefaultOptional(value, offset, length, constId, writer, textHeap);
            }
        }

    }



    private void acceptCharArrayASCII(int token, char[] value, int offset, int length, PrimitiveWriter writer) {

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
                    genWriteTextTail2(token, value, offset, length, writer, textHeap);
                }
            } else {
                // constant delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // constant
                    
                } else {
                    // delta
                    genWriteTextDelta2(token, value, offset, length, writer, textHeap);
                }
            }
        } else {
            // copy default
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {// compiler does
                                                                // all the work.
                // copy
                genWriteTextCopy2(token, value, offset, length, textHeap, writer);
            } else {
                // default
                genWriteTextDefault2(token, value, offset, length, textHeap, writer);
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

    public void openGroup(int token, int templateId, int pmapSize, PrimitiveWriter writer) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        if (pmapSize > 0) {
            genWriteOpenTemplatePMap(templateId, pmapSize, writer);
        } else {
            genWriteOpenTemplate(templateId, writer);
        }
    }



    public void closeGroup(int token, PrimitiveWriter writer) {
        assert (token < 0);
        assert (0 != (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));

        if (0 != (token & (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER))) {
            if (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) {
                genWriteCloseTemplatePMap(writer, this);
            } else {
                genWriteClosePMap(writer);
            }
        } else {
            if (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER))) {
                genWriteCloseTemplate(this);
            }
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

    public boolean dispatchWriteByToken(int fieldPos, PrimitiveWriter writer) {

        int token = fullScript[activeScriptCursor];

        assert (gatherWriteData(writer, token, activeScriptCursor, fieldPos, ringBuffers[activeScriptCursor]));

        if (0 == (token & (16 << TokenBuilder.SHIFT_TYPE))) {
            // 0????
            if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
                // 00???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    
                    if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                        // the work.
                        // not optional
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            acceptIntegerUnsigned(token, fieldPos, ringBuffers[activeScriptCursor], writer);
                        } else {
                            acceptIntegerSigned(token, fieldPos, ringBuffers[activeScriptCursor], writer);
                        }
                    } else {

                        // optional
                        //TODO: B, Add lookup for value of absent/null instead of this constant.
                        int valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {                            
                            acceptIntegerUnsignedOptional(token, valueOfNull, fieldPos, ringBuffers[activeScriptCursor], writer);                            
                        } else {        
                            acceptIntegerSignedOptional(token, valueOfNull, fieldPos, ringBuffers[activeScriptCursor], writer);
                        }

                    }
                } else {
                    long value = FASTRingBufferReader.readLong(ringBuffers[activeScriptCursor], fieldPos);
                    assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
                    
                    if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                        // the work.
                        // not optional
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            acceptLongUnsigned(token, fieldPos, ringBuffers[activeScriptCursor], writer);
                        } else {
                            acceptLongSigned(token, fieldPos, ringBuffers[activeScriptCursor], writer);
                        }
                    } else {
                        if (value == TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG) {
                            write(token, writer);
                        } else {
                            // optional
                            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                                acceptLongUnsignedOptional(token, value, writer);
                            } else {
                                acceptLongSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG, value, rbRingBufferLocal, writer);
                            }
                        }
                    }
                }
            } else {
                // 01???
                if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                    int length = FASTRingBufferReader.readTextLength(ringBuffers[activeScriptCursor], fieldPos);
                    if (length < 0) {
                        write(token, writer);
                    } else {
                        char[] buffer = ringBuffers[activeScriptCursor].readRingCharBuffer(fieldPos);
                        write(token, ringCharSequence.set(buffer, ringBuffers[activeScriptCursor].readRingCharPos(fieldPos), ringBuffers[activeScriptCursor].readRingCharMask(), length),writer);
                    }
                } else {
                    // 011??
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // 0110? Decimal and DecimalOptional
                        
                        int expoToken = token;
                        
                        int exponent = FASTRingBufferReader.readInt(ringBuffers[activeScriptCursor], fieldPos);
                        long mantissa = FASTRingBufferReader.readLong(ringBuffers[activeScriptCursor], fieldPos + 1);

                        
                        //at runtime if the value is null for the exponent must not
                        //write the mantissa to the stream.
                        
                        if (0 == (expoToken & (1 << TokenBuilder.SHIFT_TYPE))) {
                            acceptIntegerSigned(expoToken, fieldPos, ringBuffers[activeScriptCursor], writer);
                        } else {
                                    
                            //TODO: B, Add lookup for value of absent/null instead of this constant.
                            int valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                                                        
                            acceptIntegerSignedOptional(expoToken, valueOfNull, fieldPos, ringBuffers[activeScriptCursor], writer);

                        }
                        
                        int mantToken = fullScript[++activeScriptCursor];
                        
                        if (0 == (mantToken & (1 << TokenBuilder.SHIFT_TYPE))) {
                            acceptLongSigned(mantToken, fieldPos + 1, ringBuffers[activeScriptCursor], writer);
                        } else {
                            long valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
                            
                            if (TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG==mantissa) {
                                int idx = mantToken & longInstanceMask; 
                                
                                //TODO: B, Must not write null if we have already done so above, but this must also be compiled.
                                
                                writeNullLong(mantToken, idx, writer, longValues); 
                            } else {
                                acceptLongSignedOptional(mantToken, valueOfNull, mantissa, rbRingBufferLocal, writer);
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
                        openGroup(token, nonTemplatePMapSize, writer);

                    } else {
                        // System.err.println("close group:"+TokenBuilder.tokenToString(token));
                        closeGroup(token, writer);// closing this seq causing throw!!
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
                    int length = FASTRingBufferReader.readInt(ringBuffers[activeScriptCursor], fieldPos);
                    if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                        // the work.
                        // not optional
                        if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                            acceptIntegerUnsigned(token, fieldPos, ringBuffers[activeScriptCursor], writer);
                        } else {
                            acceptIntegerSigned(token, fieldPos, ringBuffers[activeScriptCursor], writer);
                        }
                    } else {
                        if (length == TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT) {
                            write(token, writer);
                        } else {
                            // optional
                            //TODO: B, Add lookup for value of absent/null instead of this constant.
                            int valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                            
                            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {                                
                                acceptIntegerUnsignedOptional(token, valueOfNull, fieldPos, ringBuffers[activeScriptCursor], writer);
                            } else {
                                acceptIntegerSignedOptional(token, valueOfNull, fieldPos, ringBuffers[activeScriptCursor], writer);
                            }
                        }
                    }

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

    public void openMessage(int pmapMaxSize, int templateId, PrimitiveWriter writer) {

        genWriteOpenMessage(pmapMaxSize, templateId, writer);

    }

    public void writePreamble(byte[] preambleData, PrimitiveWriter writer) {
        genWritePreamble(preambleData, writer);
    }

    public void writeNullLong(int token, int idx, PrimitiveWriter writer, long[] dictionary) {
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta (both do not use pmap)
                genWriteNullNoPMapLong(writer, dictionary, idx);  
                // no pmap, yes change to last value
            } else {
                // Copy and Increment
                genWriteNullCopyIncLong(writer, dictionary, idx); // yes pmap, yes change to last value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                genWriteNullPMap(writer);
            } else {
                // default
                genWriteNullDefaultLong(writer, dictionary, idx); 
            }
        }
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


    public void writeNullBytes(int token, PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask) {
        
        if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
            if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
                //None and Delta and Tail
                genWriteNullNoPMapBytes(token, writer, byteHeap, instanceMask);              //no pmap, yes change to last value
            } else {
                //Copy and Increment
                int idx = token & instanceMask;
                genWriteNullCopyIncBytes(writer, byteHeap, idx);  //yes pmap, yes change to last value 
            }
        } else {
            if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                genWriteNullPMap(writer);      
            } else {    
                //default
                genWriteNullDefaultBytes(token, writer, byteHeap, instanceMask);  //yes pmap,  no change to last value
            }   
        }
        
    }



    
    
    ///////////////////////
 
}
