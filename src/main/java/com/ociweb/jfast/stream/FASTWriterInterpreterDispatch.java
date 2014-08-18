//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.generator.FASTWriterDispatchTemplates;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

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
        
        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
        
        int target = (token & longInstanceMask);
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongSignedOptional(valueOfNull, target,  rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    genWriteLongSignedDeltaOptional(valueOfNull, target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // constant
                genWriteLongSignedConstantOptional(valueOfNull, target, rbPos, writer, buffer, mask, workingTailPos);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                
                // copy, increment
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongSignedCopyOptional(target, source, valueOfNull, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteLongSignedIncrementOptional(target, source, rbPos, valueOfNull, writer, rLongDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default              
                
                int idx = token & longInstanceMask;
                long constDefault = rLongDictionary[idx];                
                
                genWriteLongSignedDefaultOptional(target, rbPos, valueOfNull, constDefault, writer, buffer, mask, workingTailPos, rLongDictionary);
            }
        }
    }


    public void acceptLongSigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongSignedNone(idx, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    
                    genWriteLongSignedDelta(target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
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
                    genWriteLongSignedCopy(target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteLongSignedIncrement(target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = rLongDictionary[idx];
                
                genWriteLongSignedDefault(constDefault, rbPos, writer, buffer, mask, workingTailPos);
            }
        }
    }



    void acceptLongUnsignedOptional(int token, long valueOfNull, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {

        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
        
        int target = token & longInstanceMask;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteLongUnsignedNoneOptional(valueOfNull, target, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;

                    genWriteLongUnsignedDeltaOptional(valueOfNull, target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // constant
                genWriteLongUnsignedConstantOptional(valueOfNull, target, rbPos, writer, buffer, mask, workingTailPos);
                // the writeNull will take care of the rest.
            }

        } else {
            // copy, default, increment
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                
                // copy, increment
                int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // copy
                    genWriteLongUnsignedCopyOptional(valueOfNull, target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteLongUnsignedIncrementOptional(valueOfNull, target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default               
                
                int idx = token & longInstanceMask;
                long constDefault = rLongDictionary[idx];
                
                genWriteLongUnsignedDefaultOptional(valueOfNull, target, constDefault, rbPos, writer, buffer, mask, workingTailPos, rLongDictionary);
            }
        }
    }


    public void acceptLongUnsigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & longInstanceMask;

                    genWriteLongUnsignedNone(idx, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // delta
                    //Delta opp never uses PMAP
                    int target = (token & longInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                    
                    genWriteLongUnsignedDelta(target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
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
                    genWriteLongUnsignedCopy(target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteLongUnsignedIncrement(target, source, rbPos, writer, rLongDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default
                int idx = token & longInstanceMask;
                long constDefault = rLongDictionary[idx];
                
                genWriteLongUnsignedDefault(constDefault, rbPos, writer, buffer, mask, workingTailPos);
            }
        }
    }



    public void acceptIntegerSigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & intInstanceMask;

                    genWriteIntegerSignedNone(idx, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & intInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    
                    genWriteIntegerSignedDelta(target, source, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
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
                    genWriteIntegerSignedCopy(target, source, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteIntegerSignedIncrement(target, source, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = rIntDictionary[idx];

                genWriteIntegerSignedDefault(constDefault, rbPos, writer, buffer, mask, workingTailPos);
            }
        }
    }

    public void acceptIntegerUnsigned(int token, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;

        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    int idx = token & intInstanceMask;
                    
                    
                    genWriteIntegerUnsignedNone(idx, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int target = (token & intInstanceMask);
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerUnsignedDelta(target, source, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
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
                    genWriteIntegerUnsignedCopy(target, source, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteIntegerUnsignedIncrement(target, source, rbPos, writer, rIntDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = rIntDictionary[idx];
                genWriteIntegerUnsignedDefault(constDefault, rbPos, writer, buffer, mask, workingTailPos);
            }
        }
    }
 
    public void acceptIntegerSignedOptional(int token, int valueOfNull, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
        
        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = (token & intInstanceMask);
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteIntegerSignedNoneOptional(target, rbPos, valueOfNull, writer, rIntDictionary, buffer, mask, workingTailPos);
                } else {
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerSignedDeltaOptional(target, source, rbPos, valueOfNull, writer, rIntDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // constant
                genWriteIntegerSignedConstantOptional(valueOfNull, rbPos, writer, buffer, mask, workingTailPos);
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
                    genWriteIntegerSignedCopyOptional(target, source, rbPos, valueOfNull, writer, rIntDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteIntegerSignedIncrementOptional(target, source, rbPos, valueOfNull, writer, rIntDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = rIntDictionary[idx];


                genWriteIntegerSignedDefaultOptional(source, rbPos, constDefault, valueOfNull, writer, buffer, mask, workingTailPos, rIntDictionary);
            }
        }
    }

    public void acceptIntegerUnsignedOptional(int token, int valueOfNull, int rbPos, FASTRingBuffer rbRingBuffer, PrimitiveWriter writer) {
       
        int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
        int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
        PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
            // none, constant, delta
            if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                int target = (token & intInstanceMask);
                // none, delta
                if (0 == (token & (4 << TokenBuilder.SHIFT_OPER))) {
                    // none
                    genWriteIntegerUnsignedNoneOptional(target, valueOfNull, rbPos, writer, buffer, mask, workingTailPos, rIntDictionary);
                } else {
                    // delta
                    // Delta opp never uses PMAP
                    int source = readFromIdx > 0 ? readFromIdx & intInstanceMask : target;
                    genWriteIntegerUnsignedDeltaOptional(target, source, rbPos, valueOfNull, writer, rIntDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // constant
                genWriteIntegerUnsignedConstantOptional(rbPos, valueOfNull, writer, buffer, mask, workingTailPos);
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
                    genWriteIntegerUnsignedCopyOptional(target, source, rbPos, valueOfNull, writer, rIntDictionary, buffer, mask, workingTailPos);
                } else {
                    // increment
                    genWriteIntegerUnsignedIncrementOptional(target, source, rbPos, valueOfNull, writer, rIntDictionary, buffer, mask, workingTailPos);
                }
            } else {
                // default
                int idx = token & intInstanceMask;
                int constDefault = rIntDictionary[idx];

                genWriteIntegerUnsignedDefaultOptional(source, rbPos, valueOfNull, constDefault, writer, buffer, mask, workingTailPos, rIntDictionary);
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
                genWriteBytesDefaultOptional(rbPos, writer, rbRingBuffer);
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
                genWriteBytesDefault(rbPos, writer, rbRingBuffer);
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
                
//                int[] buffer = rbRingBuffer==null?null:rbRingBuffer.buffer;
//                int mask = rbRingBuffer==null?0:rbRingBuffer.mask;
//                PaddedLong workingTailPos = rbRingBuffer==null?null:rbRingBuffer.workingTailPos;
                
                genWriteTextDefaultOptional(fieldPos, writer, rbRingBuffer);

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
                genWriteTextDefault(fieldPos, writer, rbRingBuffer);
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

    
    private static Map<String, AtomicInteger> tokenCount = new HashMap<String, AtomicInteger>();
    
    
    private boolean dispatchWriteByToken(PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {

        int token = fullScript[activeScriptCursor];
       
        assert (gatherWriteData(writer, token, activeScriptCursor, fieldPos, rbRingBuffer));
        
        //gather most common tokens together
//        String key = TokenBuilder.tokenToString(token);
//        key = key.substring(0, key.lastIndexOf('/'));
//        
//      //  System.err.println(key);
//        if (!tokenCount.containsKey(key)) {
//            tokenCount.put(key, new AtomicInteger(1));
//        } else {
//            tokenCount.get(key).incrementAndGet();
//        }       
//        System.err.println();
//        String biggestKey = "";
//        int biggestValue = Integer.MIN_VALUE;
//        for (Entry<String, AtomicInteger> entry:tokenCount.entrySet()) {
//            System.err.println(entry);
//            
////            if (entry.getValue().get() > biggestValue) {
////                biggestValue = entry.getValue().get();
////                biggestKey = entry.getKey();
////            }
//        }
        
     //  System.err.println(biggestValue+" "+biggestKey);
//                Decimal:001100/Default:000011=2
//                IntegerSignedOptional:000011/Delta:000100=2
//                IntegerUnsignedOptional:000001/Default:000011=8 ***
//                IntegerUnsigned:000000/Constant:000010=4 ***** NOTHING TO DO
//                Group:010000/Open:Seq:PMap::001100=4
//                LongUnsigned:000100/None:000000=2
//                DecimalOptional:001101/Default:000011=2
//                ASCIIOptional:001001/None:000000=2
//                ASCII:001000/Constant:000010=16   ***** NOTHING TO DO
//                Group:010000/Close:Seq:PMap::001101=4
//                Length:010100/None:000000=4
//                IntegerUnsigned:000000/None:000000=12   ***** DONE
//                Group:010000/Open:DynTempl::000010=6  ****
//                ASCII:001000/Copy:000001=2 //TODO: B, should copy also check bit flags for write?
//                IntegerUnsignedOptional:000001/None:000000=2
//                IntegerUnsigned:000000/Default:000011=2
//                LongUnsignedOptional:000101/None:000000=2
//                Dictionary:011000/Reset:000000=2
//                IntegerUnsigned:000000/Copy:000001=6  *****
//                IntegerUnsigned:000000/Increment:000101=2
//                ASCIIOptional:001001/Default:000011=10  **** DONE
//                IntegerUnsignedOptional:000001/Delta:000100=2  
        
        
        
        
    ///   System.err.println((writer.totalWritten(writer)+writer.limit)+" Write: "+TokenBuilder.tokenToString(token));
       
       
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
                            activeScriptCursor++;
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
                        if (isTemplate) {
                            
                            //must start at a location after the preamble and templateId.
                            fieldPos = RingBuffers.getFrom(ringBuffers).templateOffset+1;
                            
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
                        acceptIntegerUnsigned(token, fieldPos, rbRingBuffer, writer);
                    } else {
                        // optional
                        //TODO: B, Add lookup for value of absent/null instead of this constant.
                        int valueOfNull = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                        acceptIntegerUnsignedOptional(token, valueOfNull, fieldPos, rbRingBuffer, writer);
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
                                        genWriteDictionaryIntegerReset(idx,intInit[idx], rIntDictionary);
                                    }
                                } else {
                                    // long
                                    while (m < limit && (idx = members[m++]) >= 0) {
                                        genWriteDictionaryLongReset(idx, longInit[idx], rLongDictionary);
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
                                    genWriteDecimalNoneOptionalNone(exponentTarget, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                                } else {
                                    // none delta
                                    genWriteDecimalNoneOptionalDelta(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                                }
                            } else {
                                // none constant
                                genWriteDecimalNoneOptionalConstant(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, this);
                            }

                        } else {
                            // copy, default, increment
                            if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                                // copy, increment
                                if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                    // none copy
                                    genWriteDecimalNoneOptionalCopy(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                                } else {
                                    // none increment
                                    genWriteDecimalNoneOptionalIncrement(exponentTarget, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                                }
                            } else {
                                // none default
                                long mantissaConstDefault = rLongDictionary[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                                genWriteDecimalNoneOptionalDefault(exponentTarget, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, rIntDictionary, rbRingBuffer, this);
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
                                genWriteDecimalDeltaOptionalNone(exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            } else {
                                // delta delta
                                genWriteDecimalDeltaOptionalDelta(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            }
                        } else {
                            // delta constant
                            genWriteDecimalDeltaOptionalConstant(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, this);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // delta copy
                                genWriteDecimalDeltaOptionalCopy(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            } else {
                                // delta increment
                                genWriteDecimalDeltaOptionalIncrement(exponentTarget, mantissaSource, mantissaTarget, exponentSource, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            }
                        } else {
                            // delta default
                            long mantissaConstDefault = rLongDictionary[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalDeltaOptionalDefault(exponentTarget, mantissaTarget, exponentSource, exponentValueOfNull, mantissaConstDefault, rbPos, writer, rIntDictionary, rbRingBuffer, this);
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
                            genWriteDecimalConstantOptionalNone(exponentValueOfNull, mantissaTarget, rbPos, writer, rbRingBuffer, rLongDictionary, this);
                        } else {
                            // constant delta
                            genWriteDecimalConstantOptionalDelta(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer, rLongDictionary, this);
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
                            genWriteDecimalConstantOptionalCopy(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer, rLongDictionary, this);
                        } else {
                            // constant increment
                            genWriteDecimalConstantOptionalIncrement(exponentValueOfNull, mantissaSource, mantissaTarget, rbPos, writer, rbRingBuffer, rLongDictionary, this);
                        }
                    } else {
                        // constant default
                        long mantissaConstDefault = rLongDictionary[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
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
                                genWriteDecimalCopyOptionalNone(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            } else {
                                // copy delta
                                genWriteDecimalCopyOptionalDelta(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            }
                        } else {
                            // copy constant
                            genWriteDecimalCopyOptionalConstant(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, this);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // copy copy
                                genWriteDecimalCopyOptionalCopy(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            } else {
                                // copy increment
                                genWriteDecimalCopyOptionalIncrement(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            }
                        } else {
                            // copy default
                            long mantissaConstDefault = rLongDictionary[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalCopyOptionalDefault(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, rIntDictionary, rbRingBuffer, this);
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
                                genWriteDecimalIncrementOptionalNone(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            } else {
                                // increment delta
                                genWriteDecimalIncrementOptionalDelta(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            }
                        } else {
                            // increment constant
                            genWriteDecimalIncrementOptionalConstant(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, this);
                        }

                    } else {
                        // copy, default, increment
                        if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                            // copy, increment
                           // int target = (mantissaToken & longInstanceMask);
                          //  int source = readFromIdx > 0 ? readFromIdx & longInstanceMask : target;
                            if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                                // increment copy
                                genWriteDecimalIncrementOptionalCopy(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            } else {
                                // increment increment
                                genWriteDecimalIncrementOptionalIncrement(exponentTarget, exponentSource, mantissaSource, mantissaTarget, exponentValueOfNull, rbPos, writer, rIntDictionary, rbRingBuffer, rLongDictionary, this);
                            }
                        } else {
                            // increment default
                            long mantissaConstDefault = rLongDictionary[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                            genWriteDecimalIncrementOptionalDefault(exponentTarget, exponentSource, mantissaTarget, exponentValueOfNull, mantissaConstDefault, rbPos, writer, rIntDictionary, rbRingBuffer, this);
                        }
                    }
                }
            } else {
                // default
                int exponentConstDefault = rIntDictionary[expoToken & intInstanceMask]; //this is a runtime constant so we look it up here
                
                if (0 == (mantissaToken & (1 << TokenBuilder.SHIFT_OPER))) {
                    // none, constant, delta
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // none, delta
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // default none                            
                            genWriteDecimalDefaultOptionalNone(exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, rLongDictionary, rIntDictionary, this);
                        } else {
                            
                          // default delta
                            genWriteDecimalDefaultOptionalDelta(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, rLongDictionary, rIntDictionary, this);
                        }
                    } else {
                        // default constant
                        genWriteDecimalDefaultOptionalConstant(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, rIntDictionary, this);
                    }

                } else {
                    // copy, default, increment
                    if (0 == (mantissaToken & (2 << TokenBuilder.SHIFT_OPER))) {
                        // copy, increment
                        if (0 == (mantissaToken & (4 << TokenBuilder.SHIFT_OPER))) {
                            // default copy
                            genWriteDecimalDefaultOptionalCopy(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, rLongDictionary, this);
                        } else {
                            // default increment
                            genWriteDecimalDefaultOptionalIncrement(exponentSource, mantissaSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, rbPos, writer, rbRingBuffer, rLongDictionary, this);
                        }
                    } else {
                        // default default
                        long mantissaConstDefault = rLongDictionary[mantissaToken & longInstanceMask];//this is a runtime constant so we look it up here
                        genWriteDecimalDefaultOptionalDefault(exponentSource, mantissaTarget, exponentConstDefault, exponentValueOfNull, mantissaConstDefault, rbPos, writer, rbRingBuffer, rIntDictionary, this);
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

    
    private void openMessage(int token, int pmapSize, int fieldPos,  PrimitiveWriter writer, FASTRingBuffer queue) {
        assert (token < 0);
        assert (0 == (token & (OperatorMask.Group_Bit_Close << TokenBuilder.SHIFT_OPER)));
        assert (0 != (token & (OperatorMask.Group_Bit_Templ << TokenBuilder.SHIFT_OPER)));

        //add 1 bit to pmap and write the templateId
      //  System.err.println("open msg");
        int[] buffer = queue==null?null:queue.buffer;
        int mask = queue==null?0:queue.mask;
        PaddedLong workingTailPos = queue==null?null:queue.workingTailPos;
        
        genWriteOpenTemplatePMap(pmapSize, fieldPos, writer, buffer, mask, workingTailPos, this);
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
        //called only once when generating to create the needed method for beginning of messages
        fieldPos = 0;
        
        if (preambleData.length != 0) {
            int[] buffer = ringBuffer==null?null:ringBuffer.buffer;
            int mask = ringBuffer==null?0:ringBuffer.mask;
            PaddedLong workingTailPos = ringBuffer==null?null:ringBuffer.workingTailPos;
            
            genWritePreamble(fieldPos, writer, buffer, mask, workingTailPos, this);
            
            fieldPos += (preambleData.length+3)>>2;//must adjust this because it is meta data and when generating it will be used.
        };

        // template processing (can these be nested?)
      //  int templateId = FASTRingBufferReader.readInt(ringBuffer, idx);
        
        fieldPos++;  
    }
    
    @Override
    public void runFromCursor() {
         //NOTE: openMessage has fieldPos set based on constant in encode, if not this default value will stand
         fieldPos = 0;//value only needed for start of fragments that are NOT messages
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
    public void encode(PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        
        //set the cursor positions for the interpreter if we are not generating code
        if (null!=rbRingBuffer) {
            //cursor and limit already set
            setActiveScriptCursor(rbRingBuffer.consumerData.cursor); 
            setActiveScriptLimit(rbRingBuffer.consumerData.cursor + rbRingBuffer.fragmentSteps());
            fieldPos = 0;//needed for fragments in interpreter but is not called when generating
        }
        
        //start new message with preamble if needed        
        if (null!=rbRingBuffer && rbRingBuffer.consumerData.isNewMessage()) {                
            callBeginMessage(writer, rbRingBuffer);
        }
        
        //loop over every cursor position and dispatch to do the right activity
        int stop = activeScriptLimit;         
        while (activeScriptCursor<stop) { 
            if (dispatchWriteByToken(writer,rbRingBuffer)) {
                break;//for stops for fragments in the middle of a message
            };
            activeScriptCursor++; 
        }
    }

    @Override
    public int scriptLength() {
        return fullScript.length;
    }

 
}
