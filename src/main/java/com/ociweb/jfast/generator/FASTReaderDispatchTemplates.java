package com.ociweb.jfast.generator;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBuffer.PaddedLong;

//TODO: A, needs support for messageRef where we can inject template in another and return to the previouslocation. Needs STACK in dispatch!
//TODO: Z, can we send catalog in-band as a byteArray to push dynamic changes,  Need a unit test for this.
//TODO: B, set the default template for the case when it is undefined in catalog.
//TODO: C, Must add unit test for message length field start-of-frame testing, FrameLength bytes to read before decoding, is before pmap/templateId
//TODO: D, perhaps frame support is related to buffer size in primtive write so the right number of bits can be set.
//TODO: X, Add un-decoded field option so caller can deal with the subtraction of optionals.
//TODO: X, constants do not need to be written to ring buffer they can be de-ref by the reading static method directly.

//TODO: X, Send Amazon gift card to anyone who can supply another software based project, template, and example file that can run faster than this implementation. (One per project)

//TODO: A, UTF-8 Takes bytes count not char count, MUST confirm this is implemented correctly.

//TODO: T, Document, the fact that anything at the end is ignored and can be injected runtime references.

public abstract class FASTReaderDispatchTemplates extends FASTDecoder {

   /**
    * This class holds all the atomic field logic that will be directly called from the interpreter decoder 
    * and will be copied into the compiled decoder.  Note that when the copy is made the first arguments, as
    * determined by the generator class will be replaced by constants in place.  This will allow for simple 
    * math against those to be compiled away when the class is generated.
    * 
    * Each each method that requires local variables must also add a protective { } scope to eliminate 
    * Variable name collisions and simplify the code generator.
    * 
    * The benefit of this approach is to allow for full testing of each atomic field in the interpreter before
    * it is injected into the dynamically constructed class.  Testing should include both correct behavior and 
    * performance.
    * 
    * @param catalog
    */
    public FASTReaderDispatchTemplates(TemplateCatalogConfig catalog) {
        super(catalog);
    }
    
    public FASTReaderDispatchTemplates(byte[] catBytes) {
        super(new TemplateCatalogConfig(catBytes));
    }
    

    protected void genReadCopyText(int source, int target, TextHeap textHeap) {
        textHeap.copy(source,target);
    }

    protected void genReadCopyBytes(int source, int target, ByteHeap byteHeap) {
        byteHeap.copy(source,target);
    }
    
    // each sequence will need to repeat the pmap but we only need to push
    // and pop the stack when the sequence is first encountered.
    // if count is zero we can pop it off but not until then.
    protected void genReadSequenceClose(int backvalue, int topCursorPos, FASTDecoder dispatch) {
       
        if (dispatch.sequenceCountStackHead >= 0) {        
            if (--dispatch.sequenceCountStack[dispatch.sequenceCountStackHead] < 1) {
                // this group is a sequence so pop it off the stack.
                --dispatch.sequenceCountStackHead;
                // finished this sequence so leave pointer where it is               
            } else {                  
                // do this sequence again so move pointer back                
                dispatch.neededSpaceOrTemplate = 1 + (backvalue << 2);
                dispatch.activeScriptCursor = topCursorPos;    
            }
        }
    }
    
    protected void genReadGroupPMapOpen(int nonTemplatePMapSize, PrimitiveReader reader) {
        PrimitiveReader.openPMap(nonTemplatePMapSize, reader);
    }

    protected void genReadGroupClose(PrimitiveReader reader) {
        PrimitiveReader.closePMap(reader);
    }
    
    
    //length methods
    protected int genReadLengthDefault(int constDefault,  int jumpToTarget, int jumpToNext, int[] rbB, PrimitiveReader reader, int rbMask, PaddedLong rbPos, FASTDecoder dispatch) {
        {
            int length;
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, length = (PrimitiveReader.popPMapBit(reader) == 0 ? constDefault : PrimitiveReader.readIntegerUnsigned(reader)));
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                return jumpToTarget;
              
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                return jumpToNext;
               
           }
        }
    }

    //TODO: C, once this all works find a better way to inline it with only 1 conditional.
    
    
    protected void genReadLengthIncrement(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length;
            int value = length = (PrimitiveReader.popPMapBit(reader) == 0 ? (rIntDictionary[target] = rIntDictionary[source] + 1)
            : (rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader)));
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, value);
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                dispatch.activeScriptCursor = jumpToTarget;
               
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                dispatch.activeScriptCursor = jumpToNext;
               
           }
        }
    }

    protected void genReadLengthCopy(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length;
            int value = length = StaticGlue.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, value);
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                dispatch.activeScriptCursor = jumpToTarget;
               
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                dispatch.activeScriptCursor = jumpToNext;
                
           }
        }
    }

    protected void genReadLengthConstant(int constDefault,  int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, PaddedLong rbPos, FASTDecoder dispatch) {
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
        if (constDefault == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            dispatch.activeScriptCursor = jumpToTarget;
            
        } else {
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = constDefault;
            dispatch.activeScriptCursor = jumpToNext;
            
       }
    }

    protected void genReadLengthDelta(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length = (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader)));
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, length);
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                dispatch.activeScriptCursor = jumpToTarget;
                
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                dispatch.activeScriptCursor = jumpToNext;
                
           }
        }
    }

    protected void genReadLength(int target,  int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length;
   
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = length = PrimitiveReader.readIntegerUnsigned(reader));
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                dispatch.activeScriptCursor = jumpToTarget;
                
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                dispatch.activeScriptCursor = jumpToNext;
                
           }
        }
    }
    
   
    // int methods

    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
        } else {
           int value = PrimitiveReader.readIntegerUnsigned(reader)-1;
           int mask = value>>31;
           FASTRingBuffer.addValue(rbB, rbMask, rbPos, (constAbsent&mask) | ((~mask)&value) );           
        }

    }
    //branching version
//            int value = PrimitiveReader.readIntegerUnsigned(reader);
//            if (0==value) {
//                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
//            } else {
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, value-1);
//            }
    
    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            if (rIntDictionary[target] == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
            }
        } else {
            int value;
            if ((value = PrimitiveReader.readIntegerUnsigned(reader)) == 0) {
                rIntDictionary[target] = 0;
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
            }
        }
    }

    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = StaticGlue.readIntegerUnsignedCopy(target, source, rIntDictionary, reader)-1;
            int mask = value>>31;
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, (constAbsent&mask) | ((~mask)&value) );
        }
    }
//branching version            
//            int xi1 = PrimitiveReader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
//            if (0 == xi1) {
//                //FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
//            } else {                
//                //FASTRingBuffer.addValue(rbB,rbMask,rbPos, xi1 - 1);
//            }

    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst));
    }

    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            
            if (0 == value) {
                rIntDictionary[target] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                //FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value)));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + ((value-1)+((value-1)>>63))));
            
            }
        }
    }

    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerUnsigned(reader)-1;
            int mask = value>>31;
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, (constAbsent&mask) | ((~mask)&value) );  
                                    
        }
    }
    //branched      
//            int xi1 = PrimitiveReader.readIntegerUnsigned(reader);
//            if (0==xi1) {
//                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
//            } else {                
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, xi1 - 1);
//            }

    protected void genReadIntegerUnsignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (PrimitiveReader.popPMapBit(reader) == 0 ? constDefault : PrimitiveReader.readIntegerUnsigned(reader)));
    }

    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (PrimitiveReader.popPMapBit(reader) == 0 ? (rIntDictionary[target] = rIntDictionary[source] + 1)
        : (rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader))));
    }

    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, StaticGlue.readIntegerUnsignedCopy(target, source, rIntDictionary, reader));
    }

    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, constDefault);
    }

    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader))));
    }

    protected void genReadIntegerUnsigned(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader));
    }

    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (PrimitiveReader.popPMapBit(reader) == 0 ? constDefault : PrimitiveReader.readIntegerSigned(reader)));
    }

    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (PrimitiveReader.popPMapBit(reader) == 0 ? (rIntDictionary[target] = rIntDictionary[source] + 1)
        : (rIntDictionary[target] = PrimitiveReader.readIntegerSigned(reader))));
    }

    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, StaticGlue.readIntegerSignedCopy(target, source, rIntDictionary, reader));
    }

    protected void genReadIntegerConstant(int constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, constDefault);
    }

    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader))));
    }

    protected void genReadIntegerSignedNone(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = PrimitiveReader.readIntegerSigned(reader));
    }

    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
        } else {
            int value = PrimitiveReader.readIntegerSigned(reader);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos,  value == 0 ? constAbsent : (value > 0 ? value - 1 : value));
        }
    }


    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] == 0 ? constAbsent : (rIntDictionary[target] = rIntDictionary[source] + 1)));
        } else {
            int value;
            if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                rIntDictionary[target] = 0;
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
            }
        }
    }
    

    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int xi1 = StaticGlue.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (xi1 > 0 ? xi1 - 1 : xi1));
            }
        }
    }

    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst));
    }
    
    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rIntDictionary[target] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                //FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value)));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + ((value-1)+((value-1)>>63))));
            }
        }
    }

    
    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0 == value) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, value > 0 ? value - 1 : value);
                
            }
        }
    }

   
     
    
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //optional decimals//////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    /*
     * From the spec:
     * 
    Decimal fields with individual operators have the following utilization:
        • If the decimal has mandatory presence, the exponent and mantissa fields are treated as two
        separate mandatory integer fields as described above.
        • If the decimal has optional presence, the exponent field is treated as an optional integer field and
        the mantissa field is treated as a mandatory integer field. The presence of the mantissa field and
        any related bits in the presence map are dependent on the presence of the exponent. The
        mantissa field appears in the stream iff the exponent value is considered present. If the mantissa
        has an operator that requires a bit in the presence map, this bit is present iff the exponent value is
        considered present.
    */
    
    //The parser will ensure that the second token used for the decimal will never be of type optional.
    //The code here will skip over reading the pmap bit and data if the exponent is "Absent"
    
    
    //default
    
    protected void genReadDecimalDefaultOptionalMantissaDefault(int constAbsent, int constDefault, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {            
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));
                }
            }            
            //Long signed default
            if (0==PrimitiveReader.popPMapBit(reader)) {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
            } else {
                long tmpLng = PrimitiveReader.readLongSigned(reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }            
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {   
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                if (0==rIntDictionary[target]) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            //Long signed default
            if (0==PrimitiveReader.popPMapBit(reader)) {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
            } else {
                long tmpLng = PrimitiveReader.readLongSigned(reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
         
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int xi1 = StaticGlue.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (xi1 > 0 ? xi1 - 1 : xi1));
                //Long signed default
                if (0==PrimitiveReader.popPMapBit(reader)) {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
                }
            }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaDefault(int constAbsent, int constConst, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbPos.value+=2;
        } else {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constConst);
            //Long signed default
           if (0==PrimitiveReader.popPMapBit(reader)) {
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
           } else {
               long tmpLng = PrimitiveReader.readLongSigned(reader);
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
           }
        }
        
    }
    protected void genReadDecimalDeltaOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value)));
                //Long signed default
                if (0==PrimitiveReader.popPMapBit(reader)) {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
                }
            }
        }
    }
    
    protected void genReadDecimalOptionalMantissaDefault(int constAbsent, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));                    
              //Long signed default
                if (0==PrimitiveReader.popPMapBit(reader)) {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
                }
            }
            
        }
    }

    
    
    //increment
    
    protected void genReadDecimalDefaultOptionalMantissaIncrement(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, value > 0 ? value - 1 : value);
                }
            }
            //Long signed increment
            long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                if (0==rIntDictionary[target]) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            //Long signed increment
            long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF)); 
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int xi1 = StaticGlue.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (xi1 > 0 ? xi1 - 1 : xi1));
                //Long signed increment
                long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaIncrement(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constConst);
                //Long signed increment
               long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }

    }
    protected void genReadDecimalDeltaOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value)));
                //Long signed increment
                long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaIncrement(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));                    
                //Long signed increment
               long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
     
        }
    }
    
    //copy
    
    protected void genReadDecimalDefaultOptionalMantissaCopy(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {            
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));
                }                
            }
            //Long signed copy
            long tmpLng=StaticGlue.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));

        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[target]) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            
            //Long signed copy
            long tmpLng=StaticGlue.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }    
            
         
    }
    
    protected void genReadDecimalCopyOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int xi1 = StaticGlue.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (xi1 > 0 ? xi1 - 1 : xi1));
                //Long signed copy
                long tmpLng=StaticGlue.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
              }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaCopy(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constConst);
                //Long signed copy
               long tmpLng=StaticGlue.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }             

    }
    protected void genReadDecimalDeltaOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value)));
                //Long signed copy
                long tmpLng=StaticGlue.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaCopy(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));                    
                //Long signed copy
               long tmpLng=StaticGlue.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
                    
        }
    }
    
    //constant
    
    protected void genReadDecimalDefaultOptionalMantissaConstant(int constAbsent, int constDefault, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));
                }                
            }
            //Long signed constant
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[target]) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            
                //Long signed constant
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int xi1 = StaticGlue.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (xi1 > 0 ? xi1 - 1 : xi1));
                //Long signed constant
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
             }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaConstant(int constAbsent, int constConst, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constConst);
                //Long signed constant
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
               FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
            } 
            
    }
      
    protected void genReadDecimalDeltaOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value)));
                //Long signed constant
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
            }
            
        }
    }

    protected void genReadDecimalOptionalMantissaConstant(int constAbsent, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));                    
                //Long signed constant
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault & 0xFFFFFFFF));
            }
                        
        }
    }
    
    //delta

    protected void genReadDecimalDefaultOptionalMantissaDelta(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, long[] rLongDictionary, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos,  (value-1)+((value-1)>>31)   );// Branchless
                   // FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));
                }                
            }
            //Long signed delta
            long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }
    }
    //branching example for the code above
    //FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));
    
    protected void genReadDecimalIncrementOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[target]) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            
            //Long signed delta
            long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            int xi1 = StaticGlue.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (xi1 > 0 ? xi1 - 1 : xi1));
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }           
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaDelta(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbPos.value+=2;
        } else {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constConst);
            //Long signed delta
           long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
           FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
           FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        } 
        
    }
    protected void genReadDecimalDeltaOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value)));
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaDelta(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));                    
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    //none

    protected void genReadDecimalDefaultOptionalMantissaNone(int constAbsent, int constDefault, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));
                }                
            }
            //Long signed none
            long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaNone(int expoTarget, int expoSource, int expoConstAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[expoTarget]) {
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[expoTarget] = rIntDictionary[expoSource] + 1));
                }        
               
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[expoTarget] = 0;
                    FASTRingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    FASTRingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[expoTarget] = value) - 1);
                }
            }
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
           
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaNone(int expoTarget, int expoSource, int expoConstAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            int xi1 = StaticGlue.readIntegerSignedCopy(expoTarget, expoSource, rIntDictionary, reader);
            if (0==xi1) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (xi1 > 0 ? xi1 - 1 : xi1));
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
      
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaNone(int expoConstAbsent, int expoConstConst, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbPos.value+=2;
        } else {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, expoConstConst);
            //Long signed none
           long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
           FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
           FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        } 

    }
    protected void genReadDecimalDeltaOptionalMantissaNone(int expoTarget, int expoSource, int expoConstAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[expoTarget] = 0;// set to absent
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[expoTarget] = (int) (rIntDictionary[expoSource] + (value > 0 ? value - 1 : value)));
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaNone(int expoConstAbsent, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));                    
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }                      
    
        }
    }    
        
    /////////////////////////////////////////////////////////////////////////////
    /////end ////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
    
    
    
    // long methods

    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? constDefault : PrimitiveReader.readLongUnsigned(reader));
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    protected void genReadLongUnsignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[idx] = rLongDictionary[source] + 1)
        : (rLongDictionary[idx] = PrimitiveReader.readLongUnsigned(reader)));
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    protected void genReadLongUnsignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long tmpLng=StaticGlue.readLongUnsignedCopy(idx, source, rLongDictionary, reader);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    protected void genReadLongConstant(long constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault & 0xFFFFFFFF));
    }

    protected void genReadLongUnsignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    protected void genReadLongUnsignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long tmpLng=rLongDictionary[idx] = PrimitiveReader.readLongUnsigned(reader);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long result;
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            result = constDefault;
        } else {
            long value = PrimitiveReader.readLongUnsigned(reader);
            if (0==value) {
                result = constAbsent;
            } else {
                result = value -1;
            }
        }
        long tmpLng=result;
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    protected void genReadLongUnsignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            if (0 == rLongDictionary[idx]) {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[idx] = rLongDictionary[source] + 1);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
        } else {
            long value;
            if ((value = PrimitiveReader.readLongUnsigned(reader)) == 0) {
                rLongDictionary[idx] = 0;
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[idx] = value) - 1;
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
        }

    }

    protected void genReadLongUnsignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
        long xl1;
        long tmpLng=(0 == (xl1 = StaticGlue.readLongUnsignedCopy(idx, source, rLongDictionary, reader)) ? constAbsent : xl1 - 1);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
        } else {
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constConst >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constConst & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rLongDictionary[idx] = 0;// set to absent
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = rLongDictionary[idx] = (rLongDictionary[source] + (value > 0 ? value - 1 : value));
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
        }
    }

    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongUnsigned(reader)-1;
            long mask = value>>63;
            long result = (constAbsent&mask) | ((~mask)&value);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (result >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (result & 0xFFFFFFFF));
            
        }
    }
    //branched version
//            long value = PrimitiveReader.readLongUnsigned(reader);
//            if (0==value) {
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
//            } else {
//                long tmpLng = value-1;
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
//            }

    protected void genReadLongSignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault & 0xFFFFFFFF));
        } else {
            long tmpLng= PrimitiveReader.readLongSigned(reader);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }        
    }

    protected void genReadLongSignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=(PrimitiveReader.popPMapBit(reader) == 0 ? (rLongDictionary[idx] = rLongDictionary[source] + 1) : (rLongDictionary[idx] = PrimitiveReader.readLongSigned(reader)));
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongSignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=StaticGlue.readLongSignedCopy(idx, source, rLongDictionary, reader);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongSignedConstant(long constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault & 0xFFFFFFFF));
    }

    protected void genReadLongSignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }
  
    protected void genReadLongSignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        long tmpLng=rLongDictionary[idx] = PrimitiveReader.readLongSigned(reader);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
    }

    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault & 0xFFFFFFFF));
        } else {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (value > 0 ? value - 1 : value);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
        }
    }

    protected void genReadLongSignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) { 

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            if (0 == rLongDictionary[idx]) {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[idx] = rLongDictionary[source] + 1);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
        } else {
            long value;
            if ((value = PrimitiveReader.readLongSigned(reader)) == 0) {
                rLongDictionary[idx] = 0;
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[idx] = value) - 1;
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
        }

    }

    protected void genReadLongSignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long xl1 = StaticGlue.readLongSignedCopy(idx, source, rLongDictionary, reader);
            if (0 == xl1) {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));   
            } else {
                long tmpLng=  xl1>0? xl1 - 1:xl1;
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }
        }
    }

    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0==PrimitiveReader.popPMapBit(reader)) {
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
        } else {
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constConst >>> 32)); 
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constConst & 0xFFFFFFFF));
        }
    }

    protected void genReadLongSignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rLongDictionary[idx] = 0;// set to absent
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                StaticGlue.readLongSignedDeltaOptional(idx, source, rLongDictionary, rbB, rbMask, rbPos, value);
            }
        }
    }

    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng= (value > 0 ? value - 1 : value);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32)); 
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng & 0xFFFFFFFF));
            }        
        }
    }

    // text methods.

    
    protected void genReadUTF8None(int idx, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadUTF8TailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim == 0) {
            TextHeap.setNull(idx, textHeap);
        } else {
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            int t = trim - 1;
            StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, t);
        }
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadUTF8DeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        if (0 == trim) {
            TextHeap.setNull(idx, textHeap);
        } else {
            StaticGlue.allocateAndDeltaUTF8(idx, textHeap, reader, trim>0?trim-1:trim);
        }
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }
    
    protected void genReadUTF8Delta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        StaticGlue.allocateAndDeltaUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerSigned(reader));
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }
    
    protected void genReadASCIITail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        StaticGlue.readASCIITail(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader));
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, constIdx);
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, constLen);
    }

    protected void genReadASCIIDelta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        if (trim >=0) {
            StaticGlue.readASCIITail(idx, textHeap, reader, trim); 
        } else {
            StaticGlue.readASCIIHead(idx, trim, textHeap, reader);
        }
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadASCIICopy(int idx, int rbMask, int[] rbB, PrimitiveReader reader, TextHeap textHeap, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
            int len = (PrimitiveReader.popPMapBit(reader)!=0) ? StaticGlue.readASCIIToHeap(idx, reader, textHeap) : textHeap.valueLength(idx);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }
    
    protected void genReadASCIICopyOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
            int len = (PrimitiveReader.popPMapBit(reader) != 0) ? StaticGlue.readASCIIToHeap(idx, reader, textHeap) : textHeap.valueLength(idx);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadUTF8Tail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
            int trim = PrimitiveReader.readIntegerSigned(reader);
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            
            StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, trim);
            int len = textHeap.valueLength(idx);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }


    protected void genReadUTF8Copy(int idx, int optOff, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
        }
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadUTF8Default(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, defIdx);
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, defLen);
        } else {
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
            int len = textHeap.valueLength(idx);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
        }
    }

    protected void genReadASCIINone(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        byte val;
        int tmp;
        if (0 != (tmp = 0x7F & (val = PrimitiveReader.readTextASCIIByte(reader)))) {
            tmp=StaticGlue.readASCIIToHeapValue(idx, val, tmp, textHeap, reader);
        } else {
            tmp=StaticGlue.readASCIIToHeapNone(idx, val, textHeap, reader);
        }
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, tmp, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, tmp);
    }

    protected void genReadASCIITailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        int tail = PrimitiveReader.readIntegerUnsigned(reader);
        if (0 == tail) {
            TextHeap.setNull(idx, textHeap);
        } else {
           StaticGlue.readASCIITail(idx, textHeap, reader, tail-1);
        }
        int len = textHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }
    
    protected void genReadASCIIDeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        //TODO: B, extract the constant length from here.
        int optionalTrim = PrimitiveReader.readIntegerSigned(reader);
        int tempId = (0 == optionalTrim ? 
                         textHeap.initStartOffset( TextHeap.INIT_VALUE_MASK | idx) |TextHeap.INIT_VALUE_MASK : 
                         (optionalTrim > 0 ? StaticGlue.readASCIITail(idx, textHeap, reader, optionalTrim - 1) :
                                             StaticGlue.readASCIIHead(idx, optionalTrim, textHeap, reader)));
        int len = textHeap.valueLength(tempId);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(tempId, len, textHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0 != PrimitiveReader.popPMapBit(reader) ) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constInit);
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constInitLen);
        } else {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constValue);
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constValueLen);
        }
    }

    
    //TODO: C, perf problem. 6% in profiler, compiler should ONLY write back to heap IFF this field is read by another field.
    //this block is no longer in use however the  performance did not show up. so....

    protected void genReadASCIIDefault(int idx, int defIdx, int defLen, int rbMask, int[] rbB, PrimitiveReader reader, TextHeap textHeap, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, defIdx);
                FASTRingBuffer.addValue(rbB, rbMask, rbPos, defLen);
            } else {
                
                int lenTemp = PrimitiveReader.readTextASCIIIntoRing(rbRingBuffer.charBuffer,
                                                                    rbRingBuffer.addCharPos, 
                                                                    rbRingBuffer.charMask,
                                                                    reader);
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, rbRingBuffer.addCharPos);
                rbRingBuffer.addCharPos+=lenTemp;                
                FASTRingBuffer.addValue(rbB,rbMask,rbPos, lenTemp);
                
//                //TODO: AA: old code we only want if this default field is read from another, eg dictionary sharing.
//                int len = StaticGlue.readASCIIToHeap(idx, reader, textHeap);
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(idx, len, textHeap, rbRingBuffer));
//                FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
            }
    }
        
    protected void genReadBytesConstant(int constIdx, int constLen, int[] rbB, int rbMask, PaddedLong rbPos) {
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, constIdx);
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, constLen);
    }

    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        if (0 != PrimitiveReader.popPMapBit(reader) ) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constInit);
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constInitLen);
        } else {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constValue);
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, constValueLen);
        }
    }

    protected void genReadBytesDefault(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, defIdx);
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, defLen);
        } else {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
            int len = byteHeap.valueLength(idx);
            FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
            FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
        }
    }

    protected void genReadBytesCopy(int idx, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        }
        int len = byteHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadBytesDeltaOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        if (0 == trim) {
            byteHeap.setNull(idx);
        } else {
            if (trim > 0) {
                trim--;// subtract for optional
            }        
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);        
            if (trim >= 0) {
                PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength, reader);
            } else {
                PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength, reader);
            }
        }
        int len = byteHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadBytesTailOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim == 0) {
            byteHeap.setNull(idx);
        } else {
            trim--;
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            // append to tail
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength, reader);
        }
        int len = byteHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);

    }

    protected void genReadBytesDelta(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim >= 0) {
            // append to tail
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForAppend(idx, trim, utfLength), utfLength, reader);
        } else {
            // append to head
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.makeSpaceForPrepend(idx, -trim, utfLength), utfLength, reader);
        }
        int len = byteHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }
    
    
    protected void genReadBytesTail(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        int length = PrimitiveReader.readIntegerUnsigned(reader);
        
        // append to tail
        int targetOffset = byteHeap.makeSpaceForAppend(idx, trim, length);
        PrimitiveReader.readByteData(byteHeap.rawAccess(), targetOffset, length, reader);
        int len = byteHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    
    protected void genReadBytesNoneOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int length = PrimitiveReader.readIntegerUnsigned(reader) - 1;
        PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        int len = byteHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    protected void genReadBytesNone(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int length = PrimitiveReader.readIntegerUnsigned(reader) - 0;
        PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        int len = byteHeap.valueLength(idx);
        FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap, rbRingBuffer));
        FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
    }

    // dictionary reset

    protected void genReadDictionaryBytesReset(int target, ByteHeap byteHeap) {
        byteHeap.setNull(target);
    }

    protected void genReadDictionaryTextReset(int target, TextHeap textHeap) {
        textHeap.reset(target);
    }

    protected void genReadDictionaryLongReset(int target, long resetConst, long[] rLongDictionary) {
        rLongDictionary[target] = resetConst;
    }

    protected void genReadDictionaryIntegerReset(int target, int resetConst, int[] rIntDictionary) {
        rIntDictionary[target] = resetConst;
    }

    //TODO: C, Need a way to stream to disk over gaps of time. Write FAST to a file and Write series of Dictionaries to another, this set is valid for 1 catalog.
    
    
}
