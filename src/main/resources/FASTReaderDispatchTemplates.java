package com.ociweb.jfast.generator;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTRingBuffer;

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
    public FASTReaderDispatchTemplates(TemplateCatalog catalog) {
        super(catalog);
    }
    
    public FASTReaderDispatchTemplates(byte[] catBytes) {
        super(new TemplateCatalog(catBytes));
    }
    
    //second constructor only needed for testing.
    protected FASTReaderDispatchTemplates(DictionaryFactory dcr, int nonTemplatePMapSize, int[][] dictionaryMembers,
            int maxTextLen, int maxVectorLen, int charGap, int bytesGap, int[] fullScript, int maxNestedGroupDepth,
            int primaryRingBits, int textRingBits, int maxPMapCountInBytes, int[] templateStartIdx, int[] templateLimitIdx, int stackPMapInBytes, int preambleSize) {
        super(dcr, nonTemplatePMapSize, dictionaryMembers, maxTextLen, maxVectorLen, charGap, bytesGap, fullScript, maxNestedGroupDepth,
                primaryRingBits,textRingBits,  maxPMapCountInBytes, templateStartIdx, templateLimitIdx,  stackPMapInBytes, preambleSize);
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
    protected int genReadLengthDefault(int constDefault,  int jumpToTarget, int jumpToNext, int[] rbB, PrimitiveReader reader, int rbMask, FASTRingBuffer rbRingBuffer, FASTDecoder dispatch) {
        {
            int length;
            rbB[rbMask & rbRingBuffer.addPos++] = length = PrimitiveReader.readIntegerUnsignedDefault(constDefault, reader);
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
    
    
    protected int genReadLengthIncrement(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length;
            int value = length = PrimitiveReader.readIntegerUnsignedIncrement(target, source, rIntDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = value;
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                return jumpToTarget;
               
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                return jumpToNext;
               
           }
        }
    }

    protected int genReadLengthCopy(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length;
            int value = length = PrimitiveReader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = value;
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                return jumpToTarget;
               
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                return jumpToNext;
                
           }
        }
    }

    protected int genReadLengthConstant(int constDefault,  int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, FASTDecoder dispatch) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
        if (constDefault == 0) {
            // jumping over sequence (forward) it was skipped (rare case)
            return jumpToTarget;
            
        } else {
            dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = constDefault;
            return jumpToNext;
            
       }
    }

    protected int genReadLengthDelta(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length = (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader)));
            rbB[rbMask & rbRingBuffer.addPos++] = length;
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                return jumpToTarget;
                
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                return jumpToNext;
                
           }
        }
    }

    protected int genReadLength(int target,  int jumpToTarget, int jumpToNext, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer, int[] rIntDictionary, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length;
   
            rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = length = PrimitiveReader.readIntegerUnsigned(reader);
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                return jumpToTarget;
                
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                return jumpToNext;
                
           }
        }
    }
    
   
    // int methods

    protected void genReadIntegerUnsignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
        } else {
            int value = PrimitiveReader.readIntegerUnsigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = value-1;
            }
        }

    }

    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            if (rIntDictionary[target] == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = rIntDictionary[source] + 1);
            }
        } else {
            int value;
            if ((value = PrimitiveReader.readIntegerUnsigned(reader)) == 0) {
                rIntDictionary[target] = 0;
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
            }
        }
    }

    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            int xi1 = PrimitiveReader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
            if (0 == xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;                
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = xi1 - 1;
            }
        }
    }

    protected void genReadIntegerUnsignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
    }

    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            
            if (0 == value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
            
            }
        }
    }

    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            int xi1 = PrimitiveReader.readIntegerUnsigned(reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = xi1 - 1;
            }
        }
    }

    protected void genReadIntegerUnsignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedDefault(constDefault, reader);
    }

    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedIncrement(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerUnsignedCopy(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
    }

    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader)));
    }

    protected void genReadIntegerUnsigned(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader);
    }

    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedDefault(constDefault, reader);
    }

    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedIncrement(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
    }

    protected void genReadIntegerConstant(int constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
    }

    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader)));
    }

    protected void genReadIntegerSignedNone(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = PrimitiveReader.readIntegerSigned(reader);
    }

    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
        } else {
            int value = PrimitiveReader.readIntegerSigned(reader);
            rbB[rbMask & rbRingBuffer.addPos++] =  value == 0 ? constAbsent : (value > 0 ? value - 1 : value);
        }
    }


    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] == 0 ? constAbsent : (rIntDictionary[target] = rIntDictionary[source] + 1));
        } else {
            int value;
            if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                rIntDictionary[target] = 0;
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
            }
        }
    }
    

    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            int xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (xi1 > 0 ? xi1 - 1 : xi1);
            }
        }
    }

    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (PrimitiveReader.popPMapBit(reader) == 0 ? constAbsent : constConst);
    }
    
    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
            }
        }
    }

    
    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {//TODO: AA, must remove rbRingBuffer.addPos++ and replace with array passed in OR constant local count.
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0 == value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = value > 0 ? value - 1 : value;
                
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
    
    protected void genReadDecimalDefaultOptionalMantissaDefault(int constAbsent, int constDefault, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {            
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);
                }
            }            
            //Long signed default
            if (0==PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
            } else {
                long tmpLng = PrimitiveReader.readLongSigned(reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }            
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {   
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                if (0==rIntDictionary[target]) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = rIntDictionary[source] + 1);
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
                }
            }
            //Long signed default
            if (0==PrimitiveReader.popPMapBit(reader)) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
            } else {
                long tmpLng = PrimitiveReader.readLongSigned(reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
         
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            
            int xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (xi1 > 0 ? xi1 - 1 : xi1);
                //Long signed default
                if (0==PrimitiveReader.popPMapBit(reader)) {
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
                }
            }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaDefault(int constAbsent, int constConst, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbRingBuffer.addPos+=2;
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = constConst;
            //Long signed default
           if (0==PrimitiveReader.popPMapBit(reader)) {
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
           } else {
               long tmpLng = PrimitiveReader.readLongSigned(reader);
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
           }
        }
        
    }
    protected void genReadDecimalDeltaOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
                //Long signed default
                if (0==PrimitiveReader.popPMapBit(reader)) {
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
                }
            }
        }
    }
    
    protected void genReadDecimalOptionalMantissaDefault(int constAbsent, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);                    
              //Long signed default
                if (0==PrimitiveReader.popPMapBit(reader)) {
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                    rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
                }
            }
            
        }
    }
    
    //increment
    
    protected void genReadDecimalDefaultOptionalMantissaIncrement(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = value > 0 ? value - 1 : value;
                }
            }
            //Long signed increment
            long tmpLng=PrimitiveReader.readLongSignedIncrement(mantissaTarget, mantissaSource, rLongDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                if (0==rIntDictionary[target]) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = rIntDictionary[source] + 1);
                }    
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
                }
            }
            //Long signed increment
            long tmpLng=PrimitiveReader.readLongSignedIncrement(mantissaTarget, mantissaSource, rLongDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF); 
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            
            int xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (xi1 > 0 ? xi1 - 1 : xi1);
                //Long signed increment
                long tmpLng=PrimitiveReader.readLongSignedIncrement(mantissaTarget, mantissaSource, rLongDictionary, reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaIncrement(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {

            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = constConst;
                //Long signed increment
               long tmpLng=PrimitiveReader.readLongSignedIncrement(mantissaTarget, mantissaSource, rLongDictionary, reader);
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }

    }
    protected void genReadDecimalDeltaOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
                //Long signed increment
                long tmpLng=PrimitiveReader.readLongSignedIncrement(mantissaTarget, mantissaSource, rLongDictionary, reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaIncrement(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);                    
                //Long signed increment
               long tmpLng=PrimitiveReader.readLongSignedIncrement(mantissaTarget, mantissaSource, rLongDictionary, reader);
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
     
        }
    }
    
    //copy
    
    protected void genReadDecimalDefaultOptionalMantissaCopy(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {            
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);
                }                
            }
            //Long signed copy
            long tmpLng=PrimitiveReader.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);

        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[target]) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = rIntDictionary[source] + 1);
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
                }
            }
            
            //Long signed copy
            long tmpLng=PrimitiveReader.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }    
            
         
    }
    
    protected void genReadDecimalCopyOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            int xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (xi1 > 0 ? xi1 - 1 : xi1);
                //Long signed copy
                long tmpLng=PrimitiveReader.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
              }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaCopy(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {

            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = constConst;
                //Long signed copy
               long tmpLng=PrimitiveReader.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }             

    }
    protected void genReadDecimalDeltaOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
                //Long signed copy
                long tmpLng=PrimitiveReader.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaCopy(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);                    
                //Long signed copy
               long tmpLng=PrimitiveReader.readLongSignedCopy(mantissaTarget, mantissaSource, rLongDictionary, reader);
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
                    
        }
    }
    
    //constant
    
    protected void genReadDecimalDefaultOptionalMantissaConstant(int constAbsent, int constDefault, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);
                }                
            }
            //Long signed constant
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[target]) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = rIntDictionary[source] + 1);
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
                }
            }
            
                //Long signed constant
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            int xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (xi1 > 0 ? xi1 - 1 : xi1);
                //Long signed constant
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
             }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaConstant(int constAbsent, int constConst, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = constConst;
                //Long signed constant
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
               rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
            } 
            
    }
      
    protected void genReadDecimalDeltaOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
                //Long signed constant
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
            }
            
        }
    }

    protected void genReadDecimalOptionalMantissaConstant(int constAbsent, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);                    
                //Long signed constant
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (mantissaConstDefault & 0xFFFFFFFF);
            }
                        
        }
    }
    
    //delta

    protected void genReadDecimalDefaultOptionalMantissaDelta(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, long[] rLongDictionary, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);
                }                
            }
            //Long signed delta
            long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[target]) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = rIntDictionary[source] + 1);
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
                }
            }
            
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            int xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (xi1 > 0 ? xi1 - 1 : xi1);
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }           
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaDelta(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbRingBuffer.addPos+=2;
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = constConst;
            //Long signed delta
           long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
           rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
           rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        } 
        
    }
    protected void genReadDecimalDeltaOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaDelta(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);                    
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
            
        }
    }
    
    //none

    protected void genReadDecimalDefaultOptionalMantissaNone(int constAbsent, int constDefault, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                rbB[rbMask & rbRingBuffer.addPos++] = constDefault;
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);
                }                
            }
            //Long signed none
            long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaNone(int target, int source, int constAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            if (PrimitiveReader.popPMapBit(reader) == 0) {
                
                if (0==rIntDictionary[target]) {
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = rIntDictionary[source] + 1);
                }        
               
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbRingBuffer.addPos+=2;
                    return;
                } else {
                    rbB[rbMask & rbRingBuffer.addPos++] = (rIntDictionary[target] = value) - 1;
                }
            }
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
           
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaNone(int target, int source, int constAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            int xi1 = PrimitiveReader.readIntegerSignedCopy(target, source, rIntDictionary, reader);
            if (0==xi1) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = (xi1 > 0 ? xi1 - 1 : xi1);
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
      
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaNone(int constAbsent, int constConst, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbRingBuffer.addPos+=2;
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = constConst;
            //Long signed none
           long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
           rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
           rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        } 

    }
    protected void genReadDecimalDeltaOptionalMantissaNone(int target, int source, int constAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {
                rbB[rbMask & rbRingBuffer.addPos++] = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaNone(int constAbsent, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer, long[] rLongDictionary) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = constAbsent;                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbRingBuffer.addPos+=2;
            } else {                
                rbB[rbMask & rbRingBuffer.addPos++] = (value > 0 ? value - 1 : value);                    
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }                      
    
        }
    }    
        
    /////////////////////////////////////////////////////////////////////////////
    /////end ////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
    
    
    
    // long methods

    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedDefault(constDefault, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedIncrement(idx, source, rLongDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=PrimitiveReader.readLongUnsignedCopy(idx, source, rLongDictionary, reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=rLongDictionary[idx] = PrimitiveReader.readLongUnsigned(reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
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
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongUnsignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            if (0 == rLongDictionary[idx]) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng = (rLongDictionary[idx] = rLongDictionary[source] + 1);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        } else {
            long value;
            if ((value = PrimitiveReader.readLongUnsigned(reader)) == 0) {
                rLongDictionary[idx] = 0;
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng = (rLongDictionary[idx] = value) - 1;
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        }

    }

    protected void genReadLongUnsignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
        long xl1;
        long tmpLng=(0 == (xl1 = PrimitiveReader.readLongUnsignedCopy(idx, source, rLongDictionary, reader)) ? constAbsent : xl1 - 1);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }
    }

    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constConst >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constConst & 0xFFFFFFFF);
        }
    }

    protected void genReadLongUnsignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rLongDictionary[idx] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng = rLongDictionary[idx] = (rLongDictionary[source] + (value > 0 ? value - 1 : value));
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        }
    }

    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongUnsigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng = value-1;
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        }
    }

    protected void genReadLongSignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault & 0xFFFFFFFF);
        } else {
            long tmpLng= PrimitiveReader.readLongSigned(reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }        
    }

    protected void genReadLongSignedIncrement(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long tmpLng=PrimitiveReader.readLongSignedIncrement(idx, source, rLongDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }
    }

    protected void genReadLongSignedCopy(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long tmpLng=PrimitiveReader.readLongSignedCopy(idx, source, rLongDictionary, reader);
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
        }
    }

    protected void genReadLongSignedConstant(long constDefault, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDelta(int idx, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=(rLongDictionary[idx] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }
  
    protected void genReadLongSignedNone(int idx, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        long tmpLng=rLongDictionary[idx] = PrimitiveReader.readLongSigned(reader);
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
        rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
    }

    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constDefault & 0xFFFFFFFF);
        } else {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng = (value > 0 ? value - 1 : value);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        }
    }

    protected void genReadLongSignedIncrementOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) { 

        if (PrimitiveReader.popPMapBit(reader) == 0) {
            if (0 == rLongDictionary[idx]) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng = (rLongDictionary[idx] = rLongDictionary[source] + 1);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        } else {
            long value;
            if ((value = PrimitiveReader.readLongSigned(reader)) == 0) {
                rLongDictionary[idx] = 0;
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng = (rLongDictionary[idx] = value) - 1;
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        }

    }

    protected void genReadLongSignedCopyOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long xl1 = PrimitiveReader.readLongSignedCopy(idx, source, rLongDictionary, reader);
            if (0 == xl1) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);   
            } else {
                long tmpLng=  xl1>0? xl1 - 1:xl1;
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }
        }
    }

    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (0==PrimitiveReader.popPMapBit(reader)) {
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
        } else {
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constConst >>> 32); 
            rbB[rbMask & rbRingBuffer.addPos++] = (int) (constConst & 0xFFFFFFFF);
        }
    }

    protected void genReadLongSignedDeltaOptional(int idx, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rLongDictionary[idx] = 0;// set to absent
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                StaticGlue.readLongSignedDeltaOptional(idx, source, rLongDictionary, rbB, rbMask, rbRingBuffer, value);
            }
        }
    }

    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (constAbsent & 0xFFFFFFFF);
            } else {
                long tmpLng= (value > 0 ? value - 1 : value);
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng >>> 32); 
                rbB[rbMask & rbRingBuffer.addPos++] = (int) (tmpLng & 0xFFFFFFFF);
            }        
        }
    }

    // text methods.

    
    protected void genReadUTF8None(int idx, int optOff, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8TailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        if (trim == 0) {
            textHeap.setNull(idx);
        } else {
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            int t = trim - 1;
            StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, t);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8DeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
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
        StaticGlue.allocateAndDeltaUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerSigned(reader));
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIITail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        StaticGlue.readASCIITail(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader));
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask, FASTRingBuffer rbRingBuffer) {
        rbB[rbMask & rbRingBuffer.addPos++] = constIdx;
        rbB[rbMask & rbRingBuffer.addPos++] = constLen;
    }

    protected void genReadASCIIDelta(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int trim = PrimitiveReader.readIntegerSigned(reader);
        if (trim >=0) {
            StaticGlue.readASCIITail(idx, textHeap, reader, trim); 
        } else {
            StaticGlue.readASCIIHead(idx, trim, textHeap, reader);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadASCIICopy(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
            int len = (PrimitiveReader.popPMapBit(reader)!=0) ? StaticGlue.readASCIIToHeap(idx, reader, textHeap) : textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIICopyOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            int len = (PrimitiveReader.popPMapBit(reader) != 0) ? StaticGlue.readASCIIToHeap(idx, reader, textHeap) : textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadUTF8Tail(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
            int trim = PrimitiveReader.readIntegerSigned(reader);
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            
            StaticGlue.allocateAndAppendUTF8(idx, textHeap, reader, utfLength, trim);
            int len = textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
    }


    protected void genReadUTF8Copy(int idx, int optOff, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
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
            StaticGlue.allocateAndCopyUTF8(idx, textHeap, reader, PrimitiveReader.readIntegerUnsigned(reader) - optOff);
            int len = textHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        }
    }

    protected void genReadASCIINone(int idx, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
        byte val;
        int tmp;
        if (0 != (tmp = 0x7F & (val = PrimitiveReader.readTextASCIIByte(reader)))) {
            tmp=StaticGlue.readASCIIToHeapValue(idx, val, tmp, textHeap, reader);
        } else {
            tmp=StaticGlue.readASCIIToHeapNone(idx, val, textHeap, reader);
        }
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, tmp, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = tmp;
    }

    protected void genReadASCIITailOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        int tail = PrimitiveReader.readIntegerUnsigned(reader);
        if (0 == tail) {
            textHeap.setNull(idx);
        } else {
           StaticGlue.readASCIITail(idx, textHeap, reader, tail-1);
        }
        int len = textHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    protected void genReadASCIIDeltaOptional(int idx, int[] rbB, int rbMask, TextHeap textHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        //TODO: B, extract the constant length from here.
        int optionalTrim = PrimitiveReader.readIntegerSigned(reader);
        int tempId = (0 == optionalTrim ? 
                         textHeap.initStartOffset( TextHeap.INIT_VALUE_MASK | idx) |TextHeap.INIT_VALUE_MASK : 
                         (optionalTrim > 0 ? StaticGlue.readASCIITail(idx, textHeap, reader, optionalTrim - 1) :
                                             StaticGlue.readASCIIHead(idx, optionalTrim, textHeap, reader)));
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

//                rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.addCharPos;
//                int lenTemp = PrimitiveReader.readTextASCIIIntoRing(rbRingBuffer.charBuffer, rbRingBuffer.addCharPos, rbRingBuffer.charMask, reader);
//                rbRingBuffer.addCharPos+=lenTemp;                
//                rbB[rbMask & rbRingBuffer.addPos++] = lenTemp;
    
    //TODO: perf problem. 6% in profiler, compiler should ONLY write back to heap IFF this field is read by another field.
    //this block is no longer in use however the  performance did not show up. so....

    protected void genReadASCIIDefault(int idx, int defIdx, int defLen, int[] rbB, int rbMask, PrimitiveReader reader, TextHeap textHeap, FASTRingBuffer rbRingBuffer) {
            if (0 == PrimitiveReader.popPMapBit(reader)) {
                StaticGlue.setInt(rbB,rbMask,rbRingBuffer,defIdx);
                StaticGlue.setInt(rbB,rbMask,rbRingBuffer,defLen);
            } else {
                
                //is not clear why but this block is faster than the direct copy from stream
                int len = StaticGlue.readASCIIToHeap(idx, reader, textHeap);
                StaticGlue.setInt(rbB,rbMask,rbRingBuffer,rbRingBuffer.writeTextToRingBuffer(idx, len, textHeap));
                StaticGlue.setInt(rbB,rbMask,rbRingBuffer,len);
            }
    }
        
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

    protected void genReadBytesDefault(int idx, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        
        if (0 == PrimitiveReader.popPMapBit(reader)) {
            rbB[rbMask & rbRingBuffer.addPos++] = defIdx;
            rbB[rbMask & rbRingBuffer.addPos++] = defLen;
        } else {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
            int len = byteHeap.valueLength(idx);
            rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
            rbB[rbMask & rbRingBuffer.addPos++] = len;
        }
    }

    protected void genReadBytesCopy(int idx, int optOff, int[] rbB, int rbMask, ByteHeap byteHeap, PrimitiveReader reader, FASTRingBuffer rbRingBuffer) {
        if (PrimitiveReader.popPMapBit(reader) != 0) {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
            PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        }
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesDeltaOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
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
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesTailOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
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
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;

    }

    protected void genReadBytesDelta(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
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
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }
    
    
    protected void genReadBytesTail(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int trim = PrimitiveReader.readIntegerUnsigned(reader);
        int length = PrimitiveReader.readIntegerUnsigned(reader);
        
        // append to tail
        int targetOffset = byteHeap.makeSpaceForAppend(idx, trim, length);
        PrimitiveReader.readByteData(byteHeap.rawAccess(), targetOffset, length, reader);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    
    protected void genReadBytesNoneOptional(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int length = PrimitiveReader.readIntegerUnsigned(reader) - 1;
        PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
    }

    protected void genReadBytesNone(int idx, int[] rbB, int rbMask, ByteHeap byteHeap, FASTRingBuffer rbRingBuffer, PrimitiveReader reader) {
        int length = PrimitiveReader.readIntegerUnsigned(reader) - 0;
        PrimitiveReader.readByteData(byteHeap.rawAccess(), byteHeap.allocate(idx, length), length, reader);
        int len = byteHeap.valueLength(idx);
        rbB[rbMask & rbRingBuffer.addPos++] = rbRingBuffer.writeBytesToRingBuffer(idx, len, byteHeap);
        rbB[rbMask & rbRingBuffer.addPos++] = len;
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
