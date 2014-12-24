package com.ociweb.jfast.generator;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingWalker;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.util.Histogram;
import com.ociweb.pronghorn.ring.util.hash.LongHashTable;
import com.ociweb.jfast.stream.FASTDecoder;

//TODO: B, needs support for messageRef where we can inject template in another and return to the previouslocation. Needs STACK in dispatch!
//TODO: B, set the default template for the case when it is undefined in catalog.
//TODO: C, Must add unit test for message length field start-of-frame testing, FrameLength bytes to read before decoding, is before pmap/templateId
//TODO: D, perhaps frame support is related to buffer size in primtive write so the right number of bits can be set.

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
        super(catalog, 
        	  RingBuffers.buildNoFanRingBuffers(new RingBuffer((byte)catalog.clientConfig().getPrimaryRingBits(),(byte)catalog.clientConfig().getTextRingBits(),catalog.ringByteConstants(), catalog.getFROM())));
    }
    
    public FASTReaderDispatchTemplates(TemplateCatalogConfig catalog, RingBuffers ringBuffers) {
        super(catalog, ringBuffers);
    }
    
    public FASTReaderDispatchTemplates(byte[] catBytes, RingBuffers ringBuffers) {
        this(new TemplateCatalogConfig(catBytes), ringBuffers);
    }
    

    protected void genReadTemplateId(int preambleDataLength, int maxTemplatePMapSize, PrimitiveReader reader, FASTDecoder dispatch) {

        {
        	int startPos = reader.position;
        	

	            // write template id at the beginning of this message
	            PrimitiveReader.openPMap(maxTemplatePMapSize, reader);
            
	           
	            //NOTE: we are assuming the first bit is the one for the templateId identifier (from the spec)
	            long templateId = (0 !=  PrimitiveReader.readPMapBit(reader)) ? PrimitiveReader.readLongUnsigned(reader) : -42;
	            
	            		
		        //because this point does extra checks it often caputres errors long after they have happened. TODO: Need better debugging tools     
	            if (templateId<0) {
	            	           	            	
	            	System.err.println(dispatch.msgIdx+" "+Integer.toBinaryString(dispatch.msgIdx)+" start openPMap at pos "+startPos+"  error in feed at "+PrimitiveReader.totalRead(reader)); //expected to be 1 less
	            	PrimitiveReader.printDebugData(reader);
	            	throw new FASTException();
	            }
	            
            //given the template id from the compressed FAST feed what is the script position?
            dispatch.msgIdx = LongHashTable.getItem(dispatch.templateStartIdx,templateId); 
            
            // fragment size plus 1 for template id and preamble data length in bytes
           	dispatch.activeScriptCursor = dispatch.msgIdx; 
            
            //we know the templateId so we now know which ring buffer to use.
            RingBuffer rb = RingBuffers.get(dispatch.ringBuffers,dispatch.activeScriptCursor);          
            
            //confirm that this ring buffer has enough room to hold the new results, and wait if it does not
            rb.consumerData.tailCache = RingBuffer.spinBlockOnTail(rb.consumerData.tailCache, 1 + preambleDataLength + rb.workingHeadPos.value - rb.maxSize, rb);
            
           // rb.consumerData.tailCache = RingBuffer.spinBlock(rb.tailPos, rb.consumerData.tailCache, 1 + preambleDataLength + rb.workingHeadPos.value - rb.maxSize);
            //TODO: B, should only spin loock above once but afte this method call it is done again, also sping lock causes cpu to sleep, how to avoid.
  
        }
    }
    
    protected void genWriteTemplateId(FASTDecoder dispatch) {
        {
        RingBuffer rb = RingBuffers.get(dispatch.ringBuffers,dispatch.activeScriptCursor);  
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, dispatch.msgIdx);
        }
    }

    protected void genWritePreambleB(FASTDecoder dispatch) {
        {
        RingBuffer rb = RingBuffers.get(dispatch.ringBuffers,dispatch.activeScriptCursor);  
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, dispatch.preambleB);
        }
    }

    protected void genWritePreambleA(FASTDecoder dispatch) {
        {
        RingBuffer rb = RingBuffers.get(dispatch.ringBuffers,dispatch.activeScriptCursor);
        RingBuffer.addValue(rb.buffer, rb.mask, rb.workingHeadPos, dispatch.preambleA);
        }
    }

    protected void genReadPreambleB(PrimitiveReader reader, FASTDecoder dispatch) {
        dispatch.preambleB = PrimitiveReader.readRawInt(reader);
    }

    protected void genReadPreambleA(PrimitiveReader reader, FASTDecoder dispatch) {
        dispatch.preambleA = PrimitiveReader.readRawInt(reader);
    }
    
    protected void genReadCopyBytes(int source, int target, LocalHeap byteHeap) {
        LocalHeap.copy(source,target,byteHeap);
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
    
    protected void genReadGroupCloseMessage(PrimitiveReader reader, FASTDecoder dispatch) {
        if (dispatch.sequenceCountStackHead<0) { 
            dispatch.activeScriptCursor = -1;
            PrimitiveReader.closePMap(reader);
        }
    }
    
    //length methods

    protected int genReadLengthDefault(int constDefault,  int jumpToTarget, int jumpToNext, int[] rbB, PrimitiveReader reader, int rbMask, PaddedLong rbPos, FASTDecoder dispatch) {
        {
            int length;
            RingBuffer.addValue(rbB,rbMask,rbPos, length = (0 == PrimitiveReader.readPMapBit(reader) ? constDefault : PrimitiveReader.readIntegerUnsigned(reader)));
            if (length == 0) {
                // jumping over sequence (forward) it was skipped (rare case)
                return jumpToTarget;
              
            } else {
                dispatch.sequenceCountStack[++dispatch.sequenceCountStackHead] = length;
                return jumpToNext;
               
           }
        }
    }

    
    
    protected void genReadLengthIncrement(int target, int source,  int jumpToTarget, int jumpToNext, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos, PrimitiveReader reader, FASTDecoder dispatch) {
        {
            int length;
            int value = length = (0 == PrimitiveReader.readPMapBit(reader) ? (rIntDictionary[target] = rIntDictionary[source] + 1)
            : (rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader)));
            RingBuffer.addValue(rbB, rbMask, rbPos, value);
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
            int value = length = rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] : PrimitiveReader.readIntegerUnsigned(reader));
            RingBuffer.addValue(rbB, rbMask, rbPos, value);
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
        RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
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
            RingBuffer.addValue(rbB, rbMask, rbPos, length);
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

            RingBuffer.addValue(rbB,rbMask,rbPos, length = PrimitiveReader.readIntegerUnsigned(reader));
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

        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
        } else {
           int value = PrimitiveReader.readIntegerUnsigned(reader)-1;
           int mask = value>>31;
           RingBuffer.addValue(rbB, rbMask, rbPos, (constAbsent&mask) | ((~mask)&value) );           
        }

    }


    protected void genReadIntegerUnsignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            if (rIntDictionary[target] == 0) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
            }
        } else {
            int value;
            if ((value = PrimitiveReader.readIntegerUnsigned(reader)) == 0) {
                rIntDictionary[target] = 0;
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
            }
        }
    }
    
    protected void genReadIntegerUnsignedIncrementOptionalTS(int targsrc, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            if (rIntDictionary[targsrc] == 0) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, ++rIntDictionary[targsrc]);
            }
        } else {
            int value;
            if ((value = PrimitiveReader.readIntegerUnsigned(reader)) == 0) {
                rIntDictionary[targsrc] = 0;
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[targsrc] = value) - 1);
            }
        }
    }

    protected void genReadIntegerUnsignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = (rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] : PrimitiveReader.readIntegerUnsigned(reader)))-1;
            int mask = value>>31;
            RingBuffer.addValue(rbB, rbMask, rbPos, (constAbsent&mask) | ((~mask)&value) );
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
        RingBuffer.addValue(rbB,rbMask,rbPos, (0 == PrimitiveReader.readPMapBit(reader) ? constAbsent : constConst));
    }

    protected void genReadIntegerUnsignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            
            if (0 == value) {
                rIntDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);

            } else {
                //
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + ((value-1)+((value-1)>>63))));    
            }
        }
    }

    protected void genReadIntegerUnsignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerUnsigned(reader)-1;
            int mask = value>>31;
            RingBuffer.addValue(rbB, rbMask, rbPos, (constAbsent&mask) | ((~mask)&value) );  
                                    
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
        RingBuffer.addValue(rbB,rbMask,rbPos, (0 == PrimitiveReader.readPMapBit(reader) ? constDefault : PrimitiveReader.readIntegerUnsigned(reader)));
    }

    protected void genReadIntegerUnsignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, 
                (0 == PrimitiveReader.readPMapBit(reader) ? (rIntDictionary[target] = rIntDictionary[source] + 1)
                                                          : (rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader))));
    }
    
    protected void genReadIntegerUnsignedIncrementTS(int targsrc, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, 
                (0 == PrimitiveReader.readPMapBit(reader) ? (++rIntDictionary[targsrc])
                                                          : (rIntDictionary[targsrc] = PrimitiveReader.readIntegerUnsigned(reader))));
    }

    protected void genReadIntegerUnsignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] : PrimitiveReader.readIntegerUnsigned(reader)));
    }
    
    protected void genReadIntegerUnsignedCopyTS(int target, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, 0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[target] : (rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader)) );
    }

    protected void genReadIntegerUnsignedConstant(int constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, constDefault);
    }

    protected void genReadIntegerUnsignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader))));
    }

    protected void genReadIntegerUnsigned(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = PrimitiveReader.readIntegerUnsigned(reader));
    }

    protected void genReadIntegerSignedDefault(int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, (0 == PrimitiveReader.readPMapBit(reader) ? constDefault : PrimitiveReader.readIntegerSigned(reader)));
    }

    protected void genReadIntegerSignedIncrement(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, (0 == PrimitiveReader.readPMapBit(reader) ? (rIntDictionary[target] = rIntDictionary[source] + 1)
        : (rIntDictionary[target] = PrimitiveReader.readIntegerSigned(reader))));
    }

    protected void genReadIntegerSignedCopy(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader)));
    }

    protected void genReadIntegerConstant(int constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, constDefault);

    }

    protected void genReadIntegerSignedDelta(int target, int source, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = (int) (rIntDictionary[source] + PrimitiveReader.readLongSigned(reader))));
    }

    protected void genReadIntegerSignedNone(int target, int[] rbB, int rbMask, PrimitiveReader reader, int[] rIntDictionary, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = PrimitiveReader.readIntegerSigned(reader));
    }

    protected void genReadIntegerSignedDefaultOptional(int constAbsent, int constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
        } else {
            int value = PrimitiveReader.readIntegerSigned(reader);
            RingBuffer.addValue(rbB,rbMask,rbPos,  value == 0 ? constAbsent : (-1 + (value + (value >>> 31))) );
        }
    }


    protected void genReadIntegerSignedIncrementOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] == 0 ? constAbsent : (rIntDictionary[target] = rIntDictionary[source] + 1)));
        } else {
            int value;
            if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                rIntDictionary[target] = 0;
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
            }
        }
    }
    

    protected void genReadIntegerSignedCopyOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int xi1 = rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader));
            if (0==xi1) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (xi1 + (xi1 >>> 31))) );
            }
        }
    }

    protected void genReadIntegerSignedConstantOptional(int constAbsent, int constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        RingBuffer.addValue(rbB,rbMask,rbPos, (0 == PrimitiveReader.readPMapBit(reader) ? constAbsent : constConst));
    }
    
    protected void genReadIntegerSignedDeltaOptional(int target, int source, int constAbsent, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rIntDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] +  (-1 + (value + (value >>> 63))) ));  
            }
        }
    }

    
    protected void genReadIntegerSignedOptional(int constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0 == value) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos,   (-1 + (value + (value >>> 31))) ); 
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
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))) );
                }
            }            
            //Long signed default
            if (0==PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
            } else {
                long tmpLng = PrimitiveReader.readLongSigned(reader);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }            
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {   
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                if (0==rIntDictionary[target]) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            //Long signed default
            if (0==PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
            } else {
                long tmpLng = PrimitiveReader.readLongSigned(reader);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
         
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int xi1 = rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader));
            if (0==xi1) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (xi1 + (xi1 >>> 31))) );
                //Long signed default
                if (0==PrimitiveReader.readPMapBit(reader)) {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
                }
            }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaDefault(int constAbsent, int constConst, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbPos.value+=2;
        } else {
            RingBuffer.addValue(rbB, rbMask, rbPos, constConst);
            //Long signed default
           if (0==PrimitiveReader.readPMapBit(reader)) {
               RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
           } else {
               long tmpLng = PrimitiveReader.readLongSigned(reader);
               RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
           }
        }
        
    }
    protected void genReadDecimalDeltaOptionalMantissaDefault(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (-1 + (value + (value >>> 31)))));
                //Long signed default
                if (0==PrimitiveReader.readPMapBit(reader)) {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
                }
            }
        }
    }
    
    protected void genReadDecimalOptionalMantissaDefault(int constAbsent, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));                    
              //Long signed default
                if (0==PrimitiveReader.readPMapBit(reader)) {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
                } else {
                    long tmpLng = PrimitiveReader.readLongSigned(reader);
                    RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
                }
            }
            
        }
    }

    
    
    //increment
    
    protected void genReadDecimalDefaultOptionalMantissaIncrement(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));
                }
            }
            //Long signed increment
            long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                if (0==rIntDictionary[target]) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            //Long signed increment
            long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF)); 
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int xi1 = rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader));
            if (0==xi1) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (xi1 + (xi1 >>> 31))));
                //Long signed increment
                long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaIncrement(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB, rbMask, rbPos, constConst);
                //Long signed increment
               long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
               RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }

    }
    protected void genReadDecimalDeltaOptionalMantissaIncrement(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (-1 + (value + (value >>> 31)))));
                //Long signed increment
                long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaIncrement(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));                    
                //Long signed increment
               long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[mantissaTarget] = rLongDictionary[mantissaSource] + 1) : (rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader)));
               RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
     
        }
    }
    
    //copy
    
    protected void genReadDecimalDefaultOptionalMantissaCopy(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {            
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));
                }                
            }
            //Long signed copy
            long tmpLng=rLongDictionary[mantissaTarget] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[mantissaSource] : PrimitiveReader.readLongSigned(reader));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));

        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                
                if (0==rIntDictionary[target]) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            
            //Long signed copy
            long tmpLng=rLongDictionary[mantissaTarget] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[mantissaSource] : PrimitiveReader.readLongSigned(reader));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }    
            
         
    }
    
    protected void genReadDecimalCopyOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int xi1 = rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader));
            if (0==xi1) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (xi1 + (xi1 >>> 31))));
                //Long signed copy
                long tmpLng=rLongDictionary[mantissaTarget] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[mantissaSource] : PrimitiveReader.readLongSigned(reader));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
              }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaCopy(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB, rbMask, rbPos, constConst);
                //Long signed copy
               long tmpLng=rLongDictionary[mantissaTarget] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[mantissaSource] : PrimitiveReader.readLongSigned(reader));
               RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }             

    }
    protected void genReadDecimalDeltaOptionalMantissaCopy(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (-1 + (value + (value >>> 31)))));
                //Long signed copy
                long tmpLng=rLongDictionary[mantissaTarget] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[mantissaSource] : PrimitiveReader.readLongSigned(reader));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaCopy(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));                    
                //Long signed copy
               long tmpLng=rLongDictionary[mantissaTarget] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[mantissaSource] : PrimitiveReader.readLongSigned(reader));
               RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
                    
        }
    }
    
    //constant
    
    protected void genReadDecimalDefaultOptionalMantissaConstant(int constAbsent, int constDefault, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));
                }                
            }
            //Long signed constant
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));

       }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                
                if (0==rIntDictionary[target]) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            
            //Long signed constant
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));

        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            int xi1 = rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader));
            if (0==xi1) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (xi1 + (xi1 >>> 31))));
                //Long signed constant
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
           }
 
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaConstant(int constAbsent, int constConst, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
               RingBuffer.addValue(rbB, rbMask, rbPos, constConst);
                //Long signed constant
                 RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));

               
            } 
            
    }
      
    protected void genReadDecimalDeltaOptionalMantissaConstant(int target, int source, int constAbsent, long mantissaConstDefault, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (-1 + (value + (value >>> 31)))));
                //Long signed constant
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));
           }
            
        }
    }

    protected void genReadDecimalOptionalMantissaConstant(int constAbsent, long mantissaConstDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));                    
                //Long signed constant
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (mantissaConstDefault >>> 32), (int) (mantissaConstDefault & 0xFFFFFFFF));

            }
                        
        }
    }
    
    //delta

    protected void genReadDecimalDefaultOptionalMantissaDelta(int constAbsent, int constDefault, int mantissaTarget, int mantissaSource, int[] rbB, long[] rLongDictionary, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos,  (-1 + (value + (value >>> 31))) );
                }                
            }
            //Long signed delta
            long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }
    
    //branching example for the code above
    //FASTRingBuffer.addValue(rbB,rbMask,rbPos, (value > 0 ? value - 1 : value));
    
    protected void genReadDecimalIncrementOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {                
                if (0==rIntDictionary[target]) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = rIntDictionary[source] + 1));
                }    
                
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[target] = 0;
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[target] = value) - 1);
                }
            }
            
            //Long signed delta
            long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            int xi1 = rIntDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader));
            if (0==xi1) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (xi1 + (xi1 >>> 31)))); 
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }           
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaDelta(int constAbsent, int constConst, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbPos.value+=2;
        } else {
            RingBuffer.addValue(rbB, rbMask, rbPos, constConst);
            //Long signed delta
           long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
           RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }         
    }
    
    protected void genReadDecimalDeltaOptionalMantissaDelta(int target, int source, int constAbsent, int mantissaTarget, int mantissaSource, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[target] = (int) (rIntDictionary[source] + (-1 + (value + (value >>> 31)))));
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaDelta(int constAbsent, int mantissaTarget, int mantissaSource, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));                    
                //Long signed delta
                long tmpLng=(rLongDictionary[mantissaTarget] = (rLongDictionary[mantissaSource] + PrimitiveReader.readLongSigned(reader)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    //none

    protected void genReadDecimalDefaultOptionalMantissaNone(int constAbsent, int constDefault, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, constDefault);
            } else {
                int value = PrimitiveReader.readIntegerSigned(reader);
                if (0==value) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, constAbsent);
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));
                }                
            }
            //Long signed none
            long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }
    
    protected void genReadDecimalIncrementOptionalMantissaNone(int expoTarget, int expoSource, int expoConstAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                
                if (0==rIntDictionary[expoTarget]) {
                    RingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);  
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[expoTarget] = rIntDictionary[expoSource] + 1));
                }        
               
            } else {
                int value;
                if ((value = PrimitiveReader.readIntegerSigned(reader)) == 0) {
                    rIntDictionary[expoTarget] = 0;
                    RingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
                    //must still write long even when we skipped reading its pmap bit. but value is undefined.
                    rbPos.value+=2;
                    return;
                } else {
                    RingBuffer.addValue(rbB,rbMask,rbPos, (rIntDictionary[expoTarget] = value) - 1);
                }
            }
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
           
        }
    }
    
    protected void genReadDecimalCopyOptionalMantissaNone(int expoTarget, int expoSource, int expoConstAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
        	int tpos = reader.position;
        	
            boolean theBit = 0 == PrimitiveReader.readPMapBit(reader);
			int xi1 = rIntDictionary[expoTarget] = (theBit ? rIntDictionary[expoSource] :  PrimitiveReader.readIntegerSigned(reader));
            if (0==xi1) {
                RingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
            	int tmp = ((-1 + (xi1 + (xi1 >>> 31))));
            	if (tmp== -16) {
            		//+PrimitiveReader.totalRead(reader)
            		
            		PrimitiveReader.printDebugData(reader);
            		throw new FASTException();
            	}
            	
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (xi1 + (xi1 >>> 31))));
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
      
        }
    }
    
    protected void genReadDecimalConstantOptionalMantissaNone(int expoConstAbsent, int expoConstConst, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
            //must still write long even when we skipped reading its pmap bit. but value is undefined.
            rbPos.value+=2;
        } else {
            RingBuffer.addValue(rbB, rbMask, rbPos, expoConstConst);
            //Long signed none
           long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
           RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        } 

    }
    protected void genReadDecimalDeltaOptionalMantissaNone(int expoTarget, int expoSource, int expoConstAbsent, int mantissaTarget, int[] rIntDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                rIntDictionary[expoTarget] = 0;// set to absent
                RingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {
                RingBuffer.addValue(rbB,rbMask,rbPos, rIntDictionary[expoTarget] = (int) (rIntDictionary[expoSource] + (-1 + (value + (value >>> 31)))));
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
            
        }
    }
    
    protected void genReadDecimalOptionalMantissaNone(int expoConstAbsent, int mantissaTarget, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos, long[] rLongDictionary) {
        {
            
            int value = PrimitiveReader.readIntegerSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB, rbMask, rbPos, expoConstAbsent);                
                //must still write long even when we skipped reading its pmap bit. but value is undefined.
                rbPos.value+=2;
            } else {                
                RingBuffer.addValue(rbB,rbMask,rbPos, (-1 + (value + (value >>> 31))));                    
                //Long signed none
                long tmpLng=rLongDictionary[mantissaTarget] = PrimitiveReader.readLongSigned(reader);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }                      
    
        }
    }    
        
    /////////////////////////////////////////////////////////////////////////////
    /////end ////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////
    
    
    
    // long methods

    protected void genReadLongUnsignedDefault(long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? constDefault : PrimitiveReader.readLongUnsigned(reader));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[target] = rLongDictionary[source] + 1)
                                                                : (rLongDictionary[target] = PrimitiveReader.readLongUnsigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedCopy(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=rLongDictionary[target] =(0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[source] : PrimitiveReader.readLongUnsigned(reader));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongConstant(long constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32), (int) (constDefault & 0xFFFFFFFF));
    }

    protected void genReadLongUnsignedDelta(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=(rLongDictionary[target] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedNone(int target, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=rLongDictionary[target] = PrimitiveReader.readLongUnsigned(reader);
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long result;
            if (0 == PrimitiveReader.readPMapBit(reader)) {
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
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (0 == PrimitiveReader.readPMapBit(reader)) {
            if (0 == rLongDictionary[target]) {
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[target] = rLongDictionary[source] + 1);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
        } else {
            long value;
            if ((value = PrimitiveReader.readLongUnsigned(reader)) == 0) {
                rLongDictionary[target] = 0;
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[target] = value) - 1;
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
        }

    }

    protected void genReadLongUnsignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long xl1;
            long tmpLng=(0 == (xl1 = rLongDictionary[target] =(0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[source] : PrimitiveReader.readLongUnsigned(reader))) ? constAbsent : xl1 - 1);
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
        } else {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constConst >>> 32), (int) (constConst & 0xFFFFFFFF));
        }
    }

    protected void genReadLongUnsignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rLongDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = rLongDictionary[target] = (rLongDictionary[source] + (-1 + (value + (value >>> 63))));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
        }
    }

    protected void genReadLongUnsignedOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongUnsigned(reader)-1;
            long mask = value>>63;
            long result = (constAbsent&mask) | ((~mask)&value);
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (result >>> 32), (int) (result & 0xFFFFFFFF));
            
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
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32), (int) (constDefault & 0xFFFFFFFF));
        } else {
            long tmpLng= PrimitiveReader.readLongSigned(reader);
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }        
    }

    protected void genReadLongSignedIncrement(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=(0 == PrimitiveReader.readPMapBit(reader) ? (rLongDictionary[target] = rLongDictionary[source] + 1) : (rLongDictionary[target] = PrimitiveReader.readLongSigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongSignedCopy(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=rLongDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[source] : PrimitiveReader.readLongSigned(reader));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongSignedConstant(long constDefault, int[] rbB, int rbMask, PaddedLong rbPos) {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32), (int) (constDefault & 0xFFFFFFFF));
    }

    protected void genReadLongSignedDelta(int target, int source, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=(rLongDictionary[target] = (rLongDictionary[source] + PrimitiveReader.readLongSigned(reader)));
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }
    
    protected void genReadLongSignedDeltaTS(int targsrc, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=(rLongDictionary[targsrc]+=PrimitiveReader.readLongSigned(reader));        
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }
  
    protected void genReadLongSignedNone(int target, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long tmpLng=rLongDictionary[target] = PrimitiveReader.readLongSigned(reader);
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
        }
    }

    protected void genReadLongSignedDefaultOptional(long constAbsent, long constDefault, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {

        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constDefault >>> 32), (int) (constDefault & 0xFFFFFFFF));
        } else {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (-1 + (value + (value >>> 31)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
        }
    }

    protected void genReadLongSignedIncrementOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) { 

        if (0 == PrimitiveReader.readPMapBit(reader)) {
            if (0 == rLongDictionary[target]) {
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[target] = rLongDictionary[source] + 1);
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
        } else {
            long value;
            if ((value = PrimitiveReader.readLongSigned(reader)) == 0) {
                rLongDictionary[target] = 0;
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng = (rLongDictionary[target] = value) - 1;
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
        }

    }

    protected void genReadLongSignedCopyOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long xl1 = rLongDictionary[target] = (0 == PrimitiveReader.readPMapBit(reader) ? rLongDictionary[source] : PrimitiveReader.readLongSigned(reader));
            if (0 == xl1) {
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));   
            } else {
                long tmpLng=  (-1 + (xl1 + (xl1 >>> 63)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }
        }
    }

    protected void genReadLongSignedConstantOptional(long constAbsent, long constConst, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0==PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
        } else {
            RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constConst >>> 32), (int) (constConst & 0xFFFFFFFF));
        }
    }

    protected void genReadLongSignedDeltaOptional(int target, int source, long constAbsent, long[] rLongDictionary, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0 == value) {
                rLongDictionary[target] = 0;// set to absent
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                StaticGlue.readLongSignedDeltaOptional(target, source, rLongDictionary, rbB, rbMask, rbPos, value);
            }
        }
    }

    protected void genReadLongSignedNoneOptional(long constAbsent, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        {
            long value = PrimitiveReader.readLongSigned(reader);
            if (0==value) {
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (constAbsent >>> 32), (int) (constAbsent & 0xFFFFFFFF));
            } else {
                long tmpLng= (-1 + (value + (value >>> 63)));
                RingBuffer.addValue(rbB,rbMask,rbPos, (int) (tmpLng >>> 32), (int) (tmpLng & 0xFFFFFFFF));
            }        
        }
    }

    // text methods.
   
    protected void genReadASCIITail(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        {
            StaticGlue.readASCIITail(target, byteHeap, reader, PrimitiveReader.readIntegerUnsigned(reader));
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target,len,rbMask,rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    protected void genReadTextConstant(int constIdx, int constLen, int[] rbB, int rbMask, PaddedLong rbPos) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constIdx, constLen);
    }

    protected void genReadASCIIDelta(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
    	{
	    	int trim = PrimitiveReader.readIntegerSigned(reader);
	        if (trim >=0) {
	            StaticGlue.readASCIITail(target, byteHeap, reader, trim); 
	        } else {
	            StaticGlue.readASCIIHead(target, trim, byteHeap, reader);
	        }
	        int len = LocalHeap.valueLength(target,byteHeap);
	        LocalHeap.addLocalHeapValue(target,len,rbMask,rbB, rbPos, byteHeap, rbRingBuffer);
    	}
    }

    protected void genReadASCIICopy(int target, int rbMask, int[] rbB, PrimitiveReader reader, LocalHeap byteHeap, PaddedLong rbPos, RingBuffer rbRingBuffer) {
            LocalHeap.addLocalHeapValue(target,((0 == PrimitiveReader.readPMapBit(reader)) ? LocalHeap.valueLength(target,byteHeap) : StaticGlue.readASCIIToHeap(target, reader, byteHeap)),rbMask,rbB, rbPos, byteHeap, rbRingBuffer);
    }
    
    protected void genReadASCIICopyOptional(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
            LocalHeap.addLocalHeapValue(target,((0 == PrimitiveReader.readPMapBit(reader)) ? LocalHeap.valueLength(target,byteHeap) : StaticGlue.readASCIIToHeap(target, reader, byteHeap)),rbMask,rbB, rbPos, byteHeap, rbRingBuffer);
    }
    
    protected void genReadASCIINone(int target, int[] rbB, int rbMask, PrimitiveReader reader, LocalHeap byteHeap, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        {
            byte val;
            int tmp;
            if (0 != (tmp = 0x7F & (val = PrimitiveReader.readTextASCIIByte(reader)))) {
                tmp=StaticGlue.readASCIIToHeapValue(target, val, tmp, byteHeap, reader);
            } else {
                tmp=StaticGlue.readASCIIToHeapNone(target, val, byteHeap, reader);
            }
            
            LocalHeap.addLocalHeapValue(target,tmp,rbMask,rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    protected void genReadASCIITailOptional(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        {
            int tail = PrimitiveReader.readIntegerUnsigned(reader);
            if (0 == tail) {
                LocalHeap.setNull(target, byteHeap);
            } else {
               StaticGlue.readASCIITail(target, byteHeap, reader, tail-1);
            }
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target,len,rbMask,rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }
    
    protected void genReadASCIIDeltaOptional(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        {
            int optionalTrim = PrimitiveReader.readIntegerSigned(reader);
            int tempId = (0 == optionalTrim ? 
                             LocalHeap.initStartOffset(LocalHeap.INIT_VALUE_MASK | target, byteHeap) |LocalHeap.INIT_VALUE_MASK : 
                             (optionalTrim > 0 ? StaticGlue.readASCIITail(target, byteHeap, reader, optionalTrim - 1) :
                                                 StaticGlue.readASCIIHead(target, optionalTrim, byteHeap, reader)));
            int len = LocalHeap.valueLength(tempId,byteHeap);
            LocalHeap.addLocalHeapValue(tempId,len,rbMask,rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    protected void genReadTextConstantOptional(int constInit, int constValue, int constInitLen, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        if (0 != PrimitiveReader.readPMapBit(reader) ) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constInit, constInitLen);
        } else {
            RingBuffer.addValue(rbB, rbMask, rbPos, constValue, constValueLen);
        }
    }

    
    protected void genReadASCIIDefault(int target, int defIdx, int defLen, int rbMask, int[] rbB, PrimitiveReader reader, LocalHeap byteHeap, PaddedLong rbPos, byte[] byteBuffer, int byteMask, RingBuffer rbRingBuffer) {
            if (0 == PrimitiveReader.readPMapBit(reader)) {
                RingBuffer.addValue(rbB, rbMask, rbPos, defIdx, defLen);
            } else {
                int bytePos = rbRingBuffer.byteWorkingHeadPos.value;
                int lenTemp = PrimitiveReader.readTextASCIIIntoRing(byteBuffer,
                                                                    bytePos, 
                                                                    byteMask,
                                                                    reader);
                RingBuffer.addValue(rbB,rbMask,rbPos, bytePos, lenTemp);
                rbRingBuffer.byteWorkingHeadPos.value = bytePos+lenTemp;                
            }
    }    
//                //TODO: B: old code we only want if this default field is read from another, eg dictionary sharing.
//                int len = StaticGlue.readASCIIToHeap(target, reader, byteHeap);
//                FASTRingBuffer.addValue(rbB,rbMask,rbPos, FASTRingBuffer.writeTextToRingBuffer(target, len, byteHeap, rbRingBuffer));
//                FASTRingBuffer.addValue(rbB, rbMask, rbPos, len);
     
    
    
    protected void genReadBytesConstant(int constIdx, int constLen, int[] rbB, int rbMask, PaddedLong rbPos) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constIdx, constLen);
    }

    protected void genReadBytesConstantOptional(int constInit, int constInitLen, int constValue, int constValueLen, int[] rbB, int rbMask, PrimitiveReader reader, PaddedLong rbPos) {
        
        if (0 == PrimitiveReader.readPMapBit(reader) ) {
            RingBuffer.addValue(rbB, rbMask, rbPos, constValue, constValueLen);
        } else {
            RingBuffer.addValue(rbB, rbMask, rbPos, constInit, constInitLen);
        }
    }

    protected void genReadBytesDefault(int target, int defIdx, int defLen, int optOff, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        
        if (0 == PrimitiveReader.readPMapBit(reader)) {
            RingBuffer.addValue(rbB, rbMask, rbPos, defIdx, defLen);
        } else {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
            PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.allocate(target, length, byteHeap), length, reader);
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target, len, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    protected void genReadBytesCopy(int target, int optOff, int[] rbB, int rbMask, LocalHeap byteHeap, PrimitiveReader reader, PaddedLong rbPos, RingBuffer rbRingBuffer) {
        {
            if (PrimitiveReader.readPMapBit(reader) != 0) {
                int length = PrimitiveReader.readIntegerUnsigned(reader) - optOff;
                PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.allocate(target, length, byteHeap), length, reader);
            }
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target, len, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    protected void genReadBytesDeltaOptional(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        {
            int trim = PrimitiveReader.readIntegerSigned(reader);
            if (0 == trim) {
                LocalHeap.setNull(target, byteHeap);
            } else {
                if (trim > 0) {
                    trim--;// subtract for optional
                }        
                int utfLength = PrimitiveReader.readIntegerUnsigned(reader);        
                if (trim >= 0) {
                    PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.makeSpaceForAppend(target,trim,utfLength,byteHeap), utfLength, reader);
                } else {
                    PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.makeSpaceForPrepend(target,-trim,utfLength,byteHeap), utfLength, reader);
                }
            }
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target, len, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    protected void genReadBytesTailOptional(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        {
            int trim = PrimitiveReader.readIntegerUnsigned(reader);
            if (trim == 0) {
                LocalHeap.setNull(target, byteHeap);
            } else {
                trim--;
                int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
                // append to tail
                PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.makeSpaceForAppend(target,trim,utfLength,byteHeap), utfLength, reader);
            }
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target, len, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }

    }

    protected void genReadBytesDelta(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        {
            int trim = PrimitiveReader.readIntegerSigned(reader);
            int utfLength = PrimitiveReader.readIntegerUnsigned(reader);
            if (trim >= 0) {
                // append to tail
                PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.makeSpaceForAppend(target,trim,utfLength,byteHeap), utfLength, reader);
            } else {
                // append to head
                PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.makeSpaceForPrepend(target,-trim,utfLength,byteHeap), utfLength, reader);
            }
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target, len, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }
    
    
    protected void genReadBytesTail(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        {
            int trim = PrimitiveReader.readIntegerUnsigned(reader);
            int length = PrimitiveReader.readIntegerUnsigned(reader);
            
            // append to tail
            int targetOffset = LocalHeap.makeSpaceForAppend(target,trim,length,byteHeap);
            PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), targetOffset, length, reader);
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target, len, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    
    protected void genReadBytesNoneOptional(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - 1;
                
            if (length<0) {
                RingBuffer.addValue(rbB, rbMask, rbPos, rbRingBuffer.byteWorkingHeadPos.value, length);
                return;
            }
            if (length>0) {        
            	PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.allocate(target, length, byteHeap), length, reader);
            }
            LocalHeap.addLocalHeapValue(target, length, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    protected void genReadBytesNone(int target, int[] rbB, int rbMask, LocalHeap byteHeap, PaddedLong rbPos, PrimitiveReader reader, RingBuffer rbRingBuffer) {
        {
            int length = PrimitiveReader.readIntegerUnsigned(reader) - 0;
            PrimitiveReader.readByteData(LocalHeap.rawAccess(byteHeap), LocalHeap.allocate(target, length, byteHeap), length, reader);
            int len = LocalHeap.valueLength(target,byteHeap);
            LocalHeap.addLocalHeapValue(target, len, rbMask, rbB, rbPos, byteHeap, rbRingBuffer);
        }
    }

    // dictionary reset

    protected void genReadDictionaryBytesReset(int target, LocalHeap byteHeap) {
        LocalHeap.setNull(target, byteHeap);
    }

    protected void genReadDictionaryTextReset(int target, LocalHeap byteHeap) {
        LocalHeap.reset(target, byteHeap);
    }

    protected void genReadDictionaryLongReset(int target, long resetConst, long[] rLongDictionary) {
        rLongDictionary[target] = resetConst;
    }

    protected void genReadDictionaryIntegerReset(int target, int resetConst, int[] rIntDictionary) {
        rIntDictionary[target] = resetConst;
    }

}
