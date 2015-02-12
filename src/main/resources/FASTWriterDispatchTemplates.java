package com.ociweb.jfast.generator;

import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBuffer.PaddedLong;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.RingReader;


public abstract class FASTWriterDispatchTemplates extends FASTEncoder {

    public FASTWriterDispatchTemplates(final TemplateCatalogConfig catalog) {
        super(catalog);
    }    
    
    public FASTWriterDispatchTemplates(final TemplateCatalogConfig catalog, RingBuffers ringBuffers) {
        super(catalog);
    }    

    protected void genWriteCopyBytes(int source, int target, LocalHeap byteHeap) {
        LocalHeap.copy(source,target,byteHeap);
    }

    protected void genWritePreamble(int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, FASTEncoder dispatch) {
        {
            int i = 0;
            byte[] preambleData = dispatch.preambleData;
            int s = preambleData.length;
            int p = fieldPos;
            while (i < s) {
                            
                int d = RingBuffer.readInt(rbB, rbMask, rbPos.value +  p);;
                preambleData[i++] = (byte) (0xFF & (d >>> 0));
                preambleData[i++] = (byte) (0xFF & (d >>> 8));
                preambleData[i++] = (byte) (0xFF & (d >>> 16));
                preambleData[i++] = (byte) (0xFF & (d >>> 24));
                p++;
            }
        }
        PrimitiveWriter.writeByteArrayData(dispatch.preambleData, 0, dispatch.preambleData.length, writer);
    }
    //    System.err.println("write preamble to :"+(writer.limit+writer.totalWritten(writer)));
    

    //NOTE: assumes that the use of the constant array indicates the default value, it is the responsibility of the ring buffer writer to do this correctly
    protected void genWriteTextDefault(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        {
            int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
            if (0==(rawPos>>>31)) {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                int len = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
                PrimitiveWriter.writeTextASCII(RingBuffer.byteBackingArray(rawPos, rbRingBuffer), RingBuffer.bytePositionGen(rawPos, rbRingBuffer, len), len, rbRingBuffer.byteMask, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)0, writer); 
            }            
        }
    }
    
    //NOTE: assumes that the ring buffer will only use the constant array when it is holding the default value
    //The responsibility for using the right byte array is up to the writer of the ring buffer.
    protected void genWriteTextDefaultOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        {
            int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
            if (0==(rawPos>>>31)) {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
                if (length<0) {
                    PrimitiveWriter.writeNull(writer);
                } else {
                    PrimitiveWriter.writeTextASCII(RingBuffer.byteBackingArray(rawPos, rbRingBuffer), RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length), length, rbRingBuffer.byteMask, writer);
                }                
            } else {
                PrimitiveWriter.writePMapBit((byte)0, writer); 
            }            
        }
    }


    protected void genWriteTextCopy(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
        	int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
        	
            int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
            int offset = RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = RingBuffer.byteBackingArray(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;                           
      
            if (LocalHeap.equals(target,buffer,offset,length,byteMask,byteHeap)) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeTextASCII(/*value*/ buffer, offset, length, byteMask, writer);
                LocalHeap.set(target,buffer,offset,length,byteMask,byteHeap);
            }
        }
    }
    
    protected void genWriteTextCopyOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        { 
            int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos); 
            if (length<0) {
                if (LocalHeap.isNull(target,byteHeap)) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            } else {            	
                int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
                int offset = RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length);
                // constant from heap or dynamic from char ringBuffer
                byte[] buffer = RingBuffer.byteBackingArray(rawPos, rbRingBuffer);
                int byteMask = rbRingBuffer.byteMask;                               
                
                if (LocalHeap.equals(target,buffer,offset,length,byteMask,byteHeap)) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeTextASCII(/*value*/ buffer, offset, length, byteMask, writer);
                    LocalHeap.set(target,buffer,offset,length,byteMask,byteHeap);
                }
            }
        }
    }

    protected void genWriteTextDeltaOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
  
        //	System.err.println("value to encode for delta:"+     byteHeap.toASCIIString(target)+" len "+byteHeap.length(target, byteHeap));
        	
            int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);   
            if (length<0) {
                PrimitiveWriter.writeIntegerSigned(0, writer);
            } else {
                int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
                int offset = RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length);
                // constant from heap or dynamic from char ringBuffer
                byte[] buffer = RingBuffer.byteBackingArray(rawPos, rbRingBuffer);
                int byteMask = rbRingBuffer.byteMask;
                // count matching front or back chars
                int headCount = LocalHeap.countHeadMatch(target,buffer,offset,length,byteMask,byteHeap);
                int tailCount = LocalHeap.countTailMatch(target,buffer,offset,length,byteMask,byteHeap);
                if (headCount > tailCount) {
                    int trimTail = LocalHeap.length(target,byteHeap) - headCount;
                    assert (trimTail >= 0);
                    PrimitiveWriter.writeIntegerSigned(trimTail + 1, writer);// must add one because this
                                                            // is optional
                    PrimitiveWriter.writeTextASCIIAfter(headCount, buffer, offset, length ,byteMask, writer);
                    LocalHeap.appendTail(target,trimTail,buffer,offset,headCount,byteMask,byteHeap);
                } else {
                    int trimHead = LocalHeap.length(target,byteHeap) - tailCount;
                    PrimitiveWriter.writeIntegerSigned(0 == trimHead ? 1 : -trimHead, writer);
                    
                    int sentLen = length - tailCount;
                    PrimitiveWriter.writeTextASCIIBefore(buffer,offset,byteMask, sentLen, writer);
                    LocalHeap.appendHead(target,trimHead,buffer,offset,sentLen,byteMask,byteHeap);
                }
            }
        }
    }

    protected void genWriteTextTailOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
        	//System.err.println("value to encode for tail:"+     byteHeap.toASCIIString(target));
        	
            int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
            int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
            int offset = RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = RingBuffer.byteBackingArray(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;
            
            if (length<0) {
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(target, byteHeap); // no pmap, yes change to last value
            } else {
                int headCount = LocalHeap.countHeadMatch(target,buffer,offset,length,byteMask,byteHeap);
                int trimTail = LocalHeap.length(target,byteHeap) - headCount;
                PrimitiveWriter.writeIntegerUnsigned(trimTail + 1, writer);
                PrimitiveWriter.writeTextASCIIAfter(headCount, buffer, offset, length ,byteMask, writer);
                LocalHeap.appendTail(target,trimTail,buffer,offset,headCount,byteMask,byteHeap);
            }
        }
    }

    protected void genWriteNull(PrimitiveWriter writer) {
        PrimitiveWriter.writeNull(writer);
    }
    
    
    protected void genWriteTextDelta(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
                	
            int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
            int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
            
            
            int offset = RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = RingBuffer.byteBackingArray(rawPos, rbRingBuffer);
            
            //System.err.println("the string to be written "+new String(buffer,offset,length));
            
            int byteMask = rbRingBuffer.byteMask;
            
            if (length>rbRingBuffer.maxAvgVarLen || length > writer.bufferSize) {
            	throw new UnsupportedOperationException("Text is too long found length:"+length+" writer limited to:"+writer.bufferSize+" and "+rbRingBuffer.maxAvgVarLen);
            }

            // count matching front or back chars
            int headCount = LocalHeap.countHeadMatch(target,buffer,offset,length,byteMask,byteHeap);
            int tailCount = LocalHeap.countTailMatch(target,buffer,offset,length,byteMask,byteHeap);
            if (headCount > tailCount) {
                int trimTail = LocalHeap.length(target,byteHeap) - headCount;
                if (trimTail < 0) {
                    throw new UnsupportedOperationException(trimTail + "");
                }
                PrimitiveWriter.writeIntegerSigned(trimTail, writer);
                PrimitiveWriter.writeTextASCIIAfter(headCount, buffer, offset, length ,byteMask, writer);
                LocalHeap.appendTail(target,trimTail,buffer,offset,headCount,byteMask,byteHeap);
            } else {
                int trimHead = LocalHeap.length(target,byteHeap) - tailCount;
                PrimitiveWriter.writeIntegerSigned(0 == trimHead ? 0 : -trimHead, writer);
                
                int sentLen = length - tailCount;             
                PrimitiveWriter.writeTextASCIIBefore(buffer,offset, byteMask, sentLen, writer);
                LocalHeap.appendHead(target,trimHead,buffer,offset,sentLen,byteMask,byteHeap);
            }
        }
    }
    
    protected void genWriteTextTail(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
            int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
            int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
            int offset = RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = RingBuffer.byteBackingArray(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;
            
            
            
            int headCount = LocalHeap.countHeadMatch(target,buffer,offset,length,byteMask,byteHeap);
            int trimTail = LocalHeap.length(target,byteHeap) - headCount;
            PrimitiveWriter.writeIntegerUnsigned(trimTail, writer);
            PrimitiveWriter.writeTextASCIIAfter(headCount, buffer, offset, length ,byteMask, writer);
            LocalHeap.appendTail(target,trimTail,buffer,offset,headCount,byteMask,byteHeap);
        }
    }

    protected void genWriteTextNone(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        {
            int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
            int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
            int offset = RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = RingBuffer.byteBackingArray(rawPos, rbRingBuffer);
            PrimitiveWriter.writeTextASCII(buffer, offset, length, rbRingBuffer.byteMask, writer);
        }
    }
    
    protected void genWriteTextNoneOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        {
            int length = RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos);
            if (length<0) {
                PrimitiveWriter.writeNull(writer);
            } else{        
                int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);              
                PrimitiveWriter.writeTextASCII(RingBuffer.byteBackingArray(rawPos, rbRingBuffer), RingBuffer.bytePositionGen(rawPos, rbRingBuffer, length), length, rbRingBuffer.byteMask, writer);
            }
        }
    }
        
    protected void genWriteTextConstantOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit((byte)(1&(1+(RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos)>>>31))), writer);//0 for null, 1 for present
    }
   
    // if (byteHeap.equals(target|INIT_VALUE_MASK, value, offset, length)) {
    protected void genWriteBytesDefault(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
         
        int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
        if (0==(rawPos>>>31)) {//use the default whatever it is
            int len = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(len, writer);            
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len),len, rbRingBuffer.byteMask, writer); 
        } else {
            PrimitiveWriter.writePMapBit((byte)0, writer); 
        }   
        
    }

    protected void genWriteBytesCopy(int target, int fieldPos, LocalHeap byteHeap, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
    	{
	    	int len = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
	    	assert(len>=0) :"must not be null, those have to be optional";
	        if (LocalHeap.equals(target,rbRingBuffer.byteBuffer,RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len),len,rbRingBuffer.byteMask,byteHeap)) {
	            PrimitiveWriter.writePMapBit((byte)0, writer);
	        } else {
	            PrimitiveWriter.writePMapBit((byte)1, writer);
	            PrimitiveWriter.writeIntegerUnsigned(len, writer);
	            
	            int offset = RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len);
	            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, offset, len, rbRingBuffer.byteMask, writer);
	            LocalHeap.set(target,rbRingBuffer.byteBuffer,offset,len,rbRingBuffer.byteMask,byteHeap);
	        }
	    }
    }

    public void genWriteBytesDelta(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
            int length = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            int offset = RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, length);
            int mask = rbRingBuffer.byteMask;
            byte[] value = rbRingBuffer.byteBuffer;
            
            //count matching front or back chars
            int headCount = LocalHeap.countHeadMatch(target,rbRingBuffer.byteBuffer,offset,length,rbRingBuffer.byteMask,byteHeap);
            int tailCount = LocalHeap.countTailMatch(target,rbRingBuffer.byteBuffer,offset+length,length,rbRingBuffer.byteMask,byteHeap);
            if (headCount>tailCount) {
                int trimTail = LocalHeap.length(target,byteHeap)-headCount;
                PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail, writer);
                
                int valueSend = length-headCount;
                int startAfter = offset+headCount+headCount;
                
                PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
                PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, mask, writer);
                LocalHeap.appendTail(target,trimTail,value,startAfter,valueSend,mask,byteHeap);
            } else {
                //replace head, tail matches to tailCount
                int trimHead = LocalHeap.length(target,byteHeap)-tailCount;
                PrimitiveWriter.writeIntegerSigned(trimHead==0? 0: -trimHead, writer); 
                
                int len = length - tailCount;
                PrimitiveWriter.writeIntegerUnsigned(len, writer);
                PrimitiveWriter.writeByteArrayData(value, offset, len, mask, writer);            
                LocalHeap.appendHead(target,trimHead,value,offset,len,mask,byteHeap);
            }
        }
    }

    public void genWriteBytesTail(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
            int length = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            int offset = RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, length);
            int mask = rbRingBuffer.byteMask;
            byte[] value = rbRingBuffer.byteBuffer;
            
            int headCount = LocalHeap.countHeadMatch(target,rbRingBuffer.byteBuffer,offset,length,rbRingBuffer.byteMask,byteHeap);
            
            int trimTail = LocalHeap.length(target,byteHeap)-headCount;
            PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail, writer);
            
            int valueSend = length-headCount;
            int startAfter = offset+headCount;
            
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, mask, writer);
            LocalHeap.appendTail(target,trimTail,value,startAfter,valueSend,mask,byteHeap);
        }
    }

    protected void genWriteBytesNone(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        {
            int len = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len),len, rbRingBuffer.byteMask, writer);
        }
    }
    
    public void genWriteBytesDefaultOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
       
        int rawPos = RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value);
        if (0==(rawPos>>>31)) {//use the default whatever it is
            int len = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(len+1, writer);
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len),len, rbRingBuffer.byteMask, writer);
             
        } else {
            PrimitiveWriter.writePMapBit((byte)0, writer); 
        }   

    }

    public void genWriteBytesCopyOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
    	{
    		
    		
	    	int len = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
	        
	        
	    	
	    	if (LocalHeap.equals(target,rbRingBuffer.byteBuffer,RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len),len,rbRingBuffer.byteMask,byteHeap)) {
	            PrimitiveWriter.writePMapBit((byte)0, writer);
	        } else {
	            PrimitiveWriter.writePMapBit((byte)1, writer);
	            
	            PrimitiveWriter.writeIntegerUnsigned(len+1, writer);
	            
	            int offset = RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len);
	            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, offset, len, rbRingBuffer.byteMask, writer);
	            LocalHeap.set(target,rbRingBuffer.byteBuffer,offset,len,rbRingBuffer.byteMask,byteHeap);
	        }
    	}
    }

    public void genWriteBytesDeltaOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
            int length = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            
            if (length<0) {            
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(target, byteHeap); // no pmap, yes change to last value
            } else {
            
                int offset = RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, length);
                int mask = rbRingBuffer.byteMask;
                byte[] value = rbRingBuffer.byteBuffer;
        
                //count matching front or back chars
                int headCount = LocalHeap.countHeadMatch(target,rbRingBuffer.byteBuffer,offset,length,rbRingBuffer.byteMask,byteHeap);
                int tailCount = LocalHeap.countTailMatch(target,rbRingBuffer.byteBuffer,offset+length,length,rbRingBuffer.byteMask,byteHeap);
                if (headCount>tailCount) {
                    int trimTail = LocalHeap.length(target,byteHeap)-headCount;
                    PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail, writer);
                    
                    int valueSend = length-headCount;
                    int startAfter = offset+headCount;
                    
                    PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
                    PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, mask, writer);
                    LocalHeap.appendTail(target,trimTail,value,startAfter,valueSend,mask,byteHeap);
                } else {
                    //replace head, tail matches to tailCount
                    int trimHead = LocalHeap.length(target,byteHeap)-tailCount;
                    PrimitiveWriter.writeIntegerSigned(trimHead==0? 1: -trimHead, writer); 
                    
                    int len = length - tailCount;
                    PrimitiveWriter.writeIntegerUnsigned(len, writer);
                    PrimitiveWriter.writeByteArrayData(value, offset, len, mask, writer);
                    
                    LocalHeap.appendHead(target,trimHead,value,offset,len,mask,byteHeap);
                }
            }
        }
    }

    protected void genWriteBytesConstantOptional(int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit((byte)(1&(1+(RingBuffer.readRingByteLen(fieldPos, rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos)>>>31))), writer);//0 for null, 1 for present
    }

    public void genWriteBytesTailOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, RingBuffer rbRingBuffer) {
        {
            int length = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            if (length<0) {
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(target, byteHeap); // no pmap, yes change to last value            
            } else {
            
                int offset = RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, length);
                int mask = rbRingBuffer.byteMask;
                byte[] value = rbRingBuffer.byteBuffer;
                        
                int headCount = LocalHeap.countHeadMatch(target,rbRingBuffer.byteBuffer,offset,length,rbRingBuffer.byteMask,byteHeap);
                int trimTail = LocalHeap.length(target,byteHeap)-headCount;
                PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail, writer);
                
                int valueSend = length-headCount;
                int startAfter = offset+headCount;
                
                PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
                PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, mask, writer);
                LocalHeap.appendTail(target,trimTail,value,startAfter,valueSend,mask,byteHeap);
            }
        }
    }

    protected void genWriteBytesNoneOptional(int target, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, LocalHeap byteHeap) {
        {
            int len = RingBuffer.readRingByteLen(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos);
            if (len<0) {
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(target, byteHeap); // no pmap, yes change to last value
            } else {
                PrimitiveWriter.writeIntegerUnsigned(len+1, writer);
                PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, RingBuffer.bytePositionGen(RingBuffer.readValue(fieldPos,rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value), rbRingBuffer, len),len, rbRingBuffer.byteMask, writer);
            }
        }
    }
    
    protected void genWriteIntegerSignedDefault(int constDefault, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (value == constDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(value, writer);
            }
        }
    }

    protected void genWriteIntegerSignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            int incVal;
            if (value == (incVal = rIntDictionary[source] + 1)) {
                rIntDictionary[target] = incVal;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                rIntDictionary[target] = value;
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(value, writer); //same as unsigned increment except for this one line
            }
        }
    }

    protected void genWriteIntegerSignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (value == rIntDictionary[source]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(rIntDictionary[target] = value, writer);
            }
        }
    }

    protected void genWriteIntegerSignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            int last = rIntDictionary[source];
            if (value > 0 == last > 0) { // optimization using int when possible instead of long
                PrimitiveWriter.writeIntegerSigned(value - last, writer);
                rIntDictionary[target] = value;
            } else {
                PrimitiveWriter.writeLongSigned(value - (long) last, writer);
                rIntDictionary[target] = value;
            }
        }
    }

    protected void genWriteIntegerSignedNone(int target, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        PrimitiveWriter.writeIntegerSigned(rIntDictionary[target] = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos), writer);
    }
    
    protected void genWriteIntegerUnsignedDefault(int constDefault, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (value == constDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerUnsigned(value, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedIncrement( int target, int source, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            int incVal;
            if (value == (incVal = rIntDictionary[source] + 1)) {
                rIntDictionary[target] = incVal;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                rIntDictionary[target] = value;
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerUnsigned(value, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
        //    System.err.println("copy int to write:"+value+" dictionary "+rIntDictionary[source]);
            if (value == rIntDictionary[source]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerUnsigned(rIntDictionary[target] = value, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {   
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
         //   System.err.println("delta int to write:"+value);
            PrimitiveWriter.writeLongSigned(value - (long) rIntDictionary[source], writer);
            rIntDictionary[target] = value;
        }
    }

    protected void genWriteIntegerUnsignedNone(int target, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        PrimitiveWriter.writeIntegerUnsigned(rIntDictionary[target] = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos), writer);
    }

    protected void genWriteIntegerSignedDefaultOptional(int source, int fieldPos, int constDefault, int valueOfNull, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                if (0 == rIntDictionary[source]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                } // null for default 
            } else {
                int value1 = value>=0?value+1:value;
                if (value1 == constDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(value1, writer);
                }
            }
        }
    }

    protected void genWriteIntegerSignedIncrementOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                if (0 == rIntDictionary[source]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else { 
                int last = rIntDictionary[source];
                int value1 = rIntDictionary[target] = (1+(value + (value >>> 31)));
                if (0 != last && value1 == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(value1, writer);
                }  
            }
        }
    }

    protected void genWriteIntegerSignedCopyOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                if (0 == rIntDictionary[source]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else {        
                int value1 = (1+(value + (value >>> 31)));
                if (value1 == rIntDictionary[source]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(rIntDictionary[target] = value1, writer);
                }
            }
        }   
    }

    //this is how a "boolean" is sent using a single bit in the encoding.
    protected void genWriteIntegerSignedConstantOptional(int valueOfNull, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
        PrimitiveWriter.writePMapBit(valueOfNull==RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos) ? (byte)0 : (byte)1, writer);  // 1 for const, 0 for absent
    }

    protected void genWriteIntegerSignedDeltaOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                rIntDictionary[target] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                int last = rIntDictionary[source];
                if (value > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = value - last;
                    rIntDictionary[target] = value;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = value - (long) last;
                    rIntDictionary[target] = value;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
            }
        }
    }

    protected void genWriteIntegerSignedNoneOptional(int target, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                rIntDictionary[target] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(value, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedCopyOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                if (0 == rIntDictionary[source]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else { 
                int value1 = 1+value;
                if (value1 == rIntDictionary[source]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerUnsigned(rIntDictionary[target] = value1, writer);
                }
            }
        }
    }

    
   
    protected void genWriteIntegerUnsignedDefaultOptional(int source, int fieldPos, int valueOfNull, int constDefault, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            
            if (valueOfNull == value) {
                if (0 == rIntDictionary[source]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                } // null for default 
            } else {
                int value1 = 1+value;
                if (value1 == constDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerUnsigned(value1, writer);
                }
            }
        }
    }

    
    
    protected void genWriteIntegerUnsignedIncrementOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {   
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                if (0 == rIntDictionary[source]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else { 
                if (0 != rIntDictionary[source] && value == (rIntDictionary[target] = rIntDictionary[source] + 1)) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    int tmp = rIntDictionary[target] = 1 + value;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerUnsigned(tmp, writer);
                }
            }
        }
    }

    protected void genWriteIntegerUnsignedConstantOptional(int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
        PrimitiveWriter.writePMapBit(valueOfNull==RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos) ? (byte)0 : (byte)1, writer);  // 1 for const, 0 for absent
    }

    
    protected void genWriteIntegerUnsignedDeltaOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] rIntDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                rIntDictionary[target] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                long delta = value - (long) rIntDictionary[source];//unable to leave as is for client
                PrimitiveWriter.writeLongSigned( (1+(delta + (delta >> 63))), writer);
                rIntDictionary[target] = value;
            }
        }
    }

    protected void genWriteIntegerUnsignedNoneOptional(int target, int valueOfNull, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, int[] rIntDictionary) {
        {
            int value = RingBuffer.readInt(rbB, rbMask, rbPos.value +  fieldPos);
            if (valueOfNull == value) {
                rIntDictionary[target] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerUnsigned(value + 1, writer);
            }
        }
    }
    
    ////////////////////////
    ///Decimals with optional exponent
    /////////////////////////

    //None
    
      protected void genWriteDecimalDefaultOptionalNone(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, int[] rIntDictionary, FASTEncoder dispatch) {
      {
        int exponentValue = rbRingBuffer.buffer[rbRingBuffer.mask & (int)(rbRingBuffer.workingTailPos.value+ fieldPos)];  
        if (exponentValueOfNull == exponentValue) {
            if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeNull(writer);
            } // null for default 
        } else {
            if (exponentValue == exponentConstDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
            	int value = exponentValue>=0?exponentValue+1:exponentValue; //moved here but can we do this branch free
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(value, writer);
            }
            assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
           
            PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1), writer); 
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        {
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
            if (exponentValueOfNull == exponentValue) {
                if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else { 
                int last = rIntDictionary[exponentSource];
                int value = rIntDictionary[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue);
                if (0 != last && value == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(value, writer);
                } 
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               
                PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1), writer); 
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        {   
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
    //        System.err.println("t write exponent:"+exponentValue);
            if (exponentValueOfNull == exponentValue) {
      //      	System.err.println("t nullllll");
                if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                    System.err.println("A write map 0 "+exponentValue);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    System.err.println("A write map 1 "+exponentValue);
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else {        
                int value = exponentValue>=0?exponentValue+1:exponentValue;
                
                if (value == rIntDictionary[exponentSource]) {
      //          	System.err.println("t pmap");
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                   // System.err.println("B write map 0 "+exponentValue);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                  //  System.err.println("B write map 1 "+exponentValue+" value written "+value);
                    PrimitiveWriter.writeIntegerSigned(rIntDictionary[exponentTarget] = value, writer);
                }
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               
                long mantissa = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
    //            System.err.println("t write mantissa:"+mantissa);
				PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = mantissa, writer); 
            }
        }
    }

    protected void genWriteDecimalConstantOptionalNone(int exponentValueOfNull, int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        { 
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
            if (exponentValueOfNull==exponentValue) {
                PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               
                PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1), writer);        
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalNone(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        {   
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                int last = rIntDictionary[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               
                PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1), writer); 
            }
        }
    }

    protected void genWriteDecimalNoneOptionalNone(int exponentTarget, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
        {   
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               
                PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1), writer); 
            }
        }
    }
    
    // DEFAULTS
   
   
     protected void genWriteDecimalDefaultOptionalDefault(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, int[] rIntDictionary, FASTEncoder dispatch) {
      {
        int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
        if (exponentValueOfNull == exponentValue) {
            if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeNull(writer);
            } // null for default 
        } else {
            int value = exponentValue>=0?exponentValue+1:exponentValue;
            if (value == exponentConstDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(value, writer);
            }
            assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
            long value1 = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
           
            //mantissa
            if (value1 == mantissaConstDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value1, writer);
            }
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
            if (exponentValueOfNull == exponentValue) {
                if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else { 
                int last = rIntDictionary[exponentSource];
                int value = rIntDictionary[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue);
                if (0 != last && value == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(value, writer);
                } 
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                long value1 = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               
                //mantissa
                if (value1 == mantissaConstDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(value1, writer);
                }
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {   
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
            } else {        
                int value = (1+(exponentValue + (exponentValue >>> 31)));
                if (value == rIntDictionary[exponentSource]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(rIntDictionary[exponentTarget] = value, writer);
                }
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                long value1 = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               
                //mantissa
                if (value1 == mantissaConstDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(value1, writer);
                }
            }
        }
    }

    protected void genWriteDecimalConstantOptionalDefault(int exponentValueOfNull, int mantissaTarget, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        { 
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
            if (exponentValueOfNull==exponentValue) {
                PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               
                //mantissa
                if (value == mantissaConstDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(value, writer);
                }
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalDefault(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {   
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                int last = rIntDictionary[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               
                //mantissa
                if (value == mantissaConstDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(value, writer);
                }
            }
        }
    }

    protected void genWriteDecimalNoneOptionalDefault(int exponentTarget, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {   
            int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               
                //mantissa
                if (value == mantissaConstDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(value, writer);
                }
            }
        }
    }
    
    
    // Increment
    
    
    protected void genWriteDecimalDefaultOptionalIncrement(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
     {
       int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
       if (exponentValueOfNull == exponentValue) {
           if (0 == rIntDictionary[exponentSource]) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeNull(writer);
        } // null for default 
       } else {
           int value1 = exponentValue>=0?exponentValue+1:exponentValue;
        if (value1 == exponentConstDefault) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerSigned(value1, writer);
        }
           assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
          
           //mantissa
           long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
           if (value == (1 + rLongDictionary[mantissaSource])) {
           PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
           PrimitiveWriter.writePMapBit((byte)1, writer);
           PrimitiveWriter.writeLongSigned(value, writer);
        }
           rLongDictionary[mantissaTarget] = value;
       }
     }
   }

   protected void genWriteDecimalIncrementOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
       {
           int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
           if (exponentValueOfNull == exponentValue) {
               if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeNull(writer);
            }// null for Copy and Increment 
           } else { 
               int last = rIntDictionary[exponentSource];
            int value1 = rIntDictionary[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue);
               if (0 != last && value1 == 1 + last) {// not null and matches
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(value1, writer);
            } 
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
              
               //mantissa
               long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               if (value == (1 + rLongDictionary[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               rLongDictionary[mantissaTarget] = value;
           }   
       }
   }

   protected void genWriteDecimalCopyOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
       {   
           int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
           if (exponentValueOfNull == exponentValue) {
               if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeNull(writer);
            }// null for Copy and Increment 
           } else {        
               int value1 = exponentValue>=0?exponentValue+1:exponentValue;
            if (value1 == rIntDictionary[exponentSource]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(rIntDictionary[exponentTarget] = value1, writer);
            }
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
              
               //mantissa
               long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               if (value == (1 + rLongDictionary[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               rLongDictionary[mantissaTarget] = value;
           }
       }
   }

   protected void genWriteDecimalConstantOptionalIncrement(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
       { 
           int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
           if (exponentValueOfNull==exponentValue) {
               PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
           } else {
               PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
              
               //mantissa
               long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               if (value == (1 + rLongDictionary[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               rLongDictionary[mantissaTarget] = value;
           }     
       }
   }

   protected void genWriteDecimalDeltaOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
       {   
           int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
           if (exponentValueOfNull == exponentValue) {
               rIntDictionary[exponentTarget] = 0;
            PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
           } else {
               int last = rIntDictionary[exponentSource];
            if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                int dif = exponentValue - last;
                rIntDictionary[exponentTarget] = exponentValue;
                PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
            } else {
                long dif = exponentValue - (long) last;
                rIntDictionary[exponentTarget] = exponentValue;
                PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
            }
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
              
               //mantissa
               long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               if (value == (1 + rLongDictionary[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               rLongDictionary[mantissaTarget] = value;
           }
       }
   }

   protected void genWriteDecimalNoneOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
       {   
           int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
           if (exponentValueOfNull == exponentValue) {
               rIntDictionary[exponentTarget] = 0;
            PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
           } else {
               PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
              
               //mantissa
               long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
               if (value == (1 + rLongDictionary[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               rLongDictionary[mantissaTarget] = value;
           }
       }
   }
    
   //copy
   
   protected void genWriteDecimalDefaultOptionalCopy(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
       {
         int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
         if (exponentValueOfNull == exponentValue) {
             if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeNull(writer);
            } // null for default 
         } else {
             int value1 = exponentValue>=0?exponentValue+1:exponentValue;
            if (value1 == exponentConstDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(value1, writer);
            }
             assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
            
             //mantissa
             long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);   
             if (value == rLongDictionary[mantissaSource]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = value, writer);
            }
         }
       }
     }

     protected void genWriteDecimalIncrementOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
         {
             int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
             if (exponentValueOfNull == exponentValue) {
                 if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
             } else { 
                 int last = rIntDictionary[exponentSource];
                int value1 = rIntDictionary[exponentTarget] = (1+(exponentValue + (exponentValue >>> 31)));
                 if (0 != last && value1 == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(value1, writer);
                } 
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                
                 //mantissa
                 long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
                 if (value == rLongDictionary[mantissaSource]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = value, writer);
                }
             }   
         }
     }

     protected void genWriteDecimalCopyOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
         {   
             int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
             if (exponentValueOfNull == exponentValue) {
                 if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
             } else {  
            	 {
	                int value1 = (1+(exponentValue + (exponentValue >>> 31)));
	                if (value1 == rIntDictionary[exponentSource]) {
	                    PrimitiveWriter.writePMapBit((byte)0, writer);
	                } else {
	                    PrimitiveWriter.writePMapBit((byte)1, writer);
	                    PrimitiveWriter.writeIntegerSigned(rIntDictionary[exponentTarget] = value1, writer);
	                }
            	 }
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                 {
	                 //mantissa
	                 long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
	                 if (value == rLongDictionary[mantissaSource]) {
	                    PrimitiveWriter.writePMapBit((byte)0, writer);
	                } else {
	                    PrimitiveWriter.writePMapBit((byte)1, writer);
	                    PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = value, writer);
	                }
                 }
             }
         }
     }

     protected void genWriteDecimalConstantOptionalCopy(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
         { 
             int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
             if (exponentValueOfNull==exponentValue) {
                 PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
             } else {
                 PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                
                 //mantissa
                 long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
                 if (value == rLongDictionary[mantissaSource]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = value, writer);
                }
             }     
         }
     }

     protected void genWriteDecimalDeltaOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
         {   
             int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
             if (exponentValueOfNull == exponentValue) {
                 rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
             } else {
                 int last = rIntDictionary[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                
                 //mantissa
                 long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
                 if (value == rLongDictionary[mantissaSource]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = value, writer);
                }
             }
         }
     }

     protected void genWriteDecimalNoneOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
         {   
             int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
             System.err.println("write exponent:"+exponentValue);
             if (exponentValueOfNull == exponentValue) {
                 rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
             } else {
                 PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                
                 //mantissa
                 long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1);
                 System.err.println("write mantissa:"+value);
                 if (value == rLongDictionary[mantissaSource]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(rLongDictionary[mantissaTarget] = value, writer);
                }
             }
         }
     } 
   
   //constant
  
     protected void genWriteDecimalDefaultOptionalConstant(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, int[] rIntDictionary, FASTEncoder dispatch) {
         {
           int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
           if (exponentValueOfNull == exponentValue) {
               if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeNull(writer);
            } // null for default 
           } else {
               int value = (1+(exponentValue + (exponentValue >>> 31)));
            if (value == exponentConstDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeIntegerSigned(value, writer);
            }
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
              
               //mantissa
               //is constant so do nothing
           }
         }
       }

       protected void genWriteDecimalIncrementOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {
               int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
               if (exponentValueOfNull == exponentValue) {
                   if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
               } else { 
                   int last = rIntDictionary[exponentSource];
                int value = rIntDictionary[exponentTarget] = (1+(exponentValue + (exponentValue >>> 31)));
                   if (0 != last && value == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(value, writer);
                } 
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                  
                   //mantissa
                   //is constant so do nothing
               }   
           }
       }

       protected void genWriteDecimalCopyOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {   
               int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
               if (exponentValueOfNull == exponentValue) {
                   if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }// null for Copy and Increment 
               } else {        
                   int value = (1+(exponentValue + (exponentValue >>> 31)));
                if (value == rIntDictionary[exponentSource]) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(rIntDictionary[exponentTarget] = value, writer);
                }
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                  
                   //mantissa
                   //is constant so do nothing
               }
           }
       }

       protected void genWriteDecimalConstantOptionalConstant(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
           { 
               int exponentValue = rbRingBuffer.buffer[rbRingBuffer.mask & (int)(rbRingBuffer.workingTailPos.value+ fieldPos)]; 
               if (exponentValueOfNull==exponentValue) {
                   PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
               } else {
                   PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                  
                   //mantissa
                   //is constant so do nothing
               }     
           }
       }

       protected void genWriteDecimalDeltaOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {   
               int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
               if (exponentValueOfNull == exponentValue) {
                   rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
               } else {
                   int last = rIntDictionary[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    rIntDictionary[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                  
                   //mantissa
                   //is constant so do nothing
               }
           }
       }

       protected void genWriteDecimalNoneOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {   
               int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
               if (exponentValueOfNull == exponentValue) {
                   rIntDictionary[exponentTarget] = 0;
                PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
               } else {
                   PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                  
                   //mantissa
                   //is constant so do nothing
               }
           }
       }      
     
  //delta
       
       protected void genWriteDecimalDefaultOptionalDelta(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] rLongDictionary, int[] rIntDictionary, FASTEncoder dispatch) {
           {
             int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
             if (exponentValueOfNull == exponentValue) {
                 if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                } // null for default 
             } else {
                 int value1 = (1+(exponentValue + (exponentValue >>> 31)));
                if (value1 == exponentConstDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeIntegerSigned(value1, writer);
                }
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));

                 //mantissa
                 long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1); 
                  PrimitiveWriter.writeLongSigned(value - rLongDictionary[mantissaSource], writer);
                 rLongDictionary[mantissaTarget] = value;
             }
           }
         }

         protected void genWriteDecimalIncrementOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
             {
                 int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos);  
                 if (exponentValueOfNull == exponentValue) {
                     if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                        PrimitiveWriter.writePMapBit((byte)0, writer);
                    } else {
                        rIntDictionary[exponentTarget] = 0;
                        PrimitiveWriter.writePMapBit((byte)1, writer);
                        PrimitiveWriter.writeNull(writer);
                    }// null for Copy and Increment 
                 } else { 
                     int last = rIntDictionary[exponentSource];
                    int value1 = rIntDictionary[exponentTarget] = (1+(exponentValue + (exponentValue >>> 31)));
                     if (0 != last && value1 == 1 + last) {// not null and matches
                        PrimitiveWriter.writePMapBit((byte)0, writer);
                    } else {
                        PrimitiveWriter.writePMapBit((byte)1, writer);
                        PrimitiveWriter.writeIntegerSigned(value1, writer);
                    } 
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                    
                     //mantissa
                     long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1); 
                     PrimitiveWriter.writeLongSigned(value - rLongDictionary[mantissaSource], writer);
                     rLongDictionary[mantissaTarget] = value;
                 }   
             }
         }

         protected void genWriteDecimalCopyOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] longValue, FASTEncoder dispatch) {
             {   
                 int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
                 if (exponentValueOfNull == exponentValue) {
                     if (0 == rIntDictionary[exponentSource]) { // stored value was null;
                        PrimitiveWriter.writePMapBit((byte)0, writer);
                    } else {
                        rIntDictionary[exponentTarget] = 0;
                        PrimitiveWriter.writePMapBit((byte)1, writer);
                        PrimitiveWriter.writeNull(writer);
                    }// null for Copy and Increment 
                 } else {        
                     int value1 = (1+(exponentValue + (exponentValue >>> 31)));
                    if (value1 == rIntDictionary[exponentSource]) {
                        PrimitiveWriter.writePMapBit((byte)0, writer);
                    } else {
                        PrimitiveWriter.writePMapBit((byte)1, writer);
                        PrimitiveWriter.writeIntegerSigned(rIntDictionary[exponentTarget] = value1, writer);
                    }
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                    
                     //mantissa
                     long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1); 
                     PrimitiveWriter.writeLongSigned(value - rLongDictionary[mantissaSource], writer);
                     longValue[mantissaTarget] = value;
                 }
             }
         }

         protected void genWriteDecimalConstantOptionalDelta(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, RingBuffer rbRingBuffer, long[] longValue, FASTEncoder dispatch) {
             { 
                 int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
                 if (exponentValueOfNull==exponentValue) {
                     PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
                 } else {
                     PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                    
                     //mantissa
                     long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1); 
                     PrimitiveWriter.writeLongSigned(value - rLongDictionary[mantissaSource], writer);
                     longValue[mantissaTarget] = value;
                 }     
             }
         }

         protected void genWriteDecimalDeltaOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
             {   
                 int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
                 if (exponentValueOfNull == exponentValue) {
                     rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
                 } else {
                     int last = rIntDictionary[exponentSource];
                    if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                        int dif = exponentValue - last;
                        rIntDictionary[exponentTarget] = exponentValue;
                        PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                    } else {
                        long dif = exponentValue - (long) last;
                        rIntDictionary[exponentTarget] = exponentValue;
                        PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                    }
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                    
                     //mantissa
                     long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1); 
                     PrimitiveWriter.writeLongSigned(value - rLongDictionary[mantissaSource], writer);
                     rLongDictionary[mantissaTarget] = value;
                 }
             }
         }

         protected void genWriteDecimalNoneOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] rIntDictionary, RingBuffer rbRingBuffer, long[] rLongDictionary, FASTEncoder dispatch) {
             {  
                 //FASTEncoder dispatch
                 int exponentValue = RingBuffer.readInt(rbRingBuffer.buffer,rbRingBuffer.mask,rbRingBuffer.workingTailPos.value+ fieldPos); 
                 if (exponentValueOfNull == exponentValue) {
                     rIntDictionary[exponentTarget] = 0;
                    PrimitiveWriter.writeNull(writer);// null for None and Delta (both do not use pmap)
                 } else {
                     PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                    
                     //mantissa
                     long value = RingBuffer.readLong(rbRingBuffer.buffer, rbRingBuffer.mask, rbRingBuffer.workingTailPos.value + fieldPos + 1); 
                     PrimitiveWriter.writeLongSigned(value - rLongDictionary[mantissaSource], writer);
                     rLongDictionary[mantissaTarget] = value;
                 }
             }
         } 
          
    
    //////////////
    //end of decimals
    ///////////////
    
    protected void genWriteLongUnsignedDefault(long constDefault, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {        
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == constDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongUnsigned(value, writer);
            }
        }
    }

    protected void genWriteLongUnsignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            long incVal = rLongDictionary[source] + 1;
            if (value == incVal) {
                rLongDictionary[target] = incVal;
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                rLongDictionary[target] = value;
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongUnsigned(value, writer);
            }
        }
    }

    protected void genWriteLongUnsignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == rLongDictionary[source]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongUnsigned(rLongDictionary[target] = value, writer);
            }
        }
    }

    protected void genWriteLongUnsignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);           
            PrimitiveWriter.writeLongSigned(value - rLongDictionary[source], writer);
            rLongDictionary[target] = value;
        }
    }

    protected void genWriteLongUnsignedNone(int target, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        PrimitiveWriter.writeLongUnsigned(rLongDictionary[target] = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos), writer);
    }
    
    protected void genWriteLongUnsignedDefaultOptional(long valueOfNull, int target, long constDefault, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, long[] rLongDictionary) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == valueOfNull) {
                if (rLongDictionary[target] == 0) { // stored value was null; //for default
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
              }
            long value1 = 1+value;
            // room for zero
            if (value1 == constDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongUnsigned(value1, writer);
            }
        }
    }

    protected void genWriteLongUnsignedIncrementOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            //for copy and inc
            if (value == valueOfNull) {
                if (0 == rLongDictionary[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rLongDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
            if (0 != rLongDictionary[source] && value == (rLongDictionary[target] = rLongDictionary[source] + 1)) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongUnsigned(rLongDictionary[target] = 1 + value, writer);
            }
        }
    }

    protected void genWriteLongUnsignedCopyOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            //for copy and inc
            if (value == valueOfNull) {
                if (0 == rLongDictionary[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rLongDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
            value++;// zero is held for null
            
            if (value == rLongDictionary[source]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongUnsigned(rLongDictionary[target] = value, writer);
            }
        }
    }

    //TODO: B, can optimize be creating FASTRingBufferReader.isLongEqual(rbRingBuffer, fieldPos, valueOfNull)
    protected void genWriteLongUnsignedConstantOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
            PrimitiveWriter.writePMapBit(RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos)==valueOfNull ? (byte)0 : (byte)1, writer);
    }


    //      System.err.println(fieldPos+" fieldPos write long unsigned optional none to :"+(writer.limit+writer.totalWritten(writer))+" of "+value+" null "+(value == valueOfNull)+" vs "+valueOfNull);
    protected void genWriteLongUnsignedNoneOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == valueOfNull) {
                rLongDictionary[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            } else {
                PrimitiveWriter.writeLongUnsigned(value + 1, writer);
            }
        }
    }

    protected void genWriteLongUnsignedDeltaOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == valueOfNull) {
                rLongDictionary[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            } else {
                long delta = value - rLongDictionary[source];
                PrimitiveWriter.writeLongSigned((1+(delta + (delta >> 63))), writer);
                rLongDictionary[target] = value;
            }
        }
    }
    
    protected void genWriteLongSignedDefault(long constDefault, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == constDefault) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
        }
    }

    protected void genWriteLongSignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == (1 + rLongDictionary[source])) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
            rLongDictionary[target] = value;
        }
    }

    protected void genWriteLongSignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == rLongDictionary[source]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(rLongDictionary[target] = value, writer);
            }
        }
    }

    protected void genWriteLongSignedNone(int target, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        PrimitiveWriter.writeLongSigned(rLongDictionary[target] = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos), writer);
    }

    protected void genWriteLongSignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            PrimitiveWriter.writeLongSigned(value - rLongDictionary[source], writer);
            rLongDictionary[target] = value;
        }
    }
    
    protected void genWriteLongSignedOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == valueOfNull) {
                rLongDictionary[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            } else {
                PrimitiveWriter.writeLongSignedOptional(value, writer);
            }
        }
    }

    protected void genWriteLongSignedDeltaOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            if (value == valueOfNull) {
                rLongDictionary[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            } else {
                long delta = value - rLongDictionary[source];
                PrimitiveWriter.writeLongSigned((1+(delta + (delta >> 63))), writer);
                rLongDictionary[target] = value;
            }
        }
    }

    //TODO: B, can optimize with isLongEqual
    protected void genWriteLongSignedConstantOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos) {
            PrimitiveWriter.writePMapBit(RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos) == valueOfNull ? (byte)0 : (byte)1, writer);
    }
    

    protected void genWriteLongSignedCopyOptional(int target, int source, long valueOfNull, int fieldPos, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            
            
            //for copy and inc
            if (value == valueOfNull) {
                if (0 == rLongDictionary[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rLongDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
            long value1 = value-((value>>63)-1);
    
            if (value1 == rLongDictionary[source]) {
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(rLongDictionary[target] = value1, writer);
            }
        }
    }

    protected void genWriteLongSignedIncrementOptional(int target, int source, int fieldPos, long valueOfNull, PrimitiveWriter writer, long[] rLongDictionary, int[] rbB, int rbMask, PaddedLong rbPos) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            
            if (value == valueOfNull) {
                if (0 == rLongDictionary[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rLongDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
            
            value-=((value>>63)-1);
            long last = rLongDictionary[source];
            if (0 != last && value == (1 + last)) {// not null and matches
                PrimitiveWriter.writePMapBit((byte)0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
            rLongDictionary[target] = value;
        }
    }

    protected void genWriteLongSignedDefaultOptional(int target, int fieldPos, long valueOfNull, long constDefault, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, long[] rLongDictionary) {
        {
            long value = RingBuffer.readLong(rbB, rbMask, rbPos.value + fieldPos);
            
            if (value == valueOfNull) {
                if (0 == rLongDictionary[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    rLongDictionary[target] = 0;
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            } else {
                long value1 = (1+(value + (value >>> 63)));
                if (value1 == constDefault) {
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeLongSigned(value1, writer);
                }
            }
        }
    }

    protected void genWriteDictionaryBytesReset(int target, LocalHeap byteHeap) {
        LocalHeap.setNull(target, byteHeap);
    }

    protected void genWriteDictionaryTextReset(int target, LocalHeap byteHeap) {
        LocalHeap.reset(target, byteHeap);
    }

    protected void genWriteDictionaryLongReset(int target, long constValue, long[] rLongDictionary) {
        rLongDictionary[target] = constValue;
    }

    protected void genWriteDictionaryIntegerReset(int target, int constValue, int[] rIntDictionary) {
        rIntDictionary[target] = constValue;
    }
    
    protected void genWriteClosePMap(PrimitiveWriter writer) {
        PrimitiveWriter.closePMap(writer);
    }

    protected void genWriteCloseTemplatePMap(PrimitiveWriter writer, FASTEncoder dispatch) {
        PrimitiveWriter.closePMap(writer);
        // must always pop because open will always push
        dispatch.templateStackHead--;
    }

    protected void genWriteCloseTemplate(PrimitiveWriter writer, FASTEncoder dispatch) {
        // must always pop because open will always push
        dispatch.templateStackHead--;
    }    
    
    protected void genWriteOpenTemplatePMap(int pmapSize, int fieldPos, int msgIdx, PrimitiveWriter writer, int[] rbB, int rbMask, PaddedLong rbPos, FASTEncoder dispatch) {
        PrimitiveWriter.openPMap(pmapSize, writer);  //FASTRingBuffer queue, int fieldPos
        // done here for safety to ensure it is always done at group open.
        //TODO: A, finish development of repeated dynamic templates

        //int top = dispatch.templateStack[dispatch.templateStackHead];
        //if (top == msgIdx) {
        //    PrimitiveWriter.writePMapBit((byte)0, writer);
        //} else {
            PrimitiveWriter.writePMapBit((byte)1, writer);       
            //System.err.println("encoded template id of:"+dispatch.fieldIdScript[msgIdx]+" for "+msgIdx);
            PrimitiveWriter.writeLongUnsigned(dispatch.fieldIdScript[msgIdx], writer);
        //    top = templateId;
        //}
        //
        //dispatch.templateStack[dispatch.templateStackHead++] = top;
     }
    
    protected void genWriteOpenGroup(int pmapSize, PrimitiveWriter writer) {
        PrimitiveWriter.openPMap(pmapSize, writer);
    }
}
