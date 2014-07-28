package com.ociweb.jfast.generator;

import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.stream.RingBuffers;
import com.ociweb.jfast.util.Stats;


public abstract class FASTWriterDispatchTemplates extends FASTEncoder {

    public FASTWriterDispatchTemplates(final TemplateCatalogConfig catalog) {
        super(catalog);
    }    
    
    public FASTWriterDispatchTemplates(final TemplateCatalogConfig catalog, RingBuffers ringBuffers) {
        super(catalog,ringBuffers);
    }    

    protected void genWriteCopyBytes(int source, int target, LocalHeap byteHeap) {
        LocalHeap.copy(source,target,byteHeap);
    }
    

    protected void genWritePreamble(byte[] preambleData, PrimitiveWriter writer, FASTRingBuffer ringBuffer) { //TODO: A, change from ringBuffer into array details.

        
        PrimitiveWriter.writeByteArrayData(preambleData, 0, preambleData.length, writer);
    }
    

    protected void genWriteTextDefaultOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            int offset = FASTRingBuffer.readRingBytePosition(rawPos);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;
            
            if (length<0) {
                if (LocalHeap.isNull(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK,byteHeap)) {
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            } else {
                if (LocalHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK,buffer,offset,length,byteMask,byteHeap)) {
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeTextASCII(buffer, offset, length, byteMask, writer);
                }
            }
        }
    }

    protected void genWriteTextCopyOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);        
            if (length<0) {
                if (LocalHeap.isNull(target,byteHeap)) {
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            } else {
                int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
                int byteMask = rbRingBuffer.byteMask;
                int offset = FASTRingBuffer.readRingBytePosition(rawPos);
                // constant from heap or dynamic from char ringBuffer
                byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
                if (LocalHeap.equals(target,buffer,offset,length, byteHeap)) {
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeTextASCII(/*value*/ buffer, offset, length, byteMask, writer);
                    LocalHeap.set(target,buffer,offset,length,byteMask,byteHeap);
                }
            }
        }
    }

    protected void genWriteTextDeltaOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);        
            if (length<0) {
                PrimitiveWriter.writeIntegerSigned(0, writer);
            } else {
                int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
                int offset = FASTRingBuffer.readRingBytePosition(rawPos);
                // constant from heap or dynamic from char ringBuffer
                byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
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

    protected void genWriteTextTailOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            int offset = FASTRingBuffer.readRingBytePosition(rawPos);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
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
    
    protected void genWriteTextDefault(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            int offset = FASTRingBuffer.readRingBytePosition(rawPos);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;
            
            if (LocalHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK,buffer,offset,length,byteMask,byteHeap)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeTextASCII(buffer, offset, length, byteMask, writer);
            }
        }
    }

    protected void genWriteTextCopy(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            int offset = FASTRingBuffer.readRingBytePosition(rawPos);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;
                           
            
            if (LocalHeap.equals(target,buffer,offset,length,byteMask,byteHeap)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                // System.err.println("char seq length:"+value.length());
                PrimitiveWriter.writeTextASCII(/*value*/ buffer, offset, length, byteMask, writer);
                LocalHeap.set(target,buffer,offset,length,byteMask,byteHeap);
            }
        }
    }

    protected void genWriteTextDelta(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            int offset = FASTRingBuffer.readRingBytePosition(rawPos);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;
            
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
                PrimitiveWriter.writeTextASCIIBefore(buffer,offset,byteMask, sentLen, writer);
                LocalHeap.appendHead(target,trimHead,buffer,offset,sentLen,byteMask,byteHeap);
            }
        }
    }
    
    protected void genWriteTextTail(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            int offset = FASTRingBuffer.readRingBytePosition(rawPos);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
            int byteMask = rbRingBuffer.byteMask;
            
            int headCount = LocalHeap.countHeadMatch(target,buffer,offset,length,byteMask,byteHeap);
            int trimTail = LocalHeap.length(target,byteHeap) - headCount;
            PrimitiveWriter.writeIntegerUnsigned(trimTail, writer);
            PrimitiveWriter.writeTextASCIIAfter(headCount, buffer, offset, length ,byteMask, writer);
            LocalHeap.appendTail(target,trimTail,buffer,offset,headCount,byteMask,byteHeap);
        }
    }

    protected void genWriteTextNone(int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        {
            int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            int offset = FASTRingBuffer.readRingBytePosition(rawPos);
            // constant from heap or dynamic from char ringBuffer
            byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
            PrimitiveWriter.writeTextASCII(buffer, offset, length, writer);
        }
    }
    
    protected void genWriteTextNoneOptional(int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        {
            int length = FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos);
            if (length<0) {
                PrimitiveWriter.writeNull(writer);
            } else{        
                int rawPos = FASTRingBuffer.readRingByteRawPos(fieldPos,rbRingBuffer);
                int offset = FASTRingBuffer.readRingBytePosition(rawPos);
                // constant from heap or dynamic from char ringBuffer
                byte[] buffer = FASTRingBuffer.readRingByteBuffers(rawPos, rbRingBuffer);
                PrimitiveWriter.writeTextASCII(buffer, offset, length, writer);
            }
        }
    }
        
    protected void genWriteTextConstantOptional(int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit((byte)(1&(1+(FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos)>>>31))), writer);//0 for null, 1 for present
    }
   
    // if (byteHeap.equals(target|INIT_VALUE_MASK, value, offset, length)) {
    protected void genWriteBytesDefault(int target, int fieldPos, LocalHeap byteHeap, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
         
        if (LocalHeap.equals(target,rbRingBuffer.byteBuffer,FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer),rbRingBuffer.byteMask,byteHeap)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            int len = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(len, writer);            
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),len, rbRingBuffer.byteMask, writer);
        }
    }

    protected void genWriteBytesCopy(int target, int fieldPos, LocalHeap byteHeap, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {

        if (LocalHeap.equals(target,rbRingBuffer.byteBuffer,FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer),rbRingBuffer.byteMask,byteHeap)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            int len = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            
            int offset = FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer));
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, offset, len, rbRingBuffer.byteMask, writer);
            LocalHeap.set(target,rbRingBuffer.byteBuffer,offset,len,rbRingBuffer.byteMask,byteHeap);
        }
    }

    public void genWriteBytesDelta(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int length = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            int offset = FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer));
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

    public void genWriteBytesTail(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int length = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            int offset = FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer));
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

    protected void genWriteBytesNone(int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        {
            int len = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),len, rbRingBuffer.byteMask, writer);
        }
    }
    
    public void genWriteBytesDefaultOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
       
        if (LocalHeap.equals(target,rbRingBuffer.byteBuffer,FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer),rbRingBuffer.byteMask,byteHeap)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            int len = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(len+1, writer);
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),len, rbRingBuffer.byteMask, writer);
        }
    }

    public void genWriteBytesCopyOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        
        if (LocalHeap.equals(target,rbRingBuffer.byteBuffer,FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer),rbRingBuffer.byteMask,byteHeap)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            int len = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(len+1, writer);
            
            int offset = FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer));
            PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, offset, len, rbRingBuffer.byteMask, writer);
            LocalHeap.set(target,rbRingBuffer.byteBuffer,offset,len,rbRingBuffer.byteMask,byteHeap);
        }
    }

    public void genWriteBytesDeltaOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int length = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            
            if (length<0) {            
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(target, byteHeap); // no pmap, yes change to last value
            } else {
            
                int offset = FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer));
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

    protected void genWriteBytesConstantOptional(int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit((byte)(1&(1+(FASTRingBufferReader.readDataLength(rbRingBuffer, fieldPos)>>>31))), writer);//0 for null, 1 for present
    }

    public void genWriteBytesTailOptional(int target, int fieldPos, PrimitiveWriter writer, LocalHeap byteHeap, FASTRingBuffer rbRingBuffer) {
        {
            int length = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            if (length<0) {
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(target, byteHeap); // no pmap, yes change to last value            
            } else {
            
                int offset = FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer));
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

    protected void genWriteBytesNoneOptional(int target, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, LocalHeap byteHeap) {
        {
            int len = FASTRingBuffer.readRingByteLen(fieldPos,rbRingBuffer);
            if (len<0) {
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(target, byteHeap); // no pmap, yes change to last value
            } else {
                PrimitiveWriter.writeIntegerUnsigned(len+1, writer);
                PrimitiveWriter.writeByteArrayData(rbRingBuffer.byteBuffer, FASTRingBuffer.readRingBytePosition(FASTRingBuffer.readRingByteRawPos(fieldPos, rbRingBuffer)),len, rbRingBuffer.byteMask, writer);
            }
        }
    }
    
    protected void genWriteIntegerSignedDefault(int constDefault, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSignedDefault(FASTRingBufferReader.readInt(rbRingBuffer, fieldPos), constDefault, writer);
    }

    protected void genWriteIntegerSignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            int incVal;
            if (value == (incVal = intValues[source] + 1)) {
                intValues[target] = incVal;
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                intValues[target] = value;
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeIntegerSigned(value, writer); //same as unsigned increment except for this one line
            }
        }
    }

    protected void genWriteIntegerSignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSignedCopy(FASTRingBufferReader.readInt(rbRingBuffer, fieldPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerSignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            int last = intValues[source];
            if (value > 0 == last > 0) { // optimization using int when possible instead of long
                PrimitiveWriter.writeIntegerSigned(value - last, writer);
                intValues[target] = value;
            } else {
                PrimitiveWriter.writeLongSigned(value - (long) last, writer);
                intValues[target] = value;
            }
        }
    }

    protected void genWriteIntegerSignedNone(int target, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSigned(intValues[target] = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos), writer);
    }
    
    protected void genWriteIntegerUnsignedDefault(int constDefault, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsignedDefault(FASTRingBufferReader.readInt(rbRingBuffer, fieldPos), constDefault, writer);
    }

    protected void genWriteIntegerUnsignedIncrement( int target, int source, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            int incVal;
            if (value == (incVal = intValues[source] + 1)) {
                intValues[target] = incVal;
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                intValues[target] = value;
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeIntegerUnsigned(value, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsignedCopy(FASTRingBufferReader.readInt(rbRingBuffer, fieldPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerUnsignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            PrimitiveWriter.writeLongSigned(value - (long) intValues[source], writer);
            intValues[target] = value;
        }
    }

    protected void genWriteIntegerUnsignedNone(int target, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsigned(intValues[target] = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos), writer);
    }

    protected void genWriteIntegerSignedDefaultOptional(int source, int fieldPos, int constDefault, int valueOfNull, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, int[] intValues) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullDefaultInt(writer, intValues, source); // null for default 
            } else {
                PrimitiveWriter.writeIntegerSignedDefault(value>=0?value+1:value,constDefault,writer);
            }
        }
    }

    protected void genWriteIntegerSignedIncrementOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                int last = intValues[source];
                int value1 = intValues[target] = (1+(value + (value >>> 31)));
                if (0 != last && value1 == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeIntegerSigned(value1, writer);
                }  
            }
        }
    }

    protected void genWriteIntegerSignedCopyOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            //TODO: C, these reader calls should all be inlined to remove the object de-ref by passing in the mask and buffer directly as was done in the reader.
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else {        
                PrimitiveWriter.writeIntegerSignedCopy((1+(value + (value >>> 31))),target,source,intValues,writer);
            }
        }   
    }

    //this is how a "boolean" is sent using a single bit in the encoding.
    protected void genWriteIntegerSignedConstantOptional(int valueOfNull, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit(valueOfNull==FASTRingBufferReader.readInt(rbRingBuffer, fieldPos) ? (byte)0 : (byte)1, writer);  // 1 for const, 0 for absent
    }

    protected void genWriteIntegerSignedDeltaOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                int last = intValues[source];
                if (value > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = value - last;
                    intValues[target] = value;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = value - (long) last;
                    intValues[target] = value;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
            }
        }
    }

    protected void genWriteIntegerSignedNoneOptional(int target, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(value, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedCopyOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                PrimitiveWriter.writeIntegerUnsignedCopy(1+value,target,source,intValues,writer);
            }
        }
    }

    
    /*
     * example  - valueOfNull no longer needed we know its zero, no need to add one already done.
     * 
            int value = FASTRingBufferReader.readInt(rbRingBuffer,fieldPos);
            if (0 == value) {
                StaticGlue.nullDefaultInt(writer, intValues, source); // null for default 
            } else {
                PrimitiveWriter.writeIntegerUnsignedDefault(value,constDefault,writer);
            }
            
     */
    
    
    protected void genWriteIntegerUnsignedDefaultOptional(int source, int fieldPos, int valueOfNull, int constDefault, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, int[] intValues) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullDefaultInt(writer, intValues, source); // null for default 
            } else {
                PrimitiveWriter.writeIntegerUnsignedDefault(1+value,constDefault,writer);
            }
        }
    }

    
    
    protected void genWriteIntegerUnsignedIncrementOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int value = FASTRingBufferReader.readInt(rbRingBuffer,fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                if (0 != intValues[source] && value == (intValues[target] = intValues[source] + 1)) {
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    int tmp = intValues[target] = 1 + value;
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeIntegerUnsigned(tmp, writer);
                }
            }
        }
    }

    protected void genWriteIntegerUnsignedConstantOptional(int fieldPos, int valueOfNull, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit(valueOfNull==FASTRingBufferReader.readInt(rbRingBuffer,fieldPos) ? (byte)0 : (byte)1, writer);  // 1 for const, 0 for absent
    }

    
    protected void genWriteIntegerUnsignedDeltaOptional(int target, int source, int fieldPos, int valueOfNull, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                long delta = value - (long) intValues[source];//unable to leave as is for client
                PrimitiveWriter.writeLongSigned( (1+(delta + (delta >>> 63))), writer);
                intValues[target] = value;
            }
        }
    }

    protected void genWriteIntegerUnsignedNoneOptional(int target, int valueOfNull, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, int[] intValues) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,fieldPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerUnsigned(value + 1, writer);
            }
        }
    }
    
    ////////////////////////
    ///Decimals with optional exponent
    /////////////////////////

    //None
    
      protected void genWriteDecimalDefaultOptionalNone(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, int[] intValues, FASTEncoder dispatch) {
      {
        int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
        if (exponentValueOfNull == exponentValue) {
            StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
        } else {
            PrimitiveWriter.writeIntegerSignedDefault(exponentValue>=0?exponentValue+1:exponentValue,exponentConstDefault,writer);
            assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
            dispatch.activeScriptCursor++;
            PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), writer); 
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
        {
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else { 
                int last = intValues[exponentSource];
                int value = intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue);
                if (0 != last && value == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeIntegerSigned(value, writer);
                } 
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), writer); 
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else {        
                PrimitiveWriter.writeIntegerSignedCopy(exponentValue>=0?exponentValue+1:exponentValue,exponentTarget,exponentSource,intValues,writer);
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), writer); 
            }
        }
    }

    protected void genWriteDecimalConstantOptionalNone(int exponentValueOfNull, int mantissaTarget, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
        { 
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull==exponentValue) {
                PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), writer);        
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalNone(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                int last = intValues[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), writer); 
            }
        }
    }

    protected void genWriteDecimalNoneOptionalNone(int exponentTarget, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), writer); 
            }
        }
    }
    
    // DEFAULTS
   
   
     protected void genWriteDecimalDefaultOptionalDefault(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, int[] intValues, FASTEncoder dispatch) {
      {
        int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
        if (exponentValueOfNull == exponentValue) {
            StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
        } else {
            PrimitiveWriter.writeIntegerSignedDefault(exponentValue>=0?exponentValue+1:exponentValue,exponentConstDefault,writer);
            assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
            dispatch.activeScriptCursor++;
            //mantissa
            PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), mantissaConstDefault, writer);
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else { 
                int last = intValues[exponentSource];
                int value = intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue);
                if (0 != last && value == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeIntegerSigned(value, writer);
                } 
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), mantissaConstDefault, writer);
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else {        
                PrimitiveWriter.writeIntegerSignedCopy((1+(exponentValue + (exponentValue >>> 31))),exponentTarget,exponentSource,intValues,writer);
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), mantissaConstDefault, writer);
            }
        }
    }

    protected void genWriteDecimalConstantOptionalDefault(int exponentValueOfNull, int mantissaTarget, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
        { 
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull==exponentValue) {
                PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), mantissaConstDefault, writer);
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalDefault(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                int last = intValues[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), mantissaConstDefault, writer);
            }
        }
    }

    protected void genWriteDecimalNoneOptionalDefault(int exponentTarget, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                dispatch.activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos), mantissaConstDefault, writer);
            }
        }
    }
    
    
    // Increment
    
    
    protected void genWriteDecimalDefaultOptionalIncrement(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
     {
       int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
       if (exponentValueOfNull == exponentValue) {
           StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
       } else {
           PrimitiveWriter.writeIntegerSignedDefault(exponentValue>=0?exponentValue+1:exponentValue,exponentConstDefault,writer);
           assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
           dispatch.activeScriptCursor++;
           //mantissa
           long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
           if (value == (1 + longValues[mantissaSource])) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeLongSigned(value, writer);
        }
           longValues[mantissaTarget] = value;
       }
     }
   }

   protected void genWriteDecimalIncrementOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
       {
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
           } else { 
               int last = intValues[exponentSource];
            int value1 = intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue);
               if (0 != last && value1 == 1 + last) {// not null and matches
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeIntegerSigned(value1, writer);
            } 
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               dispatch.activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
               if (value == (1 + longValues[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               longValues[mantissaTarget] = value;
           }   
       }
   }

   protected void genWriteDecimalCopyOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
       {   
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
           } else {        
               PrimitiveWriter.writeIntegerSignedCopy(exponentValue>=0?exponentValue+1:exponentValue,exponentTarget,exponentSource,intValues,writer);
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               dispatch.activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
               if (value == (1 + longValues[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               longValues[mantissaTarget] = value;
           }
       }
   }

   protected void genWriteDecimalConstantOptionalIncrement(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
       { 
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
           if (exponentValueOfNull==exponentValue) {
               PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
           } else {
               PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               dispatch.activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
               if (value == (1 + longValues[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               longValues[mantissaTarget] = value;
           }     
       }
   }

   protected void genWriteDecimalDeltaOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
       {   
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
           } else {
               int last = intValues[exponentSource];
            if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                int dif = exponentValue - last;
                intValues[exponentTarget] = exponentValue;
                PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
            } else {
                long dif = exponentValue - (long) last;
                intValues[exponentTarget] = exponentValue;
                PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
            }
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               dispatch.activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
               if (value == (1 + longValues[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               longValues[mantissaTarget] = value;
           }
       }
   }

   protected void genWriteDecimalNoneOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
       {   
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
           } else {
               PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               dispatch.activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
               if (value == (1 + longValues[mantissaSource])) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
               longValues[mantissaTarget] = value;
           }
       }
   }
    
   //copy
   
   protected void genWriteDecimalDefaultOptionalCopy(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
       {
         int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
         if (exponentValueOfNull == exponentValue) {
             StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
         } else {
             PrimitiveWriter.writeIntegerSignedDefault(exponentValue>=0?exponentValue+1:exponentValue,exponentConstDefault,writer);
             assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
             dispatch.activeScriptCursor++;
             //mantissa
             long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);   
             PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
         }
       }
     }

     protected void genWriteDecimalIncrementOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
         {
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
             } else { 
                 int last = intValues[exponentSource];
                int value1 = intValues[exponentTarget] = (1+(exponentValue + (exponentValue >>> 31)));
                 if (0 != last && value1 == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeIntegerSigned(value1, writer);
                } 
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                 dispatch.activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }   
         }
     }

     protected void genWriteDecimalCopyOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
         {   
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
             } else {        
                 PrimitiveWriter.writeIntegerSignedCopy((1+(exponentValue + (exponentValue >>> 31))),exponentTarget,exponentSource,intValues,writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                 dispatch.activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }
         }
     }

     protected void genWriteDecimalConstantOptionalCopy(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
         { 
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
             if (exponentValueOfNull==exponentValue) {
                 PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
             } else {
                 PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                 dispatch.activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }     
         }
     }

     protected void genWriteDecimalDeltaOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
         {   
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
             } else {
                 int last = intValues[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                 dispatch.activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }
         }
     }

     protected void genWriteDecimalNoneOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
         {   
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
             } else {
                 PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                 dispatch.activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }
         }
     } 
   
   //constant
  
     protected void genWriteDecimalDefaultOptionalConstant(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, int[] intValues, FASTEncoder dispatch) {
         {
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
           } else {
               PrimitiveWriter.writeIntegerSignedDefault((1+(exponentValue + (exponentValue >>> 31))),exponentConstDefault,writer);
               assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
               dispatch.activeScriptCursor++;
               //mantissa
               //is constant so do nothing
           }
         }
       }

       protected void genWriteDecimalIncrementOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
               } else { 
                   int last = intValues[exponentSource];
                int value = intValues[exponentTarget] = (1+(exponentValue + (exponentValue >>> 31)));
                   if (0 != last && value == 1 + last) {// not null and matches
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeIntegerSigned(value, writer);
                } 
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                   dispatch.activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }   
           }
       }

       protected void genWriteDecimalCopyOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {   
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
               } else {        
                   PrimitiveWriter.writeIntegerSignedCopy((1+(exponentValue + (exponentValue >>> 31))),exponentTarget,exponentSource,intValues,writer);
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                   dispatch.activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }
           }
       }

       protected void genWriteDecimalConstantOptionalConstant(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
           { 
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
               if (exponentValueOfNull==exponentValue) {
                   PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
               } else {
                   PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                   dispatch.activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }     
           }
       }

       protected void genWriteDecimalDeltaOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {   
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
               } else {
                   int last = intValues[exponentSource];
                if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = exponentValue - last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                } else {
                    long dif = exponentValue - (long) last;
                    intValues[exponentTarget] = exponentValue;
                    PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                }
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                   dispatch.activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }
           }
       }

       protected void genWriteDecimalNoneOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, FASTEncoder dispatch) {
           {   
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
               } else {
                   PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                   assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                   dispatch.activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }
           }
       }      
     
  //delta
       
       protected void genWriteDecimalDefaultOptionalDelta(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, int[] intValues, FASTEncoder dispatch) {
           {
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
             } else {
                 PrimitiveWriter.writeIntegerSignedDefault((1+(exponentValue + (exponentValue >>> 31))),exponentConstDefault,writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                 dispatch.activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos); 
                 PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                 longValues[mantissaTarget] = value;
             }
           }
         }

         protected void genWriteDecimalIncrementOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
             {
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos);  
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
                 } else { 
                     int last = intValues[exponentSource];
                    int value1 = intValues[exponentTarget] = (1+(exponentValue + (exponentValue >>> 31)));
                     if (0 != last && value1 == 1 + last) {// not null and matches
                        PrimitiveWriter.writePMapBit((byte) 0, writer);
                    } else {
                        PrimitiveWriter.writePMapBit((byte) 1, writer);
                        PrimitiveWriter.writeIntegerSigned(value1, writer);
                    } 
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                     dispatch.activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }   
             }
         }

         protected void genWriteDecimalCopyOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValue, FASTEncoder dispatch) {
             {   
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
                 } else {        
                     PrimitiveWriter.writeIntegerSignedCopy((1+(exponentValue + (exponentValue >>> 31))),exponentTarget,exponentSource,intValues,writer);
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                     dispatch.activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValue[mantissaTarget] = value;
                 }
             }
         }

         protected void genWriteDecimalConstantOptionalDelta(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValue, FASTEncoder dispatch) {
             { 
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
                 if (exponentValueOfNull==exponentValue) {
                     PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
                 } else {
                     PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                     dispatch.activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValue[mantissaTarget] = value;
                 }     
             }
         }

         protected void genWriteDecimalDeltaOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
             {   
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
                 } else {
                     int last = intValues[exponentSource];
                    if (exponentValue > 0 == last > 0) { // optimization using int when possible instead of long
                        int dif = exponentValue - last;
                        intValues[exponentTarget] = exponentValue;
                        PrimitiveWriter.writeIntegerSigned((1+(dif + (dif >>> 31))), writer);
                    } else {
                        long dif = exponentValue - (long) last;
                        intValues[exponentTarget] = exponentValue;
                        PrimitiveWriter.writeLongSigned((1+(dif + (dif >>> 63))), writer);
                    }
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                     dispatch.activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }
             }
         }

         protected void genWriteDecimalNoneOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int fieldPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues, FASTEncoder dispatch) {
             {  
                 //FASTEncoder dispatch
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, fieldPos); 
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
                 } else {
                     PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                     assert(FASTEncoder.notifyFieldPositions(writer, dispatch.activeScriptCursor));
                     dispatch.activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, fieldPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }
             }
         } 
          
    
    //////////////
    //end of decimals
    ///////////////
    
    protected void genWriteLongUnsignedDefault(long constDefault, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {        
        PrimitiveWriter.writeLongUnsignedDefault(FASTRingBufferReader.readLong(rbRingBuffer, fieldPos), constDefault, writer);
    }

    protected void genWriteLongUnsignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            long incVal = longValues[source] + 1;
            if (value == incVal) {
                longValues[target] = incVal;
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                longValues[target] = value;
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongUnsigned(value, writer);
            }
        }
    }

    protected void genWriteLongUnsignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongUnsignedCopy(FASTRingBufferReader.readLong(rbRingBuffer, fieldPos), target, source, longValues, writer);
    }

    protected void genWriteLongUnsignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            PrimitiveWriter.writeLongSigned(value - longValues[source], writer);
            longValues[target] = value;
        }
    }

    protected void genWriteLongUnsignedNone(int target, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongUnsigned(longValues[target] = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos), writer);
    }
    
    protected void genWriteLongUnsignedDefaultOptional(long valueOfNull, int target, long constDefault, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            if (value == valueOfNull) {
                if (longValues[target] == 0) { // stored value was null; //for default
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
              }
            // room for zero
            PrimitiveWriter.writeLongUnsignedDefault(1+value,constDefault,writer);
        }
    }

    protected void genWriteLongUnsignedIncrementOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            //for copy and inc
            if (value == valueOfNull) {
                if (0 == longValues[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    longValues[target] = 0;
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
            if (0 != longValues[source] && value == (longValues[target] = longValues[source] + 1)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongUnsigned(longValues[target] = 1 + value, writer);
            }
        }
    }

    protected void genWriteLongUnsignedCopyOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            //for copy and inc
            if (value == valueOfNull) {
                if (0 == longValues[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    longValues[target] = 0;
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
            value++;// zero is held for null
            
            PrimitiveWriter.writeLongUnsignedCopy(value,target,source,longValues,writer);
        }
    }

    protected void genWriteLongUnsignedConstantOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            if (value == valueOfNull) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);  // null for const optional
            }
            PrimitiveWriter.writePMapBit((byte) 1, writer);
        }
    }


    protected void genWriteLongUnsignedNoneOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            if (value == valueOfNull) {
                longValues[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            }
            PrimitiveWriter.writeLongUnsigned(value + 1, writer);
        }
    }

    protected void genWriteLongUnsignedDeltaOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            if (value == valueOfNull) {
                longValues[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            }
            long delta = value - longValues[source];
            PrimitiveWriter.writeLongSigned((1+(delta + (delta >>> 63))), writer);
            longValues[target] = value;
        }
    }
    
    protected void genWriteLongSignedDefault(long constDefault, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readLong(rbRingBuffer, fieldPos), constDefault, writer);
    }

    protected void genWriteLongSignedIncrement(int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            if (value == (1 + longValues[source])) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
            longValues[target] = value;
        }
    }

    protected void genWriteLongSignedCopy(int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongSignedCopy(FASTRingBufferReader.readLong(rbRingBuffer, fieldPos), target, source, longValues, writer);
    }

    protected void genWriteLongSignedNone(int target, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongSigned(longValues[target] = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos), writer);
    }

    protected void genWriteLongSignedDelta(int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            PrimitiveWriter.writeLongSigned(value - longValues[source], writer);
            longValues[target] = value;
        }
    }
    
    protected void genWriteLongSignedOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            if (value == valueOfNull) {
                longValues[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            }
            PrimitiveWriter.writeLongSignedOptional(value, writer);
        }
    }

    protected void genWriteLongSignedDeltaOptional(long valueOfNull, int target, int source, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            if (value == valueOfNull) {
                longValues[target] = 0; //for none and delta
                PrimitiveWriter.writeNull(writer);
            }
            long delta = value - longValues[source];
            PrimitiveWriter.writeLongSigned((1+(delta + (delta >>> 63))), writer);
            longValues[target] = value;
        }
    }

    protected void genWriteLongSignedConstantOptional(long valueOfNull, int target, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            
            if (value == valueOfNull) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);  // null for const optional
            }
            
            PrimitiveWriter.writePMapBit((byte) 1, writer);
        }
    }
    

    protected void genWriteLongSignedCopyOptional(int target, int source, long valueOfNull, int fieldPos, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            
            
            //for copy and inc
            if (value == valueOfNull) {
                if (0 == longValues[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    longValues[target] = 0;
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
    
            PrimitiveWriter.writeLongSignedCopy(value-((value>>63)-1), target, source, longValues, writer);
        }
    }

    protected void genWriteLongSignedIncrementOptional(int target, int source, int fieldPos, long valueOfNull, PrimitiveWriter writer, long[] longValues, FASTRingBuffer rbRingBuffer) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            
            if (value == valueOfNull) {
                if (0 == longValues[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    longValues[target] = 0;
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            }
            
            value-=((value>>63)-1);
            long last = longValues[source];
            if (0 != last && value == (1 + last)) {// not null and matches
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeLongSigned(value, writer);
            }
            longValues[target] = value;
        }
    }

    protected void genWriteLongSignedDefaultOptional(int target, int fieldPos, long valueOfNull, long constDefault, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {
            long value = FASTRingBufferReader.readLong(rbRingBuffer, fieldPos);
            
            if (value == valueOfNull) {
                if (0 == longValues[target]) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    longValues[target] = 0;
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeNull(writer);
                }
            } else {
                PrimitiveWriter.writeLongSignedDefault( (1+(value + (value >>> 63))) , constDefault, writer);
            }
        }
    }

    protected void genWriteDictionaryBytesReset(int target, LocalHeap byteHeap) {
        LocalHeap.setNull(target, byteHeap);
    }

    protected void genWriteDictionaryTextReset(int target, LocalHeap byteHeap) {
        LocalHeap.reset(target, byteHeap);
    }

    protected void genWriteDictionaryLongReset(int target, long constValue, long[] longValues) {
        longValues[target] = constValue;
    }

    protected void genWriteDictionaryIntegerReset(int target, int constValue, int[] intValues) {
        intValues[target] = constValue;
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
    
    

    protected void genWriteOpenTemplatePMap(int pmapSize, int fieldPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.openPMap(pmapSize, writer);  //FASTRingBuffer queue, int fieldPos
        // done here for safety to ensure it is always done at group open.
        pushTemplate(fieldPos, writer, rbRingBuffer);
     }
    
    protected void genWriteOpenGroup(int pmapSize, PrimitiveWriter writer) {
        PrimitiveWriter.openPMap(pmapSize, writer);
    }
    
    public void genWriteNullPMap(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 0, writer);  // null for const optional
    }

    public void genWriteNullNoPMapLong(int target, PrimitiveWriter writer, long[] dictionary) {
        dictionary[target] = 0;
        PrimitiveWriter.writeNull(writer);
    }
    
    public void genWriteNullDefaultText(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        if (LocalHeap.isNull(target,byteHeap)) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }

    public void genWriteNullCopyIncText(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        if (LocalHeap.isNull(target,byteHeap)) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
            LocalHeap.setNull(target, byteHeap);
        }
    }

    public void genWriteNullNoPMapText(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        PrimitiveWriter.writeNull(writer);
        LocalHeap.setNull(target, byteHeap);
    }
    
    public void genWriteNullDefaultBytes(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        if (LocalHeap.isNull(target,byteHeap)) { //stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }

    public void genWriteNullNoPMapBytes(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        PrimitiveWriter.writeNull(writer);
        LocalHeap.setNull(target, byteHeap);
    }

    public void genWriteNullCopyIncBytes(int target, PrimitiveWriter writer, LocalHeap byteHeap) {
        if (LocalHeap.isNull(target,byteHeap)) { //stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeNull(writer);
            LocalHeap.setNull(target, byteHeap);
        }
    }
}
