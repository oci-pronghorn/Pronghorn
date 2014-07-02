package com.ociweb.jfast.generator;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.stream.FASTEncoder;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTRingBufferReader;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.stream.RingBuffers;


public abstract class FASTWriterDispatchTemplates extends FASTEncoder {

    public FASTWriterDispatchTemplates(final TemplateCatalogConfig catalog) {
        super(catalog,catalog.ringBuffers());
    }    
    
    public FASTWriterDispatchTemplates(final TemplateCatalogConfig catalog, RingBuffers ringBuffers) {
        super(catalog,ringBuffers);
    }    
   
    protected void genWriteCopyText(int source, int target, TextHeap textHeap) {
        textHeap.copy(source,target);
    }

    protected void genWriteCopyBytes(int source, int target, ByteHeap byteHeap) {
        byteHeap.copy(source,target);
    }
    

    protected void genWritePreamble(byte[] preambleData, PrimitiveWriter writer) {
        PrimitiveWriter.writeByteArrayData(preambleData, 0, preambleData.length, writer);
    }
    
    protected void genWriteUTFTextDefaultOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeNull(writer);
            }
        } else {
            if (textHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeIntegerUnsigned(value.length() + 1, writer);
                PrimitiveWriter.writeTextUTF(value, writer);
            }
        }
    }

    protected void genWriteUTFTextCopyOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(value.length() + 1, writer);
            PrimitiveWriter.writeTextUTF(value, writer);
            textHeap.set(target, value, 0, value.length());
        }
    }

    protected void genWriteUTFTextDeltaOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(target, value);
        int tailCount = textHeap.countTailMatch(target, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(target) - headCount; //+1 for optional
            PrimitiveWriter.writeIntegerSigned(trimTail >= 0 ? trimTail + 1 : trimTail, writer);
            int length = (value.length() - headCount);
            PrimitiveWriter.writeIntegerUnsigned(length, writer);
            PrimitiveWriter.writeTextUTFAfter(headCount, value, writer);
            textHeap.appendTail(target, trimTail, headCount, value);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(target) - tailCount;
            PrimitiveWriter.writeIntegerSigned(0 == trimHead ? 1 : -trimHead, writer);
            int valueSend = value.length() - tailCount;
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            PrimitiveWriter.writeTextUTFBefore(value, valueSend, writer);
            textHeap.appendHead(target, trimHead, value, valueSend);
        }
    }
    
    protected void genWriteUTFTextConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteUTFTextTailOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(target, value);
        int trimTail = textHeap.length(target) - headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail + 1, writer);// plus 1 for optional
        int length = (value.length() - headCount);
        PrimitiveWriter.writeIntegerUnsigned(length, writer);
        PrimitiveWriter.writeTextUTFAfter(headCount, value, writer);
        textHeap.appendTail(target, trimTail, headCount, value);
    }

    protected void genWriteUTFTextNoneOptional(CharSequence value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(value.length() + 1, writer);
        PrimitiveWriter.writeTextUTF(value, writer);
    }
    
    protected void genWriteUTFTextDefault(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(value.length(), writer);
            PrimitiveWriter.writeTextUTF(value, writer);
        }
    }

    protected void genWriteUTFTextCopy(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(value.length(), writer);
            PrimitiveWriter.writeTextUTF(value, writer);
            textHeap.set(target, value, 0, value.length());
        }
    }

    protected void genWriteUTFTextDelta(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(target, value);
        int tailCount = textHeap.countTailMatch(target, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(target) - headCount;
            PrimitiveWriter.writeIntegerSigned(trimTail, writer);
            PrimitiveWriter.writeIntegerUnsigned(value.length() - headCount, writer);
            PrimitiveWriter.writeTextUTFAfter(headCount, value, writer);
            textHeap.appendTail(target, trimTail, headCount, value);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(target) - tailCount;
            PrimitiveWriter.writeIntegerSigned(0 == trimHead ? 0 : -trimHead, writer);
            int valueSend = value.length() - tailCount;
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            PrimitiveWriter.writeTextUTFBefore(value, valueSend, writer);
            textHeap.appendHead(target, trimHead, value, valueSend);
        }
    }

    protected void genWriteUTFTextTail(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(target, value);
        int trimTail = textHeap.length(target) - headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail, writer);
        int length = (value.length() - headCount);
        PrimitiveWriter.writeIntegerUnsigned(length, writer);
        PrimitiveWriter.writeTextUTFAfter(headCount, value, writer);
        textHeap.appendTail(target, trimTail, headCount, value);
    }

    protected void genWriteUTFTextNone(CharSequence value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(value.length(), writer);
        PrimitiveWriter.writeTextUTF(value, writer);
    }

    protected void genWriteTextDefaultOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeNull(writer);
            }
        } else {
            if (textHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeTextASCII(value, writer);
            }
        }
    }

    protected void genWriteTextCopyOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(target)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeNull(writer);
            }
        } else {
            if (textHeap.equals(target, value)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                PrimitiveWriter.writeTextASCII(value, writer);
                textHeap.set(target, value, 0, value.length());
            }
        }
    }

    protected void genWriteTextDeltaOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            PrimitiveWriter.writeIntegerSigned(0, writer);
        } else {
            // count matching front or back chars
            int headCount = textHeap.countHeadMatch(target, value);
            int tailCount = textHeap.countTailMatch(target, value);
            if (headCount > tailCount) {
                int trimTail = textHeap.length(target) - headCount;
                assert (trimTail >= 0);
                PrimitiveWriter.writeIntegerSigned(trimTail + 1, writer);// must add one because this
                                                        // is optional
                PrimitiveWriter.writeTextASCIIAfter(headCount, value, writer);
                textHeap.appendTail(target, trimTail, headCount, value);
            } else {
                int trimHead = textHeap.length(target) - tailCount;
                PrimitiveWriter.writeIntegerSigned(0 == trimHead ? 1 : -trimHead, writer);
                
                int sentLen = value.length() - tailCount;
                PrimitiveWriter.writeTextASCIIBefore(value, sentLen, writer);
                textHeap.appendHead(target, trimHead, value, sentLen);
            }
        }
    }

    protected void genWriteTextTailOptional(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(target, value);
        int trimTail = textHeap.length(target) - headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail + 1, writer);
        PrimitiveWriter.writeTextASCIIAfter(headCount, value, writer);
        textHeap.appendTail(target, trimTail, headCount, value);
    }

    protected void genWriteNull(PrimitiveWriter writer) {
        PrimitiveWriter.writeNull(writer);
    }
    
    protected void genWriteTextDefault(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeTextASCII(value, writer);
        }
    }

    protected void genWriteTextCopy(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            // System.err.println("char seq length:"+value.length());
            PrimitiveWriter.writeTextASCII(value, writer);
            textHeap.set(target, value, 0, value.length());
        }
    }

    protected void genWriteTextDelta(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(target, value);
        int tailCount = textHeap.countTailMatch(target, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(target) - headCount;
            if (trimTail < 0) {
                throw new UnsupportedOperationException(trimTail + "");
            }
            PrimitiveWriter.writeIntegerSigned(trimTail, writer);
            PrimitiveWriter.writeTextASCIIAfter(headCount, value, writer);
            textHeap.appendTail(target, trimTail, headCount, value);
        } else {
            int trimHead = textHeap.length(target) - tailCount;
            PrimitiveWriter.writeIntegerSigned(0 == trimHead ? 0 : -trimHead, writer);
            
            int sentLen = value.length() - tailCount;
            PrimitiveWriter.writeTextASCIIBefore(value, sentLen, writer);
            textHeap.appendHead(target, trimHead, value, sentLen);
        }
    }
    
    protected void genWriteTextTail(int target, CharSequence value, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(target, value);
        int trimTail = textHeap.length(target) - headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail, writer);
        PrimitiveWriter.writeTextASCIIAfter(headCount, value, writer);
        textHeap.appendTail(target, trimTail, headCount, value);
    }

    protected void genWriteTextNone(CharSequence value, PrimitiveWriter writer) {
        PrimitiveWriter.writeTextASCII(value, writer);
    }
    
    protected void genWriteTextUTFDefaultOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length + 1, writer);
            PrimitiveWriter.writeTextUTF(value, offset, length, writer);
        }
    }

    protected void genWriteTextUTFCopyOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length + 1, writer);
            PrimitiveWriter.writeTextUTF(value, offset, length, writer);
            textHeap.set(target, value, offset, length);
        }
    }

    protected void genWriteTextUTFDeltaOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int tailCount = textHeap.countTailMatch(target, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(target) - headCount;
            int valueSend = length - headCount;
            int startAfter = offset + headCount;
            textHeap.appendTail(target, trimTail, value, startAfter, valueSend);
            PrimitiveWriter.writeIntegerUnsigned(trimTail + 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            PrimitiveWriter.writeTextUTF(value, startAfter, valueSend, writer);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(target) - tailCount;
            PrimitiveWriter.writeIntegerSigned(trimHead == 0 ? 1 : -trimHead, writer);
            int len = length - tailCount;
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            PrimitiveWriter.writeTextUTF(value, offset, len, writer);
            textHeap.appendHead(target, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextUTFConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteTextUTFTailOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int trimTail = textHeap.length(target) - headCount;
        int valueSend = length - headCount;
        int startAfter = offset + headCount;
        textHeap.appendTail(target, trimTail, value, startAfter, valueSend);
        
        PrimitiveWriter.writeIntegerUnsigned(trimTail + 1, writer);
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        PrimitiveWriter.writeTextUTF(value, startAfter, valueSend, writer);
    }

    protected void genWriteTextUTFNoneOptional(int offset, int length, char[] value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(length + 1, writer);
        PrimitiveWriter.writeTextUTF(value, offset, length, writer);
    }

    protected void genWriteTextUTFDefault(int constId, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(constId, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length, writer);
            PrimitiveWriter.writeTextUTF(value, offset, length, writer);
        }
    }

    protected void genWriteTextUTFCopy(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length, writer);
            PrimitiveWriter.writeTextUTF(value, offset, length, writer);
            textHeap.set(target, value, offset, length);
        }
    }

    protected void genWriteTextUTFDelta(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int tailCount = textHeap.countTailMatch(target, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(target) - headCount;
            int valueSend = length - headCount;
            int startAfter = offset + headCount + headCount;
            textHeap.appendTail(target, trimTail, value, startAfter, valueSend);
            
            PrimitiveWriter.writeIntegerUnsigned(trimTail + 0, writer);
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            PrimitiveWriter.writeTextUTF(value, startAfter, valueSend, writer);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(target) - tailCount;
            PrimitiveWriter.writeIntegerSigned(trimHead == 0 ? 0 : -trimHead, writer);
            
            int len = length - tailCount;
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            PrimitiveWriter.writeTextUTF(value, offset, len, writer);
            
            textHeap.appendHead(target, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextUTFTail(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int trimTail = textHeap.length(target) - headCount;
        int valueSend = length - headCount;
        int startAfter = offset + headCount;
        textHeap.appendTail(target, trimTail, value, startAfter, valueSend);
        
        PrimitiveWriter.writeIntegerUnsigned(trimTail + 0, writer);
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        PrimitiveWriter.writeTextUTF(value, startAfter, valueSend, writer);
    }

    
    protected void genWriteTextUTFNone(int offset, int length, char[] value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(length, writer);
        PrimitiveWriter.writeTextUTF(value, offset, length, writer);
    }
    
    protected void genWriteTextDefaultOptional(int constId, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(constId, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeTextASCII(value, offset, length, writer);
        }
    }

    protected void genWriteTextCopyOptional(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(target, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeTextASCII(value, offset, length, writer);
            textHeap.set(target, value, offset, length);
        }
    }

    protected void genWriteTextDeltaOptional2(int target, int offset, int length, char[] value, TextHeap textHeap, PrimitiveWriter writer) {
        
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int tailCount = textHeap.countTailMatch(target, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(target) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            PrimitiveWriter.writeIntegerSigned(trimTail + 1, writer); // cut off these from tail,
                                                     // also add 1 because this
                                                     // is optional
   
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            PrimitiveWriter.writeTextASCII(value, valueStart, valueSend, writer);
            textHeap.appendTail(target, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(target) - tailCount;
            PrimitiveWriter.writeIntegerSigned(0 == trimHead ? 1 : -trimHead, writer);
        
            int len = length - tailCount;
            PrimitiveWriter.writeTextASCII(value, offset, len, writer);
            textHeap.appendHead(target, trimHead, value, offset, len);
        
        }
    }
    
    
    protected void genWriteTextConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        // the writeNull will take care of the rest.
    }

    protected void genWriteTextTailOptional2(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int trimTail = textHeap.length(target) - headCount; // head count is total that
                                                     // match from head.
        
        PrimitiveWriter.writeIntegerUnsigned(trimTail + 1, writer); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        PrimitiveWriter.writeTextASCII(value, valueStart, valueSend, writer);
        textHeap.appendTail(target, trimTail, value, valueStart, valueSend);
    }

    protected void genWriteTextNoneOptional(char[] value, int offset, int length, PrimitiveWriter writer) {
        PrimitiveWriter.writeTextASCII(value, offset, length, writer);
    }
    
    protected void genWriteTextDefault2(int target, int offset, int length, char[] value, TextHeap textHeap, PrimitiveWriter writer) {
        
        if (textHeap.equals(target | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeTextASCII(value, offset, length, writer);
        }
    }

    protected void genWriteTextCopy2(int target, int offset, int length, char[] value, TextHeap textHeap, PrimitiveWriter writer) {
        
        if (textHeap.equals(target, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeTextASCII(value, offset, length, writer);
            textHeap.set(target, value, offset, length);
        }
    }

    protected void genWriteTextDelta2(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {
        
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int tailCount = textHeap.countTailMatch(target, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(target) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            PrimitiveWriter.writeIntegerSigned(trimTail, writer); // cut off these from tail
        
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            PrimitiveWriter.writeTextASCII(value, valueStart, valueSend, writer);
            textHeap.appendTail(target, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(target) - tailCount;
            PrimitiveWriter.writeIntegerUnsigned(-trimHead, writer);
        
            int len = length - tailCount;
            PrimitiveWriter.writeTextASCII(value, offset, len, writer);
        
            textHeap.appendHead(target, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextTail2(int target, int offset, int length, char[] value, PrimitiveWriter writer, TextHeap textHeap) {

        int headCount = textHeap.countHeadMatch(target, value, offset, length);
        int trimTail = textHeap.length(target) - headCount; // head count is total that
                                                     // match from head.
        PrimitiveWriter.writeIntegerUnsigned(trimTail, writer); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        PrimitiveWriter.writeTextASCII(value, valueStart, valueSend, writer);
        textHeap.appendTail(target, trimTail, value, valueStart, valueSend);
    }

    protected void genWriteTextNone(char[] value, int offset, int length, PrimitiveWriter writer) {
        PrimitiveWriter.writeTextASCII(value, offset, length, writer);
    }
    
    protected void genWriterBytesDefaultOptional(int target, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
        
        if (byteHeap.equals(target|INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer); 
            value.position(value.limit());//skip over the data just like we wrote it.
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            int len = value.remaining();
            if (len<0) {
                len = 0;
            }
            PrimitiveWriter.writeIntegerUnsigned(len+1, writer);
            PrimitiveWriter.writeByteArrayData(value, writer);
        }
    }

    protected void genWriterBytesCopyOptional(int target, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {

        if (byteHeap.equals(target, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
            value.position(value.limit());//skip over the data just like we wrote it.
        } 
        else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(value.remaining()+1, writer);
            byteHeap.set(target, value);//position is NOT modified
            PrimitiveWriter.writeByteArrayData(value, writer); //this moves the position in value
        }
    }

    protected void genWriterBytesDeltaOptional(int target, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {

        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(target, value);
        int tailCount = byteHeap.countTailMatch(target, value);
        if (headCount>tailCount) {
            StaticGlue.writeBytesTail(target, headCount, value, 1, byteHeap, writer); //does not modify position
        } else {
            StaticGlue.writeBytesHead(target, tailCount, value, 1, byteHeap, writer); //does not modify position
        }
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriterBytesTailOptional(int target, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {

        int headCount = byteHeap.countHeadMatch(target, value);
        int trimTail = byteHeap.length(target)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+1 : trimTail, writer);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(target, trimTail, value, startAfter, valueSend);
        PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriterBytesNoneOptional(ByteBuffer value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(value.remaining()+1, writer);
        PrimitiveWriter.writeByteArrayData(value, writer); //this moves the position in value
    }

    protected void genWriteBytesDefault(int target, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
        
        if (byteHeap.equals(target|INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
            value.position(value.limit());//skip over the data just like we wrote it.
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(value.remaining(), writer);
            PrimitiveWriter.writeByteArrayData(value, writer); //this moves the position in value
        }
    }

    protected void genWriteBytesCopy(int target, ByteBuffer value, ByteHeap byteHeap, PrimitiveWriter writer) {

        if (byteHeap.equals(target, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
            value.position(value.limit());//skip over the data just like we wrote it.
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(value.remaining(), writer);
            byteHeap.set(target, value);//position is NOT modified
            PrimitiveWriter.writeByteArrayData(value, writer); //this moves the position in value
        }
    }

    protected void genWriteBytesDelta(int target, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(target, value);
        int tailCount = byteHeap.countTailMatch(target, value);
        if (headCount>tailCount) {
            int trimTail = byteHeap.length(target)-headCount;
            if (trimTail<0) {
                throw new ArrayIndexOutOfBoundsException();
            }
            PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+0 : trimTail, writer);
            
            int valueSend = value.remaining()-headCount;
            int startAfter = value.position()+headCount;
                    
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
            byteHeap.appendTail(target, trimTail, value, startAfter, valueSend);
            PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer); //does not modify position
        } else {
            //replace head, tail matches to tailCount
            int trimHead = byteHeap.length(target)-tailCount;
            PrimitiveWriter.writeIntegerSigned(trimHead==0? 0: -trimHead, writer); 
            
            int len = value.remaining() - tailCount;
            int offset = value.position();
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            PrimitiveWriter.writeByteArrayData(value, offset, len, writer);
            byteHeap.appendHead(target, trimHead, value, offset, len); //does not modify position
        }
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriteBytesTail(int target, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {

        int headCount = byteHeap.countHeadMatch(target, value);
                
        int trimTail = byteHeap.length(target)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+0 : trimTail, writer);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(target, trimTail, value, startAfter, valueSend);
        PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriteBytesNone(ByteBuffer value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(value.remaining(), writer);
        PrimitiveWriter.writeByteArrayData(value, writer); //this moves the position in value
    }
    
    protected void genWriteBytesDefault(int target, int offset, int length, byte[] value, ByteHeap byteHeap, PrimitiveWriter writer) {
        
        if (byteHeap.equals(target|INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length, writer);
            PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
        }
    }

    protected void genWriteBytesCopy(int target, int offset, int length, byte[] value, ByteHeap byteHeap, PrimitiveWriter writer) {
        
        if (byteHeap.equals(target, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        }
        else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length, writer);
            PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
            byteHeap.set(target, value, offset, length);
        }
    }

    public void genWriteBytesDelta(int target, int offset, int length, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(target, value, offset, length);
        int tailCount = byteHeap.countTailMatch(target, value, offset+length, length);
        if (headCount>tailCount) {
            writeBytesTail(target, headCount, offset+headCount, length, 0, value, writer, byteHeap);
        } else {
            writeBytesHead(target, tailCount, offset, length, 0, value, writer, byteHeap);
        }
    }

    public void genWriteBytesTail(int target, int offset, int length, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        int headCount = byteHeap.countHeadMatch(target, value, offset, length);
        
        int trimTail = byteHeap.length(target)-headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail, writer);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
        byteHeap.appendTail(target, trimTail, value, startAfter, valueSend);
    }

    protected void genWriteBytesNone(int offset, int length, byte[] value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(length, writer);
        PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
    }
    
    private void writeBytesHead(int target, int tailCount, int offset, int length, int opt, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        
        //replace head, tail matches to tailCount
        int trimHead = byteHeap.length(target)-tailCount;
        PrimitiveWriter.writeIntegerSigned(trimHead==0? opt: -trimHead, writer); 
        
        int len = length - tailCount;
        PrimitiveWriter.writeIntegerUnsigned(len, writer);
        PrimitiveWriter.writeByteArrayData(value, offset, len, writer);
        
        byteHeap.appendHead(target, trimHead, value, offset, len);
    }
    
   private void writeBytesTail(int target, int headCount, int offset, int length, final int optional, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        int trimTail = byteHeap.length(target)-headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+optional: trimTail, writer);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
        byteHeap.appendTail(target, trimTail, value, startAfter, valueSend);
    }
    
    public void genWriteBytesDefaultOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        if (byteHeap.equals(target, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length+1, writer);
            PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
        }
    }

    public void genWriteBytesCopyOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        if (byteHeap.equals(target, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeIntegerUnsigned(length+1, writer);
            PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
            byteHeap.set(target, value, offset, length);
        }
    }

    public void genWriteBytesDeltaOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(target, value, offset, length);
        int tailCount = byteHeap.countTailMatch(target, value, offset+length, length);
        if (headCount>tailCount) {
            int trimTail = byteHeap.length(target)-headCount;
            PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail, writer);
            
            int valueSend = length-headCount;
            int startAfter = offset+headCount;
            
            PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
            PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
            byteHeap.appendTail(target, trimTail, value, startAfter, valueSend);
        } else {
            //replace head, tail matches to tailCount
            int trimHead = byteHeap.length(target)-tailCount;
            PrimitiveWriter.writeIntegerSigned(trimHead==0? 1: -trimHead, writer); 
            
            int len = length - tailCount;
            PrimitiveWriter.writeIntegerUnsigned(len, writer);
            PrimitiveWriter.writeByteArrayData(value, offset, len, writer);
            
            byteHeap.appendHead(target, trimHead, value, offset, len);
        }
    }

    protected void genWriteBytesConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte)1, writer);
        //the writeNull will take care of the rest.
    }

    public void genWriteBytesTailOptional(int target, int offset, int length, byte[] value, PrimitiveWriter writer, ByteHeap byteHeap) {
        int headCount = byteHeap.countHeadMatch(target, value, offset, length);
        int trimTail = byteHeap.length(target)-headCount;
        PrimitiveWriter.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail, writer);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        PrimitiveWriter.writeIntegerUnsigned(valueSend, writer);
        PrimitiveWriter.writeByteArrayData(value, startAfter, valueSend, writer);
        byteHeap.appendTail(target, trimTail, value, startAfter, valueSend);
    }

    protected void genWriteBytesNoneOptional(int offset, int length, byte[] value, PrimitiveWriter writer) {
        PrimitiveWriter.writeIntegerUnsigned(length+1, writer);
        PrimitiveWriter.writeByteArrayData(value,offset,length, writer);
    }
    
    protected void genWriteIntegerSignedDefault(int constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSignedDefault(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), constDefault, writer);
    }

    protected void genWriteIntegerSignedIncrement(int target, int source, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSignedIncrement(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerSignedCopy(int target, int source, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSignedCopy(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerSignedDelta(int target, int source, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSignedDelta(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerSignedNone(int target, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerSigned(intValues[target] = FASTRingBufferReader.readInt(rbRingBuffer, rbPos), writer);
    }
    
    protected void genWriteIntegerUnsignedDefault(int constDefault, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsignedDefault(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), constDefault, writer);
    }

    protected void genWriteIntegerUnsignedIncrement( int target, int source, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsignedIncrement(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerUnsignedCopy(int target, int source, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsignedCopy(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerUnsignedDelta(int target, int source, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsignedDelta(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues, writer);
    }

    protected void genWriteIntegerUnsignedNone(int target, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeIntegerUnsigned(intValues[target] = FASTRingBufferReader.readInt(rbRingBuffer, rbPos), writer);
    }

    protected void genWriteIntegerSignedDefaultOptional(int source, int constDefault, int valueOfNull, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullDefaultInt(writer, intValues, source); // null for default 
            } else {
                PrimitiveWriter.writeIntegerSignedDefaultOptional(value>=0?value+1:value, constDefault, writer);
            }
        }
    }

    protected void genWriteIntegerSignedIncrementOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                int last = intValues[source];
                PrimitiveWriter.writeIntegerSignedIncrementOptional(intValues[target] = (value>=0?value+1:value), last, writer);  
            }
        }
    }

    protected void genWriteIntegerSignedCopyOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            //TODO: C, these reader calls should all be inlined to remove the object de-ref by passing in the mask and buffer directly as was done in the reader.
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else {        
                PrimitiveWriter.writeIntegerSignedCopyOptional(value>=0?value+1:value, target, source, intValues, writer);
            }
        }   
    }

    //this is how a "boolean" is sent using a single bit in the encoding.
    protected void genWriteIntegerSignedConstantOptional(int valueOfNull, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit(valueOfNull==FASTRingBufferReader.readInt(rbRingBuffer, rbPos) ? (byte)0 : (byte)1, writer);  // 1 for const, 0 for absent
    }

    protected void genWriteIntegerSignedDeltaOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedDeltaOptional(value, target, source, intValues, writer);
            }
        }
    }

    protected void genWriteIntegerSignedNoneOptional(int target, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(value, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedCopyOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                PrimitiveWriter.writeIntegerUnsignedCopyOptional(value, target, source, intValues, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedDefaultOptional(int source, int valueOfNull, int constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullDefaultInt(writer, intValues, source); // null for default 
            } else {
                PrimitiveWriter.writeIntegerUnsignedDefaultOptional(value, constDefault, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedIncrementOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                PrimitiveWriter.writeIntegerUnsignedIncrementOptional(value, target, source, intValues, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedConstantOptional(int valueOfNull, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writePMapBit(valueOfNull==FASTRingBufferReader.readInt(rbRingBuffer,rbPos) ? (byte)0 : (byte)1, writer);  // 1 for const, 0 for absent
    }

    protected void genWriteIntegerUnsignedDeltaOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,rbPos);
            if (valueOfNull == value) {
            StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerUnsignedDeltaOptional(value, target, source, intValues, writer);
            }
        }
    }

    protected void genWriteIntegerUnsignedNoneOptional(int target, int valueOfNull, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,rbPos);
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
    
      protected void genWriteDecimalDefaultOptionalNone(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, int[] intValues) {
      {
        int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
        if (exponentValueOfNull == exponentValue) {
            StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
        } else {
            PrimitiveWriter.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault, writer);
            assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
            activeScriptCursor++;
            PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), writer); 
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else { 
                int last = intValues[exponentSource];
                PrimitiveWriter.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last, writer); 
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), writer); 
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else {        
                PrimitiveWriter.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), writer); 
            }
        }
    }

    protected void genWriteDecimalConstantOptionalNone(int exponentValueOfNull, int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues) {
        { 
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull==exponentValue) {
                PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), writer);        
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalNone(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), writer); 
            }
        }
    }

    protected void genWriteDecimalNoneOptionalNone(int exponentTarget, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                PrimitiveWriter.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), writer); 
            }
        }
    }
    
    // DEFAULTS
   
   
     protected void genWriteDecimalDefaultOptionalDefault(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
      {
        int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
        if (exponentValueOfNull == exponentValue) {
            StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
        } else {
            PrimitiveWriter.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault, writer);
            assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
            activeScriptCursor++;
            //mantissa
            PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), mantissaConstDefault, writer);
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else { 
                int last = intValues[exponentSource];
                PrimitiveWriter.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last, writer); 
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), mantissaConstDefault, writer);
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else {        
                PrimitiveWriter.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), mantissaConstDefault, writer);
            }
        }
    }

    protected void genWriteDecimalConstantOptionalDefault(int exponentValueOfNull, int mantissaTarget, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        { 
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull==exponentValue) {
                PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), mantissaConstDefault, writer);
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalDefault(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), mantissaConstDefault, writer);
            }
        }
    }

    protected void genWriteDecimalNoneOptionalDefault(int exponentTarget, int mantissaTarget, int exponentValueOfNull, long mantissaConstDefault, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                activeScriptCursor++;
                //mantissa
                PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos), mantissaConstDefault, writer);
            }
        }
    }
    
    
    // Increment
    
    
    protected void genWriteDecimalDefaultOptionalIncrement(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
     {
       int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
       if (exponentValueOfNull == exponentValue) {
           StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
       } else {
           PrimitiveWriter.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault, writer);
           assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
           activeScriptCursor++;
           //mantissa
           long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
           PrimitiveWriter.writeLongSignedIncrement(value,  longValues[mantissaSource], writer);
           longValues[mantissaTarget] = value;
       }
     }
   }

   protected void genWriteDecimalIncrementOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
       {
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
           } else { 
               int last = intValues[exponentSource];
               PrimitiveWriter.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last, writer); 
               assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
               activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
               PrimitiveWriter.writeLongSignedIncrement(value,  longValues[mantissaSource], writer);
               longValues[mantissaTarget] = value;
           }   
       }
   }

   protected void genWriteDecimalCopyOptionalIncrement(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
       {   
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
           } else {        
               PrimitiveWriter.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues, writer);
               assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
               activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
               PrimitiveWriter.writeLongSignedIncrement(value,  longValues[mantissaSource], writer);
               longValues[mantissaTarget] = value;
           }
       }
   }

   protected void genWriteDecimalConstantOptionalIncrement(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
       { 
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
           if (exponentValueOfNull==exponentValue) {
               PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
           } else {
               PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
               assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
               activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
               PrimitiveWriter.writeLongSignedIncrement(value,  longValues[mantissaSource], writer);
               longValues[mantissaTarget] = value;
           }     
       }
   }

   protected void genWriteDecimalDeltaOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
       {   
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
           } else {
               PrimitiveWriter.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues, writer);
               assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
               activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
               PrimitiveWriter.writeLongSignedIncrement(value,  longValues[mantissaSource], writer);
               longValues[mantissaTarget] = value;
           }
       }
   }

   protected void genWriteDecimalNoneOptionalIncrement(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
       {   
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
           } else {
               PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
               assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
               activeScriptCursor++;
               //mantissa
               long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
               PrimitiveWriter.writeLongSignedIncrement(value,  longValues[mantissaSource], writer);
               longValues[mantissaTarget] = value;
           }
       }
   }
    
   //copy
   
   protected void genWriteDecimalDefaultOptionalCopy(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
       {
         int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
         if (exponentValueOfNull == exponentValue) {
             StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
         } else {
             PrimitiveWriter.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault, writer);
             assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
             activeScriptCursor++;
             //mantissa
             long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);   
             PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
         }
       }
     }

     protected void genWriteDecimalIncrementOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
         {
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
             } else { 
                 int last = intValues[exponentSource];
                 PrimitiveWriter.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last, writer); 
                 assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                 activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }   
         }
     }

     protected void genWriteDecimalCopyOptionalCopy(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
         {   
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
             } else {        
                 PrimitiveWriter.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues, writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                 activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }
         }
     }

     protected void genWriteDecimalConstantOptionalCopy(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
         { 
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
             if (exponentValueOfNull==exponentValue) {
                 PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
             } else {
                 PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                 assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                 activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }     
         }
     }

     protected void genWriteDecimalDeltaOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
         {   
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
             } else {
                 PrimitiveWriter.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues, writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                 activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }
         }
     }

     protected void genWriteDecimalNoneOptionalCopy(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
         {   
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
             } else {
                 PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                 activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos);
                 PrimitiveWriter.writeLongSignedCopy(value, mantissaTarget, mantissaSource, longValues, writer);
             }
         }
     } 
   
   //constant
  
     protected void genWriteDecimalDefaultOptionalConstant(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
         {
           int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
           if (exponentValueOfNull == exponentValue) {
               StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
           } else {
               PrimitiveWriter.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault, writer);
               assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
               activeScriptCursor++;
               //mantissa
               //is constant so do nothing
           }
         }
       }

       protected void genWriteDecimalIncrementOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
           {
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
               } else { 
                   int last = intValues[exponentSource];
                   PrimitiveWriter.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last, writer); 
                   assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                   activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }   
           }
       }

       protected void genWriteDecimalCopyOptionalConstant(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
           {   
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
               } else {        
                   PrimitiveWriter.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues, writer);
                   assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                   activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }
           }
       }

       protected void genWriteDecimalConstantOptionalConstant(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
           { 
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
               if (exponentValueOfNull==exponentValue) {
                   PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
               } else {
                   PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                   assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                   activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }     
           }
       }

       protected void genWriteDecimalDeltaOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
           {   
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
               } else {
                   PrimitiveWriter.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues, writer);
                   assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                   activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }
           }
       }

       protected void genWriteDecimalNoneOptionalConstant(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
           {   
               int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
               if (exponentValueOfNull == exponentValue) {
                   StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
               } else {
                   PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                   assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                   activeScriptCursor++;
                   //mantissa
                   //is constant so do nothing
               }
           }
       }      
     
  //delta
       
       protected void genWriteDecimalDefaultOptionalDelta(int exponentSource, int mantissaSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
           {
             int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
             if (exponentValueOfNull == exponentValue) {
                 StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
             } else {
                 PrimitiveWriter.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault, writer);
                 assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                 activeScriptCursor++;
                 //mantissa
                 long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos); 
                 PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                 longValues[mantissaTarget] = value;
             }
           }
         }

         protected void genWriteDecimalIncrementOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
             {
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
                 } else { 
                     int last = intValues[exponentSource];
                     PrimitiveWriter.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last, writer); 
                     assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                     activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }   
             }
         }

         protected void genWriteDecimalCopyOptionalDelta(int exponentTarget, int exponentSource, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
             {   
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
                 } else {        
                     PrimitiveWriter.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues, writer);
                     assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                     activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }
             }
         }

         protected void genWriteDecimalConstantOptionalDelta(int exponentValueOfNull, int mantissaSource, int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
             { 
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
                 if (exponentValueOfNull==exponentValue) {
                     PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
                 } else {
                     PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                     assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                     activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }     
             }
         }

         protected void genWriteDecimalDeltaOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
             {   
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
                 } else {
                     PrimitiveWriter.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues, writer);
                     assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                     activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }
             }
         }

         protected void genWriteDecimalNoneOptionalDelta(int exponentTarget, int mantissaSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
             {   
                 int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
                 if (exponentValueOfNull == exponentValue) {
                     StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
                 } else {
                     PrimitiveWriter.writeIntegerSignedOptional(exponentValue, writer);
                     assert(FASTEncoder.notifyFieldPositions(writer, activeScriptCursor));
                     activeScriptCursor++;
                     //mantissa
                     long value = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos); 
                     PrimitiveWriter.writeLongSigned(value - longValues[mantissaSource], writer);
                     longValues[mantissaTarget] = value;
                 }
             }
         } 
          
    
    //////////////
    //end of decimals
    ///////////////
    
    protected void genWriteLongUnsignedDefault(long constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {        
        PrimitiveWriter.writeLongUnsignedDefault(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), constDefault, writer);
    }

    protected void genWriteLongUnsignedIncrement(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongUnsignedIncrement(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), target, source, longValues, writer);
    }

    protected void genWriteLongUnsignedCopy(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongUnsignedCopy(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), target, source, longValues, writer);
    }

    protected void genWriteLongUnsignedDelta(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
        PrimitiveWriter.writeLongSigned(value - longValues[source], writer);
        longValues[target] = value;
    }

    protected void genWriteLongUnsignedNone(int target, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongUnsigned(longValues[target] = FASTRingBufferReader.readLong(rbRingBuffer, rbPos), writer);
    }
    
    protected void genWriteLongUnsignedDefaultOptional(long constDefault, long value, PrimitiveWriter writer) {
        PrimitiveWriter.writneLongUnsignedDefaultOptional(value, constDefault, writer);
    }

    protected void genWriteLongUnsignedIncrementOptional(int target, int source, long value, PrimitiveWriter writer, long[] longValues) {
        PrimitiveWriter.writeLongUnsignedIncrementOptional(value, target, source, longValues, writer);
    }

    protected void genWriteLongUnsignedCopyOptional(int target, int source, long value, PrimitiveWriter writer, long[] longValues) {
        value++;// zero is held for null
        
        PrimitiveWriter.writeLongUnsignedCopyOptional(value, target, source, longValues, writer);
    }

    protected void genWriteLongUnsignedConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteLongUnsignedNoneOptional(long value, PrimitiveWriter writer) {
        PrimitiveWriter.writeLongUnsigned(value + 1, writer);
    }

    protected void genWriteLongUnsignedDeltaOptional(int target, int source, long value, PrimitiveWriter writer, long[] longValues) {
        long delta = value - longValues[source];
        PrimitiveWriter.writeLongSigned(delta>=0 ? 1+delta : delta, writer);
        longValues[target] = value;
    }
    
    protected void genWriteLongSignedDefault(long constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongSignedDefault(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), constDefault, writer);
    }

    protected void genWriteLongSignedIncrement(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
        PrimitiveWriter.writeLongSignedIncrement(value,  longValues[source], writer);
        longValues[target] = value;
    }

    protected void genWriteLongSignedCopy(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
        PrimitiveWriter.writeLongSignedCopy(value, target, source, longValues, writer);
    }

    protected void genWriteLongSignedNone(int target, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        PrimitiveWriter.writeLongSigned(longValues[target] = FASTRingBufferReader.readLong(rbRingBuffer, rbPos), writer);
    }

    protected void genWriteLongSignedDelta(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
        PrimitiveWriter.writeLongSigned(value - longValues[source], writer);
        longValues[target] = value;
    }
    
    protected void genWriteLongSignedOptional(long value, PrimitiveWriter writer) {
        PrimitiveWriter.writeLongSignedOptional(value, writer);
    }

    protected void genWriteLongSignedDeltaOptional(int target, int source, long value, PrimitiveWriter writer, long[] longValues) {
        long delta = value - longValues[source];
        PrimitiveWriter.writeLongSigned(((delta + (delta >>> 63)) + 1), writer);
        longValues[target] = value;
    }

    protected void genWriteLongSignedConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }
    

    protected void genWriteLongSignedCopyOptional(int target, int source, long value, PrimitiveWriter writer, long[] longValues) {
        PrimitiveWriter.writeLongSignedCopy(value-((value>>63)-1), target, source, longValues, writer);
    }

    protected void genWriteLongSignedIncrementOptional(int target, int source, long value, PrimitiveWriter writer, long[] longValues) {
        value-=((value>>63)-1);
        PrimitiveWriter.writeLongSignedIncrementOptional(value, longValues[source], writer);
        longValues[target] = value;
    }

    //branched version replaced by  -((value>>63)-1)  
    //        if (value >= 0) {
    //            value++;// room for null
    //        }
    protected void genWriteLongSignedDefaultOptional(long constDefault, long value, PrimitiveWriter writer) {
        PrimitiveWriter.writeLongSignedDefault(value-((value>>63)-1), constDefault, writer);
    }

    protected void genWriteDictionaryBytesReset(int target, ByteHeap byteHeap) {
        byteHeap.setNull(target);
    }

    protected void genWriteDictionaryTextReset(int target, TextHeap textHeap) {
        textHeap.reset(target);
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
    
    // must happen just before Group so the Group in question must always have
    // an outer group.
    protected void pushTemplate(int fieldPos, PrimitiveWriter writer, FASTRingBuffer queue) {

        int templateId = FASTRingBufferReader.readInt(queue, fieldPos);
        
     //   int top = dispatch.templateStack[dispatch.templateStackHead];
//        if (top == templateId) {
//            PrimitiveWriter.writePMapBit((byte) 0, writer);
//        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeIntegerUnsigned(templateId, writer);
      //      top = templateId;
     //   }

        //dispatch.templateStack[dispatch.templateStackHead++] = top;
    }
    

    protected void genWriteOpenTemplatePMap(int pmapSize, int fieldPos, PrimitiveWriter writer, FASTRingBuffer queue) {
        PrimitiveWriter.openPMap(pmapSize, writer);  //FASTRingBuffer queue, int fieldPos
        // done here for safety to ensure it is always done at group open.
        pushTemplate(fieldPos, writer, queue);
     }
    
    protected void genWriteOpenGroup(int pmapSize, PrimitiveWriter writer) {
        PrimitiveWriter.openPMap(pmapSize, writer);
    }
    
    public void genWriteNullPMap(PrimitiveWriter writer) {
        StaticGlue.nullPMap(writer);  // null for const optional
    }

    public void genWriteNullDefaultLong(int target, PrimitiveWriter writer, long[] dictionary) {
        if (dictionary[target] == 0) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }

    public void genWriteNullCopyIncLong(int target, PrimitiveWriter writer, long[] dictionary) {
        if (0 == dictionary[target]) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            dictionary[target] = 0;
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }

    public void genWriteNullNoPMapLong(int target, PrimitiveWriter writer, long[] dictionary) {
        dictionary[target] = 0;
        PrimitiveWriter.writeNull(writer);
    }
    
    public void genWriteNullDefaultText(int target, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.isNull(target)) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }

    public void genWriteNullCopyIncText(int target, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.isNull(target)) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            PrimitiveWriter.writeNull(writer);
            TextHeap.setNull(target, textHeap);
        }
    }

    public void genWriteNullNoPMapText(int target, PrimitiveWriter writer, TextHeap textHeap) {
        PrimitiveWriter.writeNull(writer);
        TextHeap.setNull(target, textHeap);
    }
    
    public void genWriteNullDefaultBytes(int target, PrimitiveWriter writer, ByteHeap byteHeap) {
        if (byteHeap.isNull(target)) { //stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeNull(writer);
        }
    }

    public void genWriteNullNoPMapBytes(int target, PrimitiveWriter writer, ByteHeap byteHeap) {
        PrimitiveWriter.writeNull(writer);
        byteHeap.setNull(target);
    }

    public void genWriteNullCopyIncBytes(int target, PrimitiveWriter writer, ByteHeap byteHeap) {
        if (byteHeap.isNull(target)) { //stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            PrimitiveWriter.writeNull(writer);
            byteHeap.setNull(target);
        }
    }
}
