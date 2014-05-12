package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveWriter;


public class FASTWriterDispatchTemplates extends FASTWriterDispatchBase {

    public FASTWriterDispatchTemplates(PrimitiveWriter writer, DictionaryFactory dcr, int maxTemplates,
            int maxCharSize, int maxBytesSize, int gapChars, int gapBytes, FASTRingBuffer queue,
            int nonTemplatePMapSize, int[][] dictionaryMembers, int[] fullScript, int maxNestedGroupDepth) {
        super(writer, dcr, maxTemplates, maxCharSize, maxBytesSize, gapChars, gapBytes, queue, nonTemplatePMapSize,
                dictionaryMembers, fullScript, maxNestedGroupDepth);
    }

    
    protected void genWriteCopyText(int source, int target, TextHeap textHeap) {
        textHeap.copy(source,target);
    }

    protected void genWriteCopyBytes(int source, int target, ByteHeap byteHeap) {
        byteHeap.copy(source,target);
    }
    
    
    protected void genWriteOpenMessage(int pmapMaxSize, int templateId, PrimitiveWriter writer) {
        writer.openPMap(pmapMaxSize);
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        writer.closePMap();// TODO: A, this needs to be close but not sure this
        // is the right location.
        writer.writeIntegerUnsigned(templateId);
    }

    protected void genWritePreamble(byte[] preambleData, PrimitiveWriter writer) {
        writer.writeByteArrayData(preambleData, 0, preambleData.length);
    }
    
    protected void genWriteUTFTextDefaultOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeNull();
            }
        } else {
            if (textHeap.equals(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeIntegerUnsigned(value.length() + 1);
                writer.writeTextUTF(value);
            }
        }
    }

    protected void genWriteUTFTextCopyOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(value.length() + 1);
            writer.writeTextUTF(value);
            textHeap.set(idx, value, 0, value.length());
        }
    }

    protected void genWriteUTFTextDeltaOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value);
        int tailCount = textHeap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount; //+1 for optional
            writer.writeIntegerSigned(trimTail >= 0 ? trimTail + 1 : trimTail);
            int length = (value.length() - headCount);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTFAfter(headCount, value);
            textHeap.appendTail(idx, trimTail, headCount, value);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 1 : -trimHead);
            int valueSend = value.length() - tailCount;
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTFBefore(value, valueSend);
            textHeap.appendHead(idx, trimHead, value, valueSend);
        }
    }
    
    protected void genWriteUTFTextConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteUTFTextTailOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail + 1);// plus 1 for optional
        int length = (value.length() - headCount);
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTFAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteUTFTextNoneOptional(CharSequence value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value.length() + 1);
        writer.writeTextUTF(value);
    }
    
    protected void genWriteUTFTextDefault(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(value.length());
            writer.writeTextUTF(value);
        }
    }

    protected void genWriteUTFTextCopy(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(value.length());
            writer.writeTextUTF(value);
            textHeap.set(idx, value, 0, value.length());
        }
    }

    protected void genWriteUTFTextDelta(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value);
        int tailCount = textHeap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            writer.writeIntegerSigned(trimTail);
            writer.writeIntegerUnsigned(value.length() - headCount);
            writer.writeTextUTFAfter(headCount, value);
            textHeap.appendTail(idx, trimTail, headCount, value);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 0 : -trimHead);
            int valueSend = value.length() - tailCount;
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTFBefore(value, valueSend);
            textHeap.appendHead(idx, trimHead, value, valueSend);
        }
    }

    protected void genWriteUTFTextTail(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail);
        int length = (value.length() - headCount);
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTFAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteUTFTextNone(CharSequence value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value.length());
        writer.writeTextUTF(value);
    }

    protected void genWriteTextDefaultOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeNull();
            }
        } else {
            if (textHeap.equals(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeTextASCII(value);
            }
        }
    }

    protected void genWriteTextCopyOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            if (textHeap.isNull(idx)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeNull();
            }
        } else {
            if (textHeap.equals(idx, value)) {
                PrimitiveWriter.writePMapBit((byte) 0, writer);
            } else {
                PrimitiveWriter.writePMapBit((byte) 1, writer);
                writer.writeTextASCII(value);
                textHeap.set(idx, value, 0, value.length());
            }
        }
    }

    protected void genWriteTextDeltaOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (null == value) {
            writer.writeIntegerSigned(0);
        } else {
            // count matching front or back chars
            int headCount = textHeap.countHeadMatch(idx, value);
            int tailCount = textHeap.countTailMatch(idx, value);
            if (headCount > tailCount) {
                int trimTail = textHeap.length(idx) - headCount;
                assert (trimTail >= 0);
                writer.writeIntegerSigned(trimTail + 1);// must add one because this
                                                        // is optional
                writer.writeTextASCIIAfter(headCount, value);
                textHeap.appendTail(idx, trimTail, headCount, value);
            } else {
                int trimHead = textHeap.length(idx) - tailCount;
                writer.writeIntegerSigned(0 == trimHead ? 1 : -trimHead);
                
                int sentLen = value.length() - tailCount;
                writer.writeTextASCIIBefore(value, sentLen);
                textHeap.appendHead(idx, trimHead, value, sentLen);
            }
        }
    }

    protected void genWriteTextTailOptional(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail + 1);
        writer.writeTextASCIIAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteNull(PrimitiveWriter writer) {
        writer.writeNull();
    }
    
    protected void genWriteTextDefault(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value);
        }
    }

    protected void genWriteTextCopy(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            // System.err.println("char seq length:"+value.length());
            writer.writeTextASCII(value);
            textHeap.set(idx, value, 0, value.length());
        }
    }

    protected void genWriteTextDelta(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value);
        int tailCount = textHeap.countTailMatch(idx, value);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            if (trimTail < 0) {
                throw new UnsupportedOperationException(trimTail + "");
            }
            writer.writeIntegerSigned(trimTail);
            writer.writeTextASCIIAfter(headCount, value);
            textHeap.appendTail(idx, trimTail, headCount, value);
        } else {
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 0 : -trimHead);
            
            int sentLen = value.length() - tailCount;
            writer.writeTextASCIIBefore(value, sentLen);
            textHeap.appendHead(idx, trimHead, value, sentLen);
        }
    }
    
    protected void genWriteTextTail(CharSequence value, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value);
        int trimTail = textHeap.length(idx) - headCount;
        writer.writeIntegerUnsigned(trimTail);
        writer.writeTextASCIIAfter(headCount, value);
        textHeap.appendTail(idx, trimTail, headCount, value);
    }

    protected void genWriteTextNone(CharSequence value, PrimitiveWriter writer) {
        writer.writeTextASCII(value);
    }
    
    protected void genWriteTextUTFDefaultOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length + 1);
            writer.writeTextUTF(value, offset, length);
        }
    }

    protected void genWriteTextUTFCopyOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length + 1);
            writer.writeTextUTF(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    protected void genWriteTextUTFDeltaOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            int valueSend = length - headCount;
            int startAfter = offset + headCount;
            textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
            writer.writeIntegerUnsigned(trimTail + 1);
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTF(value, startAfter, valueSend);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(trimHead == 0 ? 1 : -trimHead);
            int len = length - tailCount;
            writer.writeIntegerUnsigned(len);
            writer.writeTextUTF(value, offset, len);
            textHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextUTFConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteTextUTFTailOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount;
        int valueSend = length - headCount;
        int startAfter = offset + headCount;
        textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        
        writer.writeIntegerUnsigned(trimTail + 1);
        writer.writeIntegerUnsigned(valueSend);
        writer.writeTextUTF(value, startAfter, valueSend);
    }

    protected void genWriteTextUTFNoneOptional(char[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(length + 1);
        writer.writeTextUTF(value, offset, length);
    }

    protected void genWriteTextUTFDefault(char[] value, int offset, int length, int constId, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(constId, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTF(value, offset, length);
        }
    }

    protected void genWriteTextUTFCopy(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(length);
            writer.writeTextUTF(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    protected void genWriteTextUTFDelta(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount;
            int valueSend = length - headCount;
            int startAfter = offset + headCount + headCount;
            textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
            
            writer.writeIntegerUnsigned(trimTail + 0);
            writer.writeIntegerUnsigned(valueSend);
            writer.writeTextUTF(value, startAfter, valueSend);
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(trimHead == 0 ? 0 : -trimHead);
            
            int len = length - tailCount;
            writer.writeIntegerUnsigned(len);
            writer.writeTextUTF(value, offset, len);
            
            textHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextUTFTail(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount;
        int valueSend = length - headCount;
        int startAfter = offset + headCount;
        textHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        
        writer.writeIntegerUnsigned(trimTail + 0);
        writer.writeIntegerUnsigned(valueSend);
        writer.writeTextUTF(value, startAfter, valueSend);
    }

    
    protected void genWriteTextUTFNone(char[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(length);
        writer.writeTextUTF(value, offset, length);
    }
    
    protected void genWriteTextDefaultOptional(char[] value, int offset, int length, int constId, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(constId, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
        }
    }

    protected void genWriteTextCopyOptional(char[] value, int offset, int length, int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    protected void genWriteTextDeltaOptional2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            writer.writeIntegerSigned(trimTail + 1); // cut off these from tail,
                                                     // also add 1 because this
                                                     // is optional
        
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            writer.writeTextASCII(value, valueStart, valueSend);
            textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerSigned(0 == trimHead ? 1 : -trimHead);
        
            int len = length - tailCount;
            writer.writeTextASCII(value, offset, len);
            textHeap.appendHead(idx, trimHead, value, offset, len);
        
        }
    }
    
    protected void genWriteTextConstantOptional() {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        // the writeNull will take care of the rest.
    }

    protected void genWriteTextTailOptional2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount; // head count is total that
                                                     // match from head.
        
        writer.writeIntegerUnsigned(trimTail + 1); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        writer.writeTextASCII(value, valueStart, valueSend);
        textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
    }

    protected void genWriteTextNoneOptional(char[] value, int offset, int length) {
        writer.writeTextASCII(value, offset, length);
    }
    
    protected void genWriteTextDefault2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        if (textHeap.equals(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
        }
    }

    protected void genWriteTextCopy2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        if (textHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    protected void genWriteTextDelta2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        // count matching front or back chars
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = textHeap.countTailMatch(idx, value, offset + length, length);
        if (headCount > tailCount) {
            int trimTail = textHeap.length(idx) - headCount; // head count is total
                                                         // that match from
                                                         // head.
            writer.writeIntegerSigned(trimTail); // cut off these from tail
        
            int valueSend = length - headCount;
            int valueStart = offset + headCount;
        
            writer.writeTextASCII(value, valueStart, valueSend);
            textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
        
        } else {
            // replace head, tail matches to tailCount
            int trimHead = textHeap.length(idx) - tailCount;
            writer.writeIntegerUnsigned(-trimHead);
        
            int len = length - tailCount;
            writer.writeTextASCII(value, offset, len);
        
            textHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    protected void genWriteTextTail2(int token, char[] value, int offset, int length) {
        int idx = token & TEXT_INSTANCE_MASK;
        int headCount = textHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = textHeap.length(idx) - headCount; // head count is total that
                                                     // match from head.
        writer.writeIntegerUnsigned(trimTail); // cut off these from tail
        
        int valueSend = length - headCount;
        int valueStart = offset + headCount;
        
        writer.writeTextASCII(value, valueStart, valueSend);
        textHeap.appendTail(idx, trimTail, value, valueStart, valueSend);
    }

    protected void genWriteTextNone(char[] value, int offset, int length) {
        writer.writeTextASCII(value, offset, length);
    }
    
    protected void genWriterBytesDefaultOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer); 
            value.position(value.limit());//skip over the data just like we wrote it.
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            int len = value.remaining();
            if (len<0) {
                len = 0;
            }
            writer.writeIntegerUnsigned(len+1);
            writer.writeByteArrayData(value);
        }
    }

    protected void genWriterBytesCopyOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
            value.position(value.limit());//skip over the data just like we wrote it.
        } 
        else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(value.remaining()+1);
            byteHeap.set(idx, value);//position is NOT modified
            writer.writeByteArrayData(value); //this moves the position in value
        }
    }

    protected void genWriterBytesDeltaOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value);
        int tailCount = byteHeap.countTailMatch(idx, value);
        if (headCount>tailCount) {
            writeBytesTail(idx, headCount, value, 1); //does not modify position
        } else {
            writeBytesHead(idx, tailCount, value, 1); //does not modify position
        }
        value.position(value.limit());//skip over the data just like we wrote it.
    }
    
    //TODO: B, will be static
    private void writeBytesTail(int idx, int headCount, ByteBuffer value, final int optional) {
        
    
        
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+optional : trimTail);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        writer.writeIntegerUnsigned(valueSend);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        
    }
    
    //TODO: B,  will be static
    private void writeBytesHead(int idx, int tailCount, ByteBuffer value, int opt) {
        
        //replace head, tail matches to tailCount
        int trimHead = byteHeap.length(idx)-tailCount;
        writer.writeIntegerSigned(trimHead==0? opt: -trimHead); 
        
        int len = value.remaining() - tailCount;
        int offset = value.position();
        writer.writeIntegerUnsigned(len);
        writer.writeByteArrayData(value, offset, len);
        byteHeap.appendHead(idx, trimHead, value, offset, len);
    }

    protected void genWriterBytesTailOptional(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        int headCount = byteHeap.countHeadMatch(idx, value);
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+1 : trimTail);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        writer.writeIntegerUnsigned(valueSend);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriterBytesNoneOptional(ByteBuffer value) {
        writer.writeIntegerUnsigned(value.remaining()+1);
        writer.writeByteArrayData(value); //this moves the position in value
    }

    protected void genWriteBytesDefault(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
            value.position(value.limit());//skip over the data just like we wrote it.
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(value.remaining());
            writer.writeByteArrayData(value); //this moves the position in value
        }
    }

    protected void genWriteBytesCopy(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        //System.err.println("AA");
        if (byteHeap.equals(idx, value)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
            value.position(value.limit());//skip over the data just like we wrote it.
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(value.remaining());
            byteHeap.set(idx, value);//position is NOT modified
            writer.writeByteArrayData(value); //this moves the position in value
        }
    }

    protected void genWriteBytesDelta(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value);
        int tailCount = byteHeap.countTailMatch(idx, value);
        if (headCount>tailCount) {
            int trimTail = byteHeap.length(idx)-headCount;
            if (trimTail<0) {
                throw new ArrayIndexOutOfBoundsException();
            }
            writer.writeIntegerUnsigned(trimTail>=0? trimTail+0 : trimTail);
            
            int valueSend = value.remaining()-headCount;
            int startAfter = value.position()+headCount;
                    
            writer.writeIntegerUnsigned(valueSend);
            //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
            byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
            writer.writeByteArrayData(value, startAfter, valueSend); //does not modify position
        } else {
            //replace head, tail matches to tailCount
            int trimHead = byteHeap.length(idx)-tailCount;
            writer.writeIntegerSigned(trimHead==0? 0: -trimHead); 
            
            int len = value.remaining() - tailCount;
            int offset = value.position();
            writer.writeIntegerUnsigned(len);
            writer.writeByteArrayData(value, offset, len);
            byteHeap.appendHead(idx, trimHead, value, offset, len); //does not modify position
        }
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriteBytesTail(int token, ByteBuffer value) {
        int idx = token & instanceBytesMask;
        int headCount = byteHeap.countHeadMatch(idx, value);
                
        int trimTail = byteHeap.length(idx)-headCount;
        if (trimTail<0) {
            throw new ArrayIndexOutOfBoundsException();
        }
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+0 : trimTail);
        
        int valueSend = value.remaining()-headCount;
        int startAfter = value.position()+headCount;
                
        writer.writeIntegerUnsigned(valueSend);
        //System.err.println("tail send:"+valueSend+" for headCount "+headCount);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriteBytesNone(ByteBuffer value) {
        writer.writeIntegerUnsigned(value.remaining());
        writer.writeByteArrayData(value); //this moves the position in value
    }
    
    protected void genWriteBytesDefault(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(length);
            writer.writeByteArrayData(value,offset,length);
        }
    }

    protected void genWriteBytesCopy(int token, byte[] value, int offset, int length) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        }
        else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(length);
            writer.writeByteArrayData(value,offset,length);
            byteHeap.set(idx, value, offset, length);
        }
    }

    public void genWriteBytesDelta(byte[] value, int offset, int length, int idx, PrimitiveWriter writer, ByteHeap byteHeap) {
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = byteHeap.countTailMatch(idx, value, offset+length, length);
        if (headCount>tailCount) {
            writeBytesTail(idx, headCount, value, offset+headCount, length, 0, writer, byteHeap);
        } else {
            writeBytesHead(idx, tailCount, value, offset, length, 0, writer, byteHeap);
        }
    }

    public void genWriteBytesTail(int idx, byte[] value, int offset, int length, PrimitiveWriter writer,
            ByteHeap byteHeap) {
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        
        int trimTail = byteHeap.length(idx)-headCount;
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+0: trimTail);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        writer.writeIntegerUnsigned(valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
    }

    protected void genWriteBytesNone(byte[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(length);
        writer.writeByteArrayData(value,offset,length);
    }
    
    private void writeBytesHead(int idx, int tailCount, byte[] value, int offset, int length, int opt, PrimitiveWriter writer, ByteHeap byteHeap) {
        
        //replace head, tail matches to tailCount
        int trimHead = byteHeap.length(idx)-tailCount;
        writer.writeIntegerSigned(trimHead==0? opt: -trimHead); 
        
        int len = length - tailCount;
        writer.writeIntegerUnsigned(len);
        writer.writeByteArrayData(value, offset, len);
        
        byteHeap.appendHead(idx, trimHead, value, offset, len);
    }
    
   private void writeBytesTail(int idx, int headCount, byte[] value, int offset, int length, final int optional, PrimitiveWriter writer, ByteHeap byteHeap) {
        int trimTail = byteHeap.length(idx)-headCount;
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+optional: trimTail);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        writer.writeIntegerUnsigned(valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
    }
    
    public void genWriteBytesDefaultOptional(byte[] value, int offset, int length, int idx, PrimitiveWriter writer, ByteHeap byteHeap) {
        if (byteHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(length+1);
            writer.writeByteArrayData(value,offset,length);
        }
    }

    public void genWriteBytesCopyOptional(byte[] value, int offset, int length, int idx, PrimitiveWriter writer, ByteHeap byteHeap) {
        if (byteHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(length+1);
            writer.writeByteArrayData(value,offset,length);
            byteHeap.set(idx, value, offset, length);
        }
    }

    public void genWriteBytesDeltaOptional(byte[] value, int offset, int length, int idx, PrimitiveWriter writer, ByteHeap byteHeap) {
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        int tailCount = byteHeap.countTailMatch(idx, value, offset+length, length);
        if (headCount>tailCount) {
            int trimTail = byteHeap.length(idx)-headCount;
            writer.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail);
            
            int valueSend = length-headCount;
            int startAfter = offset+headCount;
            
            writer.writeIntegerUnsigned(valueSend);
            writer.writeByteArrayData(value, startAfter, valueSend);
            byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
        } else {
            //replace head, tail matches to tailCount
            int trimHead = byteHeap.length(idx)-tailCount;
            writer.writeIntegerSigned(trimHead==0? 1: -trimHead); 
            
            int len = length - tailCount;
            writer.writeIntegerUnsigned(len);
            writer.writeByteArrayData(value, offset, len);
            
            byteHeap.appendHead(idx, trimHead, value, offset, len);
        }
    }

    protected void genWriteBytesConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte)1, writer);
        //the writeNull will take care of the rest.
    }

    public void genWriteBytesTailOptional(byte[] value, int offset, int length, int idx, PrimitiveWriter writer, ByteHeap byteHeap) {
        int headCount = byteHeap.countHeadMatch(idx, value, offset, length);
        int trimTail = byteHeap.length(idx)-headCount;
        writer.writeIntegerUnsigned(trimTail>=0? trimTail+1: trimTail);
        
        int valueSend = length-headCount;
        int startAfter = offset+headCount;
        
        writer.writeIntegerUnsigned(valueSend);
        writer.writeByteArrayData(value, startAfter, valueSend);
        byteHeap.appendTail(idx, trimTail, value, startAfter, valueSend);
    }

    protected void genWriteBytesNoneOptional(byte[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(length+1);
        writer.writeByteArrayData(value,offset,length);
    }
    
    protected void genWriteIntegerSignedDefault(int value, int constDefault, PrimitiveWriter writer) {
        writer.writeIntegerSignedDefault(value, constDefault);
    }

    protected void genWriteIntegerSignedIncrement(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerSignedIncrement(value, target, source, intValues);
    }

    protected void genWriteIntegerSignedCopy(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerSignedCopy(value, target, source, intValues);
    }

    protected void genWriteIntegerSignedDelta(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerSignedDelta(value, target, source, intValues);
    }

    protected void genWriteIntegerSignedNone(int value, int idx, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerSigned(intValues[idx] = value);
    }
    
    protected void genWriteIntegerUnsignedDefault(int value, int constDefault, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedDefault(value, constDefault);
    }

    protected void genWriteIntegerUnsignedIncrement(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerUnsignedIncrement(value, target, source, intValues);
    }

    protected void genWriteIntegerUnsignedCopy(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerUnsignedCopy(value, target, source, intValues);
    }

    protected void genWriteIntegerUnsignedDelta(int value, int idx, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerUnsignedDelta(value, idx, source, intValues);
    }

    protected void genWriteIntegerUnsignedNone(int value, int idx, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerUnsigned(intValues[idx] = value);
    }

    protected void genWriteIntegerSignedDefaultOptional(int value, int constDefault, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;// room for null
        }
        writer.writeIntegerSignedDefaultOptional(value, constDefault);
    }

    protected void genWriteIntegerSignedIncrementOptional(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        if (value >= 0) {
            value++;
        }
        int last = intValues[source];
        intValues[target] = value;
        writer.writeIntegerSignedIncrementOptional(value, last);
    }

    protected void genWriteIntegerSignedCopyOptional(int value, int idx, int source, PrimitiveWriter writer, int[] intValues) {
        if (value >= 0) {
            value++;
        }
        
        writer.writeIntegerSignedCopyOptional(value, idx, source, intValues);
    }

    protected void genWriteIntegerSignedConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteIntegerSignedDeltaOptional(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerSignedDeltaOptional(value, target, source, intValues);
    }

    protected void genWriteIntegerSignedNoneOptional(int value, PrimitiveWriter writer) {
        writer.writeIntegerSignedOptional(value);
    }

    protected void genWriteIntegerUnsignedCopyOptional(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerUnsignedCopyOptional(value, target, source, intValues);
    }

    protected void genWriteIntegerUnsignedDefaultOptional(int value, int constDefault, PrimitiveWriter writer) {
        writer.writeIntegerUnsignedDefaultOptional(value, constDefault);
    }

    protected void genWriteIntegerUnsignedIncrementOptional(int value, int idx, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerUnsignedIncrementOptional(value, idx, source, intValues);
    }

    protected void genWriteIntegerUnsignedConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteIntegerUnsignedDeltaOptional(int value, int target, int source, PrimitiveWriter writer, int[] intValues) {
        writer.writeIntegerUnsignedDeltaOptional(value, target, source, intValues);
    }

    protected void genWriteIntegerUnsignedNoneOptional(int value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value + 1);
    }

    protected void genWriteLongUnsignedDefault(long value, long constDefault, PrimitiveWriter writer) {
        writer.writeLongUnsignedDefault(value, constDefault);
    }

    protected void genWriteLongUnsignedIncrement(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongUnsignedIncrement(value, target, source, longValues);
    }

    protected void genWriteLongUnsignedCopy(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongUnsignedCopy(value, target, source, longValues);
    }

    protected void genWriteLongUnsignedDelta(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongSigned(value - longValues[source]);
        longValues[target] = value;
    }

    protected void genWriteLongUnsignedNone(long value, int idx, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongUnsigned(longValues[idx] = value);
    }
    
    protected void genWriteLongUnsignedDefaultOptional(long value, long constDefault, PrimitiveWriter writer) {
        writer.writneLongUnsignedDefaultOptional(value, constDefault);
    }

    protected void genWriteLongUnsignedIncrementOptional(long value, int idx, int source, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongUnsignedIncrementOptional(value, idx, source, longValues);
    }

    protected void genWriteLongUnsignedCopyOptional(long value, int idx, int source, PrimitiveWriter writer, long[] longValues) {
        value++;// zero is held for null
        
        writer.writeLongUnsignedCopyOptional(value, idx, source, longValues);
    }

    protected void genWriteLongUnsignedConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }

    protected void genWriteLongUnsignedNoneOptional(long value, PrimitiveWriter writer) {
        writer.writeLongUnsigned(value + 1);
    }

    protected void genWriteLongUnsignedDeltaOptional(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        long delta = value - longValues[source];
        writer.writeLongSigned(delta>=0 ? 1+delta : delta);
        longValues[target] = value;
    }
    
    protected void genWriteLongSignedDefault(long value, long constDefault, PrimitiveWriter writer) {
        writer.writeLongSignedDefault(value, constDefault);
    }

    protected void genWriteLongSignedIncrement(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongSignedIncrement(value,  longValues[source]);
        longValues[target] = value;
    }

    protected void genWriteLongSignedCopy(long value, int idx, int source, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongSignedCopy(value, idx, source, longValues);
    }

    protected void genWriteLongSignedNone(long value, int idx, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongSigned(longValues[idx] = value);
    }

    protected void genWriteLongSignedDelta(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        writer.writeLongSigned(value - longValues[source]);
        longValues[target] = value;
    }
    
    protected void genWriteLongSignedOptional(long value, PrimitiveWriter writer) {
        writer.writeLongSignedOptional(value);
    }

    protected void genWriteLongSignedDeltaOptional(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        long delta = value - longValues[source];
        writer.writeLongSigned(((delta + (delta >>> 63)) + 1));
        longValues[target] = value;
    }

    protected void genWriteLongSignedConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
    }
    

    protected void genWriteLongSignedCopyOptional(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        if (value >= 0) {
            value++;
        }
        writer.writeLongSignedCopy(value, target, source, longValues);
    }

    protected void genWriteLongSignedIncrementOptional(long value, int target, int source, PrimitiveWriter writer, long[] longValues) {
        if (value >= 0) {
            value++;
        }
        writer.writeLongSignedIncrementOptional(value, longValues[source]);
        longValues[target] = value;
    }

    protected void genWriteLongSignedDefaultOptional(long value, long constDefault, PrimitiveWriter writer) {
        if (value >= 0) {
            value++;// room for null
        }
        writer.writeLongSignedDefault(value, constDefault);
    }

    protected void genWriteDictionaryBytesReset(int idx, ByteHeap byteHeap) {
        byteHeap.setNull(idx);
    }

    protected void genWriteDictionaryTextReset(int idx, TextHeap textHeap) {
        textHeap.reset(idx);
    }

    protected void genWriteDictionaryLongReset(int idx, long constValue, long[] longValues) {
        longValues[idx] = constValue;
    }

    protected void genWriteDictionaryIntegerReset(int idx, int constValue, int[] intValues) {
        intValues[idx] = constValue;
    }
    
    protected void genWriteClosePMap(PrimitiveWriter writer) {
        writer.closePMap();
    }

    protected void genWriteCloseTemplatePMap(PrimitiveWriter writer, FASTWriterDispatchBase dispatch) {
        writer.closePMap();
        // must always pop because open will always push
        dispatch.templateStackHead--;
    }

    protected void genWriteCloseTemplate(FASTWriterDispatchBase dispatch) {
        // must always pop because open will always push
        dispatch.templateStackHead--;
    }
    
    // must happen just before Group so the Group in question must always have
    // an outer group.
    protected void pushTemplate(int templateId, FASTWriterDispatchBase dispatch, PrimitiveWriter writer) {
        int top = dispatch.templateStack[dispatch.templateStackHead];
        if (top == templateId) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeIntegerUnsigned(templateId);
            top = templateId;
        }

        dispatch.templateStack[dispatch.templateStackHead++] = top;
    }
    
    protected void genWriteOpenTemplate(int templateId, PrimitiveWriter writer) {
        // done here for safety to ensure it is always done at group open.
        pushTemplate(templateId, this, writer);
    }

    protected void genWriteOpenTemplatePMap(int templateId, int pmapSize, PrimitiveWriter writer) {
        writer.openPMap(pmapSize);
        // done here for safety to ensure it is always done at group open.
        pushTemplate(templateId, this, writer);
    }
    
    protected void genWriteOpenGroup(int pmapSize, PrimitiveWriter writer) {
        writer.openPMap(pmapSize);
    }
    
    public void genWriteNullPMap(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 0, writer);
    }

    public void genWriteNullDefaultInt(PrimitiveWriter writer, int[] dictionary, int idx) {
        if (dictionary[idx] == 0) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeNull();
        }
    }

    public void genWriteNullCopyIncInt(PrimitiveWriter writer, int[] dictionary, int idx) {
        if (0 == dictionary[idx]) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            dictionary[idx] = 0;
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeNull();
        }
    }

    public void genWriteNullNoPMapInt(PrimitiveWriter writer, int[] dictionary, int idx) {
        dictionary[idx] = 0;
        writer.writeNull();
    }

    public void genWriteNullDefaultLong(PrimitiveWriter writer, long[] dictionary, int idx) {
        if (dictionary[idx] == 0) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeNull();
        }
    }

    public void genWriteNullCopyIncLong(PrimitiveWriter writer, long[] dictionary, int idx) {
        if (0 == dictionary[idx]) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            dictionary[idx] = 0;
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeNull();
        }
    }

    public void genWriteNullNoPMapLong(PrimitiveWriter writer, long[] dictionary, int idx) {
        dictionary[idx] = 0;
        writer.writeNull();
    }
    
    public void genWriteNullDefaultText(int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.isNull(idx)) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeNull();
        }
    }

    public void genWriteNullCopyIncText(int idx, PrimitiveWriter writer, TextHeap textHeap) {
        if (textHeap.isNull(idx)) { // stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeNull();
            textHeap.setNull(idx);
        }
    }

    public void genWriteNullNoPMapText(int idx, PrimitiveWriter writer, TextHeap textHeap) {
        writer.writeNull();
        textHeap.setNull(idx);
    }
    
    public void genWriteNullDefaultBytes(int token, PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask) {
        if (byteHeap.isNull(token & instanceMask)) { //stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeNull();
        }
    }

    public void genWriteNullNoPMapBytes(int token, PrimitiveWriter writer, ByteHeap byteHeap, int instanceMask) {
        writer.writeNull();
        byteHeap.setNull(token & instanceMask);
    }

    public void genWriteNullCopyIncBytes(PrimitiveWriter writer, ByteHeap byteHeap, int idx) {
        if (byteHeap.isNull(idx)) { //stored value was null;
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeNull();
            byteHeap.setNull(idx);
        }
    }
}
