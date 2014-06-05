package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.StaticGlue;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveWriter;


public class FASTWriterDispatchTemplates extends FASTEncoder {

    public FASTWriterDispatchTemplates(final TemplateCatalogConfig catalog, FASTRingBuffer[] ringBuffers) {
        
        super(catalog.dictionaryFactory(), catalog.templatesCount(),
              catalog.maxNonTemplatePMapSize(), catalog.dictionaryResetMembers(),
              catalog.fullScript(), catalog.getMaxGroupDepth(), ringBuffers);
        
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

    protected void genWriteTextDeltaOptional2(int token, char[] value, int offset, int length, TextHeap textHeap, PrimitiveWriter writer) {
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
    //TODO: A, write templateId and dispatch instance in leading integer. If value is bad the dispatch can be reset.
    
    protected void genWriteTextConstantOptional(PrimitiveWriter writer) {
        PrimitiveWriter.writePMapBit((byte) 1, writer);
        // the writeNull will take care of the rest.
    }

    protected void genWriteTextTailOptional2(int token, char[] value, int offset, int length, PrimitiveWriter writer, TextHeap textHeap) {
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

    protected void genWriteTextNoneOptional(char[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeTextASCII(value, offset, length);
    }
    
    protected void genWriteTextDefault2(int token, char[] value, int offset, int length, TextHeap textHeap, PrimitiveWriter writer) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        if (textHeap.equals(idx | FASTWriterInterpreterDispatch.INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
        }
    }

    protected void genWriteTextCopy2(int token, char[] value, int offset, int length, TextHeap textHeap, PrimitiveWriter writer) {
        int idx = token & TEXT_INSTANCE_MASK;
        
        if (textHeap.equals(idx, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte) 0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte) 1, writer);
            writer.writeTextASCII(value, offset, length);
            textHeap.set(idx, value, offset, length);
        }
    }

    protected void genWriteTextDelta2(int token, char[] value, int offset, int length, PrimitiveWriter writer, TextHeap textHeap) {
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

    protected void genWriteTextTail2(int token, char[] value, int offset, int length, PrimitiveWriter writer, TextHeap textHeap) {
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

    protected void genWriteTextNone(char[] value, int offset, int length, PrimitiveWriter writer) {
        writer.writeTextASCII(value, offset, length);
    }
    
    protected void genWriterBytesDefaultOptional(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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

    protected void genWriterBytesCopyOptional(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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

    protected void genWriterBytesDeltaOptional(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
        int idx = token & instanceBytesMask;
        
        //count matching front or back chars
        int headCount = byteHeap.countHeadMatch(idx, value);
        int tailCount = byteHeap.countTailMatch(idx, value);
        if (headCount>tailCount) {
            StaticGlue.writeBytesTail(idx, headCount, value, 1, byteHeap, writer); //does not modify position
        } else {
            StaticGlue.writeBytesHead(idx, tailCount, value, 1, byteHeap, writer); //does not modify position
        }
        value.position(value.limit());//skip over the data just like we wrote it.
    }

    protected void genWriterBytesTailOptional(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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

    protected void genWriterBytesNoneOptional(ByteBuffer value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value.remaining()+1);
        writer.writeByteArrayData(value); //this moves the position in value
    }

    protected void genWriteBytesDefault(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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

    protected void genWriteBytesCopy(int token, ByteBuffer value, ByteHeap byteHeap, PrimitiveWriter writer) {
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

    protected void genWriteBytesDelta(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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

    protected void genWriteBytesTail(int token, ByteBuffer value, PrimitiveWriter writer, ByteHeap byteHeap) {
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

    protected void genWriteBytesNone(ByteBuffer value, PrimitiveWriter writer) {
        writer.writeIntegerUnsigned(value.remaining());
        writer.writeByteArrayData(value); //this moves the position in value
    }
    
    protected void genWriteBytesDefault(int token, byte[] value, int offset, int length, ByteHeap byteHeap, PrimitiveWriter writer) {
        int idx = token & instanceBytesMask;
        
        if (byteHeap.equals(idx|INIT_VALUE_MASK, value, offset, length)) {
            PrimitiveWriter.writePMapBit((byte)0, writer);
        } else {
            PrimitiveWriter.writePMapBit((byte)1, writer);
            writer.writeIntegerUnsigned(length);
            writer.writeByteArrayData(value,offset,length);
        }
    }

    protected void genWriteBytesCopy(int token, byte[] value, int offset, int length, ByteHeap byteHeap, PrimitiveWriter writer) {
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
    
    protected void genWriteIntegerSignedDefault(int constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerSignedDefault(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), constDefault);
    }

    protected void genWriteIntegerSignedIncrement(int target, int source, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerSignedIncrement(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues);
    }

    protected void genWriteIntegerSignedCopy(int target, int source, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerSignedCopy(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues);
    }

    protected void genWriteIntegerSignedDelta(int target, int source, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerSignedDelta(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues);
    }

    protected void genWriteIntegerSignedNone(int target, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerSigned(intValues[target] = FASTRingBufferReader.readInt(rbRingBuffer, rbPos));
    }
    
    protected void genWriteIntegerUnsignedDefault(int constDefault, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerUnsignedDefault(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), constDefault);
    }

    protected void genWriteIntegerUnsignedIncrement( int target, int source, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerUnsignedIncrement(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues);
    }

    protected void genWriteIntegerUnsignedCopy(int target, int source, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerUnsignedCopy(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues);
    }

    protected void genWriteIntegerUnsignedDelta(int target, int source, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerUnsignedDelta(FASTRingBufferReader.readInt(rbRingBuffer, rbPos), target, source, intValues);
    }

    protected void genWriteIntegerUnsignedNone(int target, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        writer.writeIntegerUnsigned(intValues[target] = FASTRingBufferReader.readInt(rbRingBuffer, rbPos));
    }

    protected void genWriteIntegerSignedDefaultOptional(int source, int constDefault, int valueOfNull, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullDefaultInt(writer, intValues, source); // null for default 
            } else {
                writer.writeIntegerSignedDefaultOptional(value>=0?value+1:value, constDefault);
            }
        }
    }

    protected void genWriteIntegerSignedIncrementOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {//TODO: C, at generation time the valueOfNull can be replaced by constant so 0 optimization can take place
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                int last = intValues[source];
                writer.writeIntegerSignedIncrementOptional(intValues[target] = (value>=0?value+1:value), last);  
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
                writer.writeIntegerSignedCopyOptional(value>=0?value+1:value, target, source, intValues);
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
                writer.writeIntegerSignedDeltaOptional(value, target, source, intValues);
            }
        }
    }

    protected void genWriteIntegerSignedNoneOptional(int target, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                writer.writeIntegerSignedOptional(value);
            }
        }
    }

    protected void genWriteIntegerUnsignedCopyOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer, rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                writer.writeIntegerUnsignedCopyOptional(value, target, source, intValues);
            }
        }
    }

    protected void genWriteIntegerUnsignedDefaultOptional(int source, int valueOfNull, int constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullDefaultInt(writer, intValues, source); // null for default 
            } else {
                writer.writeIntegerUnsignedDefaultOptional(value, constDefault);
            }
        }
    }

    protected void genWriteIntegerUnsignedIncrementOptional(int target, int source, int valueOfNull, PrimitiveWriter writer, int[] intValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullCopyIncInt(writer, intValues, source, target);// null for Copy and Increment 
            } else { 
                writer.writeIntegerUnsignedIncrementOptional(value, target, source, intValues);
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
                writer.writeIntegerUnsignedDeltaOptional(value, target, source, intValues);
            }
        }
    }

    protected void genWriteIntegerUnsignedNoneOptional(int target, int valueOfNull, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        {
            int value = FASTRingBufferReader.readInt(rbRingBuffer,rbPos);
            if (valueOfNull == value) {
                StaticGlue.nullNoPMapInt(writer, intValues, target);// null for None and Delta (both do not use pmap)
            } else {
                writer.writeIntegerUnsigned(value + 1);
            }
        }
    }
    
    ////////////////////////
    ///Decimals with optional exponent
    /////////////////////////

      protected void genWriteDecimalDefaultOptionalNone(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer, long[] longValues, int[] intValues) {
      {
        int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
        if (exponentValueOfNull == exponentValue) {
            StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
        } else {
            writer.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault);
            writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
            if (exponentValueOfNull == exponentValue) {//TODO: C, at generation time the valueOfNull can be replaced by constant so 0 optimization can take place
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else { 
                int last = intValues[exponentSource];
                writer.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last); 
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalNone(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else {        
                writer.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues);
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
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
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos));        
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalNone(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                writer.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues);
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
            }
        }
    }

    protected void genWriteDecimalNoneOptionalNone(int exponentTarget, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer, long[] longValues) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                writer.writeIntegerSignedOptional(exponentValue);
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
            }
        }
    }
    
 /*   
   
          protected void genWriteDecimalDefaultOptionalDefault(int exponentSource, int mantissaTarget, int exponentConstDefault, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
      {
        int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
        if (exponentValueOfNull == exponentValue) {
            StaticGlue.nullDefaultInt(writer, intValues, exponentSource); // null for default 
        } else {
            writer.writeIntegerSignedDefaultOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentConstDefault);
            
            //writer.writeLongSignedDefault(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), constDefault);
            
            writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
        }
      }
    }

    protected void genWriteDecimalIncrementOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos);  
            if (exponentValueOfNull == exponentValue) {//TODO: C, at generation time the valueOfNull can be replaced by constant so 0 optimization can take place
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else { 
                int last = intValues[exponentSource];
                writer.writeIntegerSignedIncrementOptional(intValues[exponentTarget] = (exponentValue>=0?exponentValue+1:exponentValue), last); 
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
            }   
        }
    }

    protected void genWriteDecimalCopyOptionalDefault(int exponentTarget, int exponentSource, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullCopyIncInt(writer, intValues, exponentSource, exponentTarget);// null for Copy and Increment 
            } else {        
                writer.writeIntegerSignedCopyOptional(exponentValue>=0?exponentValue+1:exponentValue, exponentTarget, exponentSource, intValues);
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
            }
        }
    }

    protected void genWriteDecimalConstantOptionalDefault(int exponentValueOfNull, int mantissaTarget, int rbPos, PrimitiveWriter writer, FASTRingBuffer rbRingBuffer) {
        { 
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull==exponentValue) {
                PrimitiveWriter.writePMapBit((byte)0, writer);  // 1 for const, 0 for absent
            } else {
                PrimitiveWriter.writePMapBit((byte)1, writer);  // 1 for const, 0 for absent
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos));        
            }     
        }
    }

    protected void genWriteDecimalDeltaOptionalDefault(int exponentTarget, int mantissaTarget, int exponentSource, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                writer.writeIntegerSignedDeltaOptional(exponentValue, exponentTarget, exponentSource, intValues);
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
            }
        }
    }

    protected void genWriteDecimalNoneOptionalDefault(int exponentTarget, int mantissaTarget, int exponentValueOfNull, int rbPos, PrimitiveWriter writer, int[] intValues, FASTRingBuffer rbRingBuffer) {
        {   
            int exponentValue = FASTRingBufferReader.readDecimalExponent(rbRingBuffer, rbPos); 
            if (exponentValueOfNull == exponentValue) {
                StaticGlue.nullNoPMapInt(writer, intValues, exponentTarget);// null for None and Delta (both do not use pmap)
            } else {
                writer.writeIntegerSignedOptional(exponentValue);
                writer.writeLongSigned(longValues[mantissaTarget] = FASTRingBufferReader.readDecimalMantissa(rbRingBuffer, rbPos)); 
            }
        }
    }
    
    
   
  */  
    
//TODO: B, must duplicate the code above for the other 5 types.
      
    
    //////////////
    //end of decimals
    ///////////////
    
    protected void genWriteLongUnsignedDefault(long constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {        
        writer.writeLongUnsignedDefault(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), constDefault);
    }

    protected void genWriteLongUnsignedIncrement(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeLongUnsignedIncrement(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), target, source, longValues);
    }

    protected void genWriteLongUnsignedCopy(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeLongUnsignedCopy(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), target, source, longValues);
    }

    protected void genWriteLongUnsignedDelta(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
        writer.writeLongSigned(value - longValues[source]);
        longValues[target] = value;
    }

    protected void genWriteLongUnsignedNone(int idx, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeLongUnsigned(longValues[idx] = FASTRingBufferReader.readLong(rbRingBuffer, rbPos));
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
    
    protected void genWriteLongSignedDefault(long constDefault, PrimitiveWriter writer, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeLongSignedDefault(FASTRingBufferReader.readLong(rbRingBuffer, rbPos), constDefault);
    }

    protected void genWriteLongSignedIncrement(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
        writer.writeLongSignedIncrement(value,  longValues[source]);
        longValues[target] = value;
    }

    protected void genWriteLongSignedCopy(int idx, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
        writer.writeLongSignedCopy(value, idx, source, longValues);
    }

    protected void genWriteLongSignedNone(int idx, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        writer.writeLongSigned(longValues[idx] = FASTRingBufferReader.readLong(rbRingBuffer, rbPos));
    }

    protected void genWriteLongSignedDelta(int target, int source, PrimitiveWriter writer, long[] longValues, int rbPos, FASTRingBuffer rbRingBuffer) {
        long value = FASTRingBufferReader.readLong(rbRingBuffer, rbPos);
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

    protected void genWriteCloseTemplatePMap(PrimitiveWriter writer, FASTEncoder dispatch) {
        writer.closePMap();
        // must always pop because open will always push
        dispatch.templateStackHead--;
    }

    protected void genWriteCloseTemplate(FASTEncoder dispatch) {
        // must always pop because open will always push
        dispatch.templateStackHead--;
    }
    
    // must happen just before Group so the Group in question must always have
    // an outer group.
    protected void pushTemplate(int templateId, FASTEncoder dispatch, PrimitiveWriter writer) {
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
        StaticGlue.nullPMap(writer);  // null for const optional
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
            textHeap.setNull(idx, textHeap);
        }
    }

    public void genWriteNullNoPMapText(int idx, PrimitiveWriter writer, TextHeap textHeap) {
        writer.writeNull();
        textHeap.setNull(idx, textHeap);
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
