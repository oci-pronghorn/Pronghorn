package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;

public class StreamingReadVisitorMatcher extends StreamingReadVisitorAdapter {

    private final RingBuffer expectedInput;
    private final FieldReferenceOffsetManager expectedFrom;
    private boolean needsClose = false;
    
    public StreamingReadVisitorMatcher(RingBuffer expectedInput) {
        this.expectedInput = expectedInput;
        this.expectedFrom = RingBuffer.from(expectedInput);
    }

    @Override
    public boolean paused() {
        return !RingBuffer.contentToLowLevelRead(expectedInput, 1);
    }

    @Override
    public void visitTemplateOpen(String name, long id) {
        needsClose = true;
        
        int msgIdx = RingBuffer.takeMsgIdx(expectedInput);
        
        if (id != expectedFrom.fieldIdScript[msgIdx]) {
            throw new AssertionError("expected message id: "+expectedFrom.fieldIdScript[msgIdx]+" was given "+id);
        }
        
        
    }

    @Override
    public void visitTemplateClose(String name, long id) {
        if (needsClose) {
            needsClose = false;
            RingBuffer.releaseReads(expectedInput);
        }
        
    }


    @Override
    public void visitFragmentOpen(String name, long id, int cursor) {
        needsClose = true;
    }

    @Override
    public void visitFragmentClose(String name, long id) {
        if (needsClose) {
            needsClose = false;            
            RingBuffer.releaseReads(expectedInput);
        }  
    }

        
    @Override
    public void visitSequenceOpen(String name, long id, int length) {

        int tempLen;
        if ((tempLen=RingBuffer.takeValue(expectedInput))!=length) {
            throw new AssertionError("expected length: "+Long.toHexString(tempLen)+" but got "+Long.toHexString(length));
        }
        needsClose = false;
        RingBuffer.releaseReads(expectedInput);
    }

    @Override
    public void visitSequenceClose(String name, long id) {
    }

    @Override
    public void visitSignedInteger(String name, long id, int value) { 
        needsClose = true;
        if (RingBuffer.takeValue(expectedInput) != value) {
            throw new AssertionError();
        }
    }

    @Override
    public void visitUnsignedInteger(String name, long id, long value) {
        needsClose = true;
        int temp;
        if ((temp = RingBuffer.takeValue(expectedInput)) != value) {
            throw new AssertionError("expected integer "+temp+" but found "+value);
        }
    }

    @Override
    public void visitSignedLong(String name, long id, long value) {
        needsClose = true;
        if (RingBuffer.takeLong(expectedInput) != value) {
            throw new AssertionError();
        }
    }

    @Override
    public void visitUnsignedLong(String name, long id, long value) {
        needsClose = true;
        
        long temp;
        if ((temp=RingBuffer.takeLong(expectedInput)) != value) {
            throw new AssertionError("expected long: "+Long.toHexString(temp)+" but got "+Long.toHexString(value));
        }
    }

    @Override
    public void visitDecimal(String name, long id, int exp, long mant) {
        needsClose = true;
        int tempExp;
        if ((tempExp = RingBuffer.takeValue(expectedInput)) != exp) {
            throw new AssertionError("expected integer exponent "+tempExp+" but found "+exp);
        }
        long tempMant;
        if ((tempMant=RingBuffer.takeLong(expectedInput)) != mant) {
            throw new AssertionError("expected long mantissa: "+Long.toHexString(tempMant)+" but got "+Long.toHexString(mant));
        }
    }

    @Override
    public void visitASCII(String name, long id, Appendable value) {
        needsClose = true;
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);               
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = byteMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion
        
        //ascii so the bytes will match the chars
        CharSequence seq = (CharSequence)value;
        
        if (seq.length() != len) {
            throw new AssertionError("expected ASCII length: "+Long.toHexString(len)+" but got "+Long.toHexString(seq.length()));
        }
        
        int i = 0;
        while (i<len) {
            byte actual = (byte)seq.charAt(i);
            byte expected = data[mask&(i+pos)];
            if (actual != expected) {
                throw new AssertionError("ASCII does not match at index "+i+" of length "+len);
                
            }
            i++;
        }
        
    }

    @Override
    public void visitUTF8(String name, long id, Appendable value) {
        needsClose = true;
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);               
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = byteMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion
        
        CharSequence seq = (CharSequence)value;
        int seqPos = 0;
        
        //we must check that seq equals the text encoded in data and if they do not match throw an AssertionError.        
        long charAndPos = ((long)pos)<<32;
        long limit = ((long)pos+len)<<32;

        while (charAndPos<limit) {
            charAndPos = RingBuffer.decodeUTF8Fast(data, charAndPos, mask);
            char expectedChar = (char)charAndPos;
            if (seq.charAt(seqPos++)!=expectedChar) {
                throw new AssertionError("UTF8 does not match at char index "+(seqPos-1)+" of length "+seq.length());
            }
        }
    }

    @Override
    public void visitBytes(String name, long id, ByteBuffer value) {
        needsClose = true;
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);               
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = byteMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion
        
        if (value.remaining() != len) {
            throw new AssertionError("expected bytes length: "+Long.toHexString(len)+" but got "+Long.toHexString(value.remaining()));
        }
        
        int i = 0;
        while (i<len) {
            byte actual = value.get(i+value.position());
            byte expected = data[mask&(i+pos)];
            if (actual != expected) {
                throw new AssertionError("ASCII does not match at index "+i+" of length "+len);
                
            }
            i++;
        }

    }

    @Override
    public void startup() {
    }

    @Override
    public void shutdown() {
    }

}
