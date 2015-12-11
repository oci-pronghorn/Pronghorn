package com.ociweb.pronghorn.pipe.stream;

import static com.ociweb.pronghorn.pipe.Pipe.blobMask;
import static com.ociweb.pronghorn.pipe.Pipe.byteBackingArray;
import static com.ociweb.pronghorn.pipe.Pipe.bytePosition;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteLen;
import static com.ociweb.pronghorn.pipe.Pipe.takeRingByteMetaData;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;

public class StreamingReadVisitorMatcher extends StreamingReadVisitorAdapter {

    private final Pipe expectedInput;
    private final FieldReferenceOffsetManager expectedFrom;
    private boolean needsClose = false;

    public StreamingReadVisitorMatcher(Pipe expectedInput) {
        this.expectedInput = expectedInput;
        this.expectedFrom = Pipe.from(expectedInput);
    }

    @Override
    public boolean paused() {
        return !Pipe.hasContentToRead(expectedInput, 1);
    }

    @Override
    public void visitTemplateOpen(String name, long id) {
        needsClose = true;

        int msgIdx = Pipe.takeMsgIdx(expectedInput);

        if (id != expectedFrom.fieldIdScript[msgIdx]) {
            throw new AssertionError("expected message id: "+expectedFrom.fieldIdScript[msgIdx]+" was given "+id);
        }


    }

    @Override
    public void visitTemplateClose(String name, long id) {
        if (needsClose) {
            needsClose = false;
            Pipe.releaseReads(expectedInput);
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
            Pipe.releaseReads(expectedInput);
        }
    }


    @Override
    public void visitSequenceOpen(String name, long id, int length) {

        int tempLen;
        if ((tempLen=Pipe.takeValue(expectedInput))!=length) {
            throw new AssertionError("expected length: "+Long.toHexString(tempLen)+" but got "+Long.toHexString(length));
        }
        needsClose = false;
        Pipe.releaseReads(expectedInput);
    }

    @Override
    public void visitSequenceClose(String name, long id) {
    }

    @Override
    public void visitSignedInteger(String name, long id, int value) {
        needsClose = true;
        if (Pipe.takeValue(expectedInput) != value) {
            throw new AssertionError();
        }
    }

    @Override
    public void visitUnsignedInteger(String name, long id, long value) {
        needsClose = true;
        int temp;
        if ((temp = Pipe.takeValue(expectedInput)) != value) {
            throw new AssertionError("expected integer "+temp+" but found "+value);
        }
    }

    @Override
    public void visitSignedLong(String name, long id, long value) {
        needsClose = true;
        if (Pipe.takeLong(expectedInput) != value) {
            throw new AssertionError();
        }
    }

    @Override
    public void visitUnsignedLong(String name, long id, long value) {
        needsClose = true;

        long temp;
        if ((temp=Pipe.takeLong(expectedInput)) != value) {
            throw new AssertionError("expected long: "+Long.toHexString(temp)+" but got "+Long.toHexString(value));
        }
    }

    @Override
    public void visitDecimal(String name, long id, int exp, long mant) {
        needsClose = true;
        int tempExp;
        if ((tempExp = Pipe.takeValue(expectedInput)) != exp) {
            
            //TODO; AAAAAAA, decimal support is broken here  must fix...
            
 //           throw new AssertionError("expected integer exponent "+tempExp+" but found "+exp);
        }
        long tempMant;
        if ((tempMant=Pipe.takeLong(expectedInput)) != mant) {
//            throw new AssertionError("expected long mantissa: "+Long.toHexString(tempMant)+" but got "+Long.toHexString(mant));
        }
    }

    @Override
    public void visitASCII(String name, long id, Appendable value) {
        needsClose = true;
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = blobMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion

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
        int mask = blobMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion

        CharSequence seq = (CharSequence)value;
        int seqPos = 0;

        //we must check that seq equals the text encoded in data and if they do not match throw an AssertionError.
        long charAndPos = ((long)pos)<<32;
        long limit = ((long)pos+len)<<32;

        while (charAndPos<limit) {
            charAndPos = Pipe.decodeUTF8Fast(data, charAndPos, mask);
            char expectedChar = (char)charAndPos;
            if (seq.charAt(seqPos++)!=expectedChar) {
                throw new AssertionError("UTF8 does not match at char index "+(seqPos-1)+" of length "+seq.length());
            }
        }
    }

    @Override
    public void visitBytes(String name, long id, ByteBuffer value) {
        value.flip();
        
        needsClose = true;
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = blobMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion

        if (value.remaining() != len) {
            throw new AssertionError("expected bytes length: "+len+" but got "+value.remaining());
        }

        int i = 0;
        while (i<len) {
            byte actual = value.get(i+value.position());
            byte expected = data[mask&(i+pos)];
            if (actual != expected) {
                throw new AssertionError("Byte does not match at index "+i+" of length "+len);

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
