package com.ociweb.pronghorn.ring.stream;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.ring.stream.StreamingReadVisitorAdapter;

public class StreamingReadVisitorMatcher extends StreamingReadVisitorAdapter {

    private final RingBuffer expectedInput;
    private final FieldReferenceOffsetManager expectedFrom;
    
    public StreamingReadVisitorMatcher(RingBuffer expectedInput) {
        this.expectedInput = expectedInput;
        this.expectedFrom = RingBuffer.from(expectedInput);
    }

    @Override
    public boolean paused() {
        return false;
    }

    @Override
    public void visitTemplateOpen(String name, long id) {
        
        while (!RingBuffer.contentToLowLevelRead(expectedInput, 1)) {            
        };
        int msgIdx = RingBuffer.takeMsgIdx(expectedInput);
        
        if (id != expectedFrom.fieldIdScript[msgIdx]) {
            throw new AssertionError("expected message id: "+expectedFrom.fieldIdScript[msgIdx]+" was given "+id);
        }
    }

    @Override
    public void visitTemplateClose(String name, long id) {
        endFragment();
    }

    private void endFragment() {
        if (expectedInput.readTrailCountOfBytesConsumed) {
          //has side effect of moving position
          int bytesConsumed = RingBuffer.takeValue(expectedInput);  //TODO: AAAA, need to remove once its part of releaseReadLock
          expectedInput.readTrailCountOfBytesConsumed = false;
        }
        
        //TODO: must confirm that we are at the right position to end this fragment
        
        RingBuffer.releaseReadLock(expectedInput);
    }

    @Override
    public void visitFragmentOpen(String name, long id, int cursor) {
        RingBuffer.mustReadMsgBytesConsumed(expectedInput, cursor);

        while (!RingBuffer.contentToLowLevelRead(expectedInput, 1)) {            
        };
        
       
        //TODO: the expectedInput needs to have the trailing value.
        
        
    }

    @Override
    public void visitFragmentClose(String name, long id) {
        endFragment();  
    }

    @Override
    public void visitSequenceOpen(String name, long id, int length) {
        if (RingBuffer.takeValue(expectedInput)!=length) {
            throw new AssertionError();
        };
    }

    @Override
    public void visitSequenceClose(String name, long id) {
        endFragment();
    }

    @Override
    public void visitSignedInteger(String name, long id, int value) {        
        if (RingBuffer.takeValue(expectedInput) != value) {
            throw new AssertionError();
        }
    }

    @Override
    public void visitUnsignedInteger(String name, long id, long value) {
        int temp;
        if ((temp = RingBuffer.takeValue(expectedInput)) != value) {
            throw new AssertionError("expected integer "+temp+" but found "+value);
        }
    }

    @Override
    public void visitSignedLong(String name, long id, long value) {
        if (RingBuffer.takeLong(expectedInput) != value) {
            throw new AssertionError();
        }
    }

    @Override
    public void visitUnsignedLong(String name, long id, long value) {
        long temp;
        if ((temp=RingBuffer.takeLong(expectedInput)) != value) {
            //TODO: the expected is WRONG and is looking at an index one too small.
            throw new AssertionError("expected long: "+Long.toHexString(temp)+" but got "+Long.toHexString(value));
        }
    }

    @Override
    public void visitDecimal(String name, long id, int exp, long mant) {
        if (RingBuffer.takeValue(expectedInput) != exp) {
            throw new AssertionError();
        }
        if (RingBuffer.takeLong(expectedInput) != mant) {
            throw new AssertionError();
        }
    }

    @Override
    public void visitASCII(String name, long id, Appendable value) {
        
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);               
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = byteMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion
        
        //ascii so the bytes will match the chars
        CharSequence seq = (CharSequence)value;
        
        
        
        // TODO Auto-generated method stub

    }

    @Override
    public void visitUTF8(String name, long id, Appendable target) {
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);               
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = byteMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion
        
        // TODO Auto-generated method stub

    }

    @Override
    public void visitBytes(String name, long id, ByteBuffer value) {
        int meta = takeRingByteMetaData(expectedInput);
        int len = takeRingByteLen(expectedInput);
        int pos = bytePosition(meta, expectedInput, len);               
        byte[] data = byteBackingArray(meta, expectedInput);
        int mask = byteMask(expectedInput);//NOTE: the consumer must do their own ASCII conversion
        
        
        
        // TODO Auto-generated method stub

    }

    @Override
    public void startup() {
    }

    @Override
    public void shutdown() {
    }

}
