package com.ociweb.pronghorn.ring.stream;

import java.nio.ByteBuffer;
import java.util.Random;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.stream.StreamingWriteVisitor;

public class StreamingWriteVisitorGenerator implements StreamingWriteVisitor {

    private final Random random;
    private final int[] starts;
    private final char[] workingChar;
    private final int maxLenChars;
    private final int maxBytes;
    private boolean paused = true; //must start in true state so first iteration will toggle and run
    
    public StreamingWriteVisitorGenerator(FieldReferenceOffsetManager from, Random random, int maxChars, int maxBytes) {
        this.random = random;
        this.starts = from.messageStarts();
        this.maxLenChars = maxChars;
        this.maxBytes = maxBytes;
        this.workingChar = new char[maxLenChars];
        
        //TODO: need to add generation based on operators found in template file or bound
        
    }

    @Override
    public boolean paused() {        
        return paused = !paused;
    }

    @Override
    public int pullMessageIdx() {        
        return starts[random.nextInt(starts.length)];
    }

    @Override
    public boolean isAbsent(String name, long id) {       
        return random.nextBoolean();
    }

    @Override
    public long pullSignedLong(String name, long id) {
        return random.nextLong();
    }

    @Override
    public long pullUnsignedLong(String name, long id) {
        return Math.abs(random.nextLong());
    }

    @Override
    public int pullSignedInt(String name, long id) {      
        return random.nextInt();
    }

    @Override
    public int pullUnsignedInt(String name, long id) {
        return Math.abs(random.nextInt());
    }

    @Override
    public long pullDecimalMantissa(String name, long id) {
        return Math.abs(random.nextLong());
    }

    @Override
    public int pullDecimalExponent(String name, long id) {
        return random.nextInt(128)-64;
    }

    @Override
    public CharSequence pullASCII(String name, long id) {        
        int len = random.nextInt(maxLenChars);
        int i = len;
        while (--i>=0) {
            workingChar[i] = (char)('0'+random.nextInt(62));
        }        
        return new String(workingChar, 0, len);
    }

    @Override
    public CharSequence pullUTF8(String name, long id) {
        int len = random.nextInt(maxLenChars);
        int i = len;
        while (--i>=0) {
            workingChar[i] = (char)random.nextInt(32000);
        }        
        return new String(workingChar, 0, len);
    }

    @Override
    public ByteBuffer pullByteBuffer(String name, long id) {
        int len = random.nextInt(maxBytes);
        ByteBuffer result = ByteBuffer.allocate(len);        
        int i = len;
        while (--i>=0) {
            result.put((byte)random.nextInt(255));
        }
        return result;
    }

    @Override
    public int pullSequenceLength(String name, long id) {
        return Math.abs(0x7&random.nextInt());//7 need small sequences
    }

    @Override
    public void startup() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void templateClose(String name, long id) {
    }

    @Override
    public void sequenceClose(String name, long id) {
    }

    @Override
    public void fragmentClose(String name, long id) {
    }

    @Override
    public void fragmentOpen(String string, long l) {
    }

}
