package com.ociweb.pronghorn.ring;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class RingBufferConverterTest {

    final FieldReferenceOffsetManager FROM = FieldReferenceOffsetManager.RAW_BYTES;
    final int FRAG_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
    final int FRAG_FIELD = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
    
    @Test
    public void longToASCIITest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
                
        RingBuffer.validateVarLength(ring, 10);
        
        RingBuffer.addLongAsASCII(ring, 1234567890);
        RingBuffer.publishWrites(ring);
        
        int meta = RingBuffer.takeRingByteMetaData(ring);
        int len = RingBuffer.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        RingBuffer.readASCII(ring, target, meta, len);
        
        assertEquals("1234567890",target.toString());
                
    }
    
    @Test
    public void intToASCIITest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
        ring.reset(0,0);
        
        RingBuffer.validateVarLength(ring, 10);
                
        RingBuffer.addIntAsASCII(ring, 1234567890);
        RingBuffer.publishWrites(ring);
        
        int meta = RingBuffer.takeRingByteMetaData(ring);
        int len = RingBuffer.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        RingBuffer.readASCII(ring, target, meta, len);
        
        assertEquals("1234567890",target.toString());
                
    }
    
    @Test
    public void decimalToASCIITest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
        
        RingBuffer.validateVarLength(ring, 7);
                
        RingBuffer.addDecimalAsASCII(2, 123456, ring);
        RingBuffer.addBytePosAndLen(ring,0,7);
        
        RingBuffer.publishWrites(ring);
        
        int meta = RingBuffer.takeRingByteMetaData(ring);
        int len = RingBuffer.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        RingBuffer.readASCII(ring, target, meta, len);
        
        assertEquals("1234.56",target.toString());
        
        ring.reset();
        String ringToString = ring.toString();
        assertTrue(ringToString, ringToString.contains("headPos 0"));
        assertTrue(ringToString, ringToString.contains("tailPos 0"));

        
        RingBuffer.addDecimalAsASCII(2, 1, ring); 
        RingBuffer.addBytePosAndLen(ring,0,4);
        
        RingBuffer.publishWrites(ring);
        
        meta = RingBuffer.takeRingByteMetaData(ring);
        len = RingBuffer.takeRingByteLen(ring);
        
        target.setLength(0);
        RingBuffer.readASCII(ring, target, meta, len);
        
        assertEquals("0.01",target.toString());
                        
        ring.reset();
        ringToString = ring.toString();
        assertTrue(ringToString, ringToString.contains("headPos 0"));
        assertTrue(ringToString, ringToString.contains("tailPos 0"));
        
        RingBuffer.addDecimalAsASCII(-2, 1, ring);
        RingBuffer.addBytePosAndLen(ring,0,4);
        
        RingBuffer.publishWrites(ring);
        
        meta = RingBuffer.takeRingByteMetaData(ring);
        len = RingBuffer.takeRingByteLen(ring);
        
        target.setLength(0);
        RingBuffer.readASCII(ring, target, meta, len);
        
        assertEquals("100.",target.toString());
    }
    
    @Test
    public void addByteBufferTest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        RingBuffer ring = new RingBuffer(new RingBufferConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
        ring.reset(0,0);
        
        RingBuffer.validateVarLength(ring, 10);
                
        
        
        ByteBuffer source = ByteBuffer.allocate(100);
        source.put("HelloWorld".getBytes());
        source.flip();
        
        RingBuffer.addByteBuffer(source, ring);
        RingBuffer.publishWrites(ring);
        
        int meta = RingBuffer.takeRingByteMetaData(ring);
        int len = RingBuffer.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        RingBuffer.readASCII(ring, target, meta, len);
        
        assertEquals("HelloWorld",target.toString());
                
    }
    
    
}
