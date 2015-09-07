package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;

public class RingBufferConverterTest {

    final FieldReferenceOffsetManager FROM = FieldReferenceOffsetManager.RAW_BYTES;
    final int FRAG_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
    final int FRAG_FIELD = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
    
    @Test
    public void longToASCIITest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
                
        Pipe.validateVarLength(ring, 10);
        
        Pipe.addLongAsASCII(ring, 1234567890);
        Pipe.publishWrites(ring);
        
        int meta = Pipe.takeRingByteMetaData(ring);
        int len = Pipe.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        Pipe.readASCII(ring, target, meta, len);
        
        assertEquals("1234567890",target.toString());
                
    }
    
    @Test
    public void intToASCIITest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
        ring.reset(0,0);
        
        Pipe.validateVarLength(ring, 10);
                
        Pipe.addIntAsASCII(ring, 1234567890);
        Pipe.publishWrites(ring);
        
        int meta = Pipe.takeRingByteMetaData(ring);
        int len = Pipe.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        Pipe.readASCII(ring, target, meta, len);
        
        assertEquals("1234567890",target.toString());
                
    }
    
    @Test
    public void decimalToASCIITest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
        
        Pipe.validateVarLength(ring, 7);
                
        Pipe.addDecimalAsASCII(2, 123456, ring);
        Pipe.addBytePosAndLen(ring,0,7);
        
        Pipe.publishWrites(ring);
        
        int meta = Pipe.takeRingByteMetaData(ring);
        int len = Pipe.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        Pipe.readASCII(ring, target, meta, len);
        
        assertEquals("1234.56",target.toString());
        
        ring.reset();
        String ringToString = ring.toString();
        assertTrue(ringToString, ringToString.contains("headPos 0"));
        assertTrue(ringToString, ringToString.contains("tailPos 0"));

        
        Pipe.addDecimalAsASCII(2, 1, ring); 
        Pipe.addBytePosAndLen(ring,0,4);
        
        Pipe.publishWrites(ring);
        
        meta = Pipe.takeRingByteMetaData(ring);
        len = Pipe.takeRingByteLen(ring);
        
        target.setLength(0);
        Pipe.readASCII(ring, target, meta, len);
        
        assertEquals("0.01",target.toString());
                        
        ring.reset();
        ringToString = ring.toString();
        assertTrue(ringToString, ringToString.contains("headPos 0"));
        assertTrue(ringToString, ringToString.contains("tailPos 0"));
        
        Pipe.addDecimalAsASCII(-2, 1, ring);
        Pipe.addBytePosAndLen(ring,0,4);
        
        Pipe.publishWrites(ring);
        
        meta = Pipe.takeRingByteMetaData(ring);
        len = Pipe.takeRingByteLen(ring);
        
        target.setLength(0);
        Pipe.readASCII(ring, target, meta, len);
        
        assertEquals("100.",target.toString());
    }
    
    @Test
    public void addByteBufferTest() {
    
        byte primaryRingSizeInBits = 7; //this ring is 2^7 eg 128
        byte byteRingSizeInBits = 16;
        
        Pipe ring = new Pipe(new PipeConfig(primaryRingSizeInBits, byteRingSizeInBits, null, FROM));
        ring.initBuffers();
        ring.reset(0,0);
        
        Pipe.validateVarLength(ring, 10);
                
        
        
        ByteBuffer source = ByteBuffer.allocate(100);
        source.put("HelloWorld".getBytes());
        source.flip();
        
        Pipe.addByteBuffer(source, ring);
        Pipe.publishWrites(ring);
        
        int meta = Pipe.takeRingByteMetaData(ring);
        int len = Pipe.takeRingByteLen(ring);
        
        StringBuilder target = new StringBuilder();
        Pipe.readASCII(ring, target, meta, len);
        
        assertEquals("HelloWorld",target.toString());
                
    }
    
    
}
