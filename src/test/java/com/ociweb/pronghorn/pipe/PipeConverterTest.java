package com.ociweb.pronghorn.pipe;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class PipeConverterTest {

	private final byte primaryRingSizeInBits = 5; //this ring is 2^5 eg 32
	private final byte byteRingSizeInBits = 10;
	private final PipeConfig config = new PipeConfig(RawDataSchema.instance, 1<<primaryRingSizeInBits, 1<<byteRingSizeInBits);
    
    @Test
    public void longToASCIITest() {
    
        
        Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(config);
        pipe.initBuffers();
                
        Pipe.validateVarLength(pipe, 10);
        
        int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        Pipe.addLongAsASCII(pipe, 1234567890);
        Pipe.confirmLowLevelWrite(pipe, size);
        Pipe.publishWrites(pipe);
        
        Pipe.takeMsgIdx(pipe);
        int meta = Pipe.takeRingByteMetaData(pipe);
        int len = Pipe.takeRingByteLen(pipe);
        
        StringBuilder target = new StringBuilder();
        Pipe.readASCII(pipe, target, meta, len);
        
        assertEquals("1234567890",target.toString());
                
    }
    
    @Test
    public void intToASCIITest() {
    
		Pipe<RawDataSchema> pipe = new Pipe<RawDataSchema>(config);
        pipe.initBuffers();
        pipe.reset(0,0);
        
        Pipe.validateVarLength(pipe, 10);
                
        int size = Pipe.addMsgIdx(pipe, RawDataSchema.MSG_CHUNKEDSTREAM_1);
        Pipe.addIntAsASCII(pipe, 1234567890);
        Pipe.confirmLowLevelWrite(pipe, size);
        Pipe.publishWrites(pipe);
        
        Pipe.takeMsgIdx(pipe);
        int meta = Pipe.takeRingByteMetaData(pipe);
        int len = Pipe.takeRingByteLen(pipe);
        
        StringBuilder target = new StringBuilder();
        Pipe.readASCII(pipe, target, meta, len);
        
        assertEquals("1234567890",target.toString());
                
    }
    
    @Test
    public void decimalToASCIITest() {
    
        Pipe<RawDataSchema> ring = new Pipe<RawDataSchema>(config);
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
        assertTrue(ringToString, ringToString.contains("slabHeadPos 0"));
        assertTrue(ringToString, ringToString.contains("slabTailPos 0"));

        
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
        assertTrue(ringToString, ringToString.contains("blobHeadPos 0"));
        assertTrue(ringToString, ringToString.contains("blobTailPos 0"));
        
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

        Pipe<RawDataSchema> ring = new Pipe<RawDataSchema>(config);
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
