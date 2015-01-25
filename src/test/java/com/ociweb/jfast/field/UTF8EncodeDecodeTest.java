//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.field;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

import org.junit.Test;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.pronghorn.ring.RingBuffer;

public class UTF8EncodeDecodeTest {

	String unicodeTestString = new String("A" +  "\u00ea" +      "\u00f1" + "\u00fc" +
	                                      "C" +  "\u03ff" +      "\u0fff" +
			                                     "\u3fff" +      "\uffff" + "D" +	                                             
	                                             ((char)(0xf7bff))+
	                                             ((char)(0xf7bdff))+
	                                             ((char)(0xf7bfdff))+
	                                             ((char)(0xf7bffdff))+
	                                             "E"
			                             );

	@Test
	public void testUTF8EncoderCharSeqLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputByteArray(myData), false);
		PrimitiveWriter.ensureSpace(data.length,writer);
        
        //convert from chars to bytes
        //writeByteArrayData()
        int len = unicodeTestString.length();
        byte[] buffer = writer.buffer;
        int limit = writer.limit;
        int c = 0;
        while (c < len) {
            limit = RingBuffer.encodeSingleChar((int) unicodeTestString.charAt(c++), buffer, 0xFFFFFFFF, limit);
        }
        writer.limit = limit;
		PrimitiveWriter.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	@Test
	public void testUTF8EncoderCharSeqTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(data.length,new FASTOutputByteArray(myData),true);
		PrimitiveWriter.ensureSpace(data.length,writer);
        
        //convert from chars to bytes
        //writeByteArrayData()
        int len = unicodeTestString.length();
        byte[] buffer = writer.buffer;
        int limit = writer.limit;
        int c = 0;
        while (c < len) {
            limit = RingBuffer.encodeSingleChar((int) unicodeTestString.charAt(c++), buffer, 0xFFFFFFFF, limit);
        }
        writer.limit = limit;
		PrimitiveWriter.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	@Test
	public void testUTF8EncoderArrayLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputByteArray(myData), false);
		char[] temp = unicodeTestString.toCharArray();
        int offset = 0;
        int length = temp.length;
		PrimitiveWriter.ensureSpace(data.length,writer);
        
        //convert from chars to bytes
        int limit = writer.limit;
                
        while (--length >= 0) {
            limit = RingBuffer.encodeSingleChar((int) temp[offset++], writer.buffer, 0xFFFFFFFF, limit);
        }
        writer.limit = limit;
        PrimitiveWriter.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	@Test
	public void testUTF8EncoderArrayTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		byte[] myData = new byte[data.length];
		
		PrimitiveWriter writer = new PrimitiveWriter(data.length,new FASTOutputByteArray(myData),true);
		char[] temp = unicodeTestString.toCharArray();
        int offset = 0;
        int length = temp.length;
		PrimitiveWriter.ensureSpace(data.length,writer);
        
        //convert from chars to bytes
        int limit = writer.limit;
                
        while (--length >= 0) {
            limit = RingBuffer.encodeSingleChar((int) temp[offset++], writer.buffer, 0xFFFFFFFF, limit);
        }
        writer.limit = limit;
		PrimitiveWriter.flush(writer);
				
		assertTrue("bytes do not match",Arrays.equals(data, myData));
	}
	
	
	@Test
	public void testUTF8DecoderArrayLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		//required to have extra data to make it call the faster side of the implementation.
		int copies = 10;
		byte[] paddedData = new byte[copies*data.length];
		while (--copies>=0) {
			System.arraycopy(data, 0, paddedData, copies*data.length, data.length);
		}
		
		char[] target = new char[unicodeTestString.length()];
		
		PrimitiveReader reader = new PrimitiveReader(2048, new FASTInputByteArray(paddedData), 32);
		PrimitiveReader.fetch(reader);
        int offset = 0;
        int byteCount = data.length;//required to preload the data to make it call the faster side of the implementation.
		{ 
            byte[] temp = new byte[byteCount];
            
            PrimitiveReader.readByteData(temp,0,byteCount,reader); //read bytes
            
            long charAndPos = 0;        //convert bytes to chars
            while (charAndPos>>32 < byteCount  ) {
                charAndPos = RingBuffer.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                target[offset++]=(char)charAndPos;
            }
        }
		
		assertTrue("chars do not match "+unicodeTestString+" vs "+new String(target), Arrays.equals(unicodeTestString.toCharArray(), target));	
		
	}
	
	@Test
	public void testUTF8DecoderArrayTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		char[] target = new char[unicodeTestString.length()];
		
		PrimitiveReader reader = new PrimitiveReader(data.length, new FASTInputByteArray(data), 0);
		PrimitiveReader.fetch(reader);
        int offset = 0;
        int byteCount = data.length;
		{ 
            byte[] temp = new byte[byteCount];
            
            PrimitiveReader.readByteData(temp,0,byteCount,reader); //read bytes
            
            long charAndPos = 0;   //convert bytes to chars     
            while (charAndPos>>32 < byteCount  ) {
                charAndPos = RingBuffer.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                target[offset++]=(char)charAndPos;
            }
        }
		
		assertTrue("chars do not match "+unicodeTestString+" vs "+new String(target), Arrays.equals(unicodeTestString.toCharArray(), target));	
		
	}
	
	@Test
	public void testUTF8DecoderArrayTooTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		char[] target = new char[unicodeTestString.length()];
		
		//set too small by 1 byte
		PrimitiveReader reader = new PrimitiveReader(data.length-1, new FASTInputByteArray(data), 0);
		PrimitiveReader.fetch(reader);
        int byteCount = data.length;
		{ 
            byte[] temp = new byte[byteCount];
            try {
            	PrimitiveReader.readByteData(temp,0,byteCount,reader); //read bytes
            	fail("should have thrown");
            } catch (FASTException e) {
            	//ok
            }
        }

	}
	
	
	@Test
	public void testUTF8DecoderAppendableLargeBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
				
		//required to have extra data to make it call the faster side of the implementation.
		int copies = 10;
		byte[] paddedData = new byte[copies*data.length];
		while (--copies>=0) {
			System.arraycopy(data, 0, paddedData, copies*data.length, data.length);
		}
		
		PrimitiveReader reader = new PrimitiveReader(2048, new FASTInputByteArray(paddedData), 32);
		PrimitiveReader.fetch(reader);
        int byteCount = data.length;
        Appendable target1 = new StringBuilder();
        {
            byte[] temp = new byte[byteCount];
            
            PrimitiveReader.readByteData(temp,0,byteCount,reader);//read bytes
            
            long charAndPos = 0;        //convert bytes to chars
            while (charAndPos>>32 < byteCount  ) {
                charAndPos = RingBuffer.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                try{
                    target1.append((char)charAndPos);
                } catch (IOException e) {
                    throw new FASTException(e);
                }
            }
        }
        String target = target1.toString();
		
		assertTrue("chars do not match "+unicodeTestString+" vs "+target, Arrays.equals(unicodeTestString.toCharArray(), target.toCharArray()));	
		
	}
	
	@Test
	public void testUTF8DecoderAppendableTightBuffer() {
		byte[] data = unicodeTestString.getBytes(Charset.forName("UTF8"));
		
		PrimitiveReader reader = new PrimitiveReader(data.length, new FASTInputByteArray(data), 0);
		PrimitiveReader.fetch(reader);
        int byteCount = data.length;
        Appendable target1 = new StringBuilder();
        {
            byte[] temp = new byte[byteCount];
            
            PrimitiveReader.readByteData(temp,0,byteCount,reader); //read bytes
            
            long charAndPos = 0;        
            while (charAndPos>>32 < byteCount  ) {//convert bytes to chars
                charAndPos = RingBuffer.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                try{
                    target1.append((char)charAndPos);
                } catch (IOException e) {
                    throw new FASTException(e);
                }
            }
        }
        String target = target1.toString();
		
		assertEquals("chars do not match "+unicodeTestString+" vs "+target, unicodeTestString, target);	
		
	}
}
