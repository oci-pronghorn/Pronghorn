package com.ociweb.pronghorn.stage.network;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import com.ociweb.pronghorn.network.mqtt.MQTTEncoder;
import com.ociweb.pronghorn.pipe.Pipe;

public class MQTTEncoderTest {

	
	@Test
	public void testconvertToUTF8() {
		
		int targetMask = (1<<10)-1;
		byte[] target = new byte[targetMask+1];
		int tagetIdx = 0;
		StringBuilder builder = new StringBuilder();
				
		Random random = new Random();//may want to put a seed here if this fails randomly
				
		int largestChar = 55000;// TODO: B, fix bug where last bytes mismatch, repro by using this value. 57000;
		int longestSequence = targetMask/6;
				
		int testSize = 20000;
		while (--testSize>=0) {	
		
			builder.setLength(0);
			int len = random.nextInt(longestSequence)+1;
			while (--len>=0) {
				char c = (char)random.nextInt(largestChar);
				builder.append(c);
			}
			
			String testValue = builder.toString();		
			
			int testOff = 0;
			int testLen = testValue.length();
			Pipe.convertToUTF8(testValue, testOff, testLen, target, tagetIdx, targetMask);
			
			try {
				byte[] expected = testValue.getBytes("UTF-8");
				
			    int i = expected.length;
			    while (--i>=0) {
			    	if (expected[i]!=target[i]) {

			    		System.err.println(testValue);
			    		System.err.println("exp:"+Arrays.toString(expected));
			    		System.err.println("gen:"+Arrays.toString(Arrays.copyOfRange(target, 0, expected.length)));

			    		fail("values do not match at "+i+" out of "+expected.length);
			    		
			    	}
			    }
			} catch (UnsupportedEncodingException e) {
				fail(e.getMessage());
			}
		}
	}
	
	//the new implementation has an advantage on short strings
	//this advantage disappears with strings longer than 64 chars
	//on long stings the new implementation is about the same speed
	private final int speedSizeTest = 10000000;
	private final int speedMaxChar = 55000;
	private final int speedTextSize = 4;//text up to 16 chars
	private final int speedSeed = 32;
	
	@Test
	public void testconvertToUTF8SpeeedNew() {
		
		int targetMask = (1<<speedTextSize)-1;
		byte[] target = new byte[targetMask+1];
		int tagetIdx = 0;
		StringBuilder builder = new StringBuilder();
				
		Random random = new Random(speedSeed);
				
		int largestChar = speedMaxChar;
		int longestSequence = targetMask/6;
		long sum = 0;
		int testSize = speedSizeTest;
		while (--testSize>=0) {	
		
			builder.setLength(0);
			int len = random.nextInt(longestSequence)+1;
			while (--len>=0) {
				char c = (char)random.nextInt(largestChar);
				builder.append(c);
			}
			
			int bytes = Pipe.convertToUTF8(builder, 0, builder.length(), target, tagetIdx, targetMask);
			
			sum += bytes;
			if (bytes==0) {
				System.err.println("test value:"+builder.toString());
				fail();
				
			}
			
		}
		System.out.println("sum:"+sum);
	}
	
	@Test
	public void testconvertToUTF8SpeedClassic() {
		
		int targetMask = (1<<speedTextSize)-1;
		StringBuilder builder = new StringBuilder();
				
		Random random = new Random(speedSeed);
				
		int largestChar = speedMaxChar;
		int longestSequence = targetMask/6;
				
		long sum = 0;
		int testSize = speedSizeTest;
		while (--testSize>=0) {	
		
			builder.setLength(0);
			int len = random.nextInt(longestSequence)+1;
			while (--len>=0) {
				char c = (char)random.nextInt(largestChar);
				builder.append(c);
			}
			
			String testValue = builder.toString();		
			
			
			try {
				byte[] expected = testValue.getBytes("UTF-8");
				
				sum+=expected.length;
				if (null==expected || 0==expected.length) {
					System.err.println("test value:"+testValue);
					fail();
					
				}
				
			} catch (UnsupportedEncodingException e) {
				fail(e.getMessage());
			}
		}
		System.out.println("sum:"+sum);
	}
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	@Test
	public void testEncodeVarLength() {
		//this is a very simple generative test because it just uses sequential numbers
		
		byte[] targetSpace = new byte[4];//MQTT 3.1.1 section 2.2.3, this is only supported for up to 4 bytes
		int i = 0;
		
		while (i<(1<<25)) {
			//NOTE: this test only covers the generated values not their position wrapping an array.
			Arrays.fill(targetSpace, (byte)0);
			
			MQTTEncoder.encodeVarLength(targetSpace, 0, 0xFF, i);
			
			validateMatch(targetSpace, 0, i);
			
			
			i++;
		}
		
		//System.out.println("tested up to 0x"+Integer.toHexString(i));
		
	}

	//This method taken from MQTT 3.1.1 section 2.2.3
	//	Non normative comment
	//	281
	//	282 The algorithm for encoding a non negative integer (X) into the variable length encoding scheme is as follows:
	//	283	do
	//	284    encodedByte = X MOD 128
	//	285    X = X DIV 128
	//	286 // if there are more data to encode, set the top bit of this byte
	//	287 if ( X > 0 )
	//	288	    encodedByte = encodedByte OR 128
	//	289	endif
	//	290      'output' encodedByte
	//	291	while ( X > 0 )
	//	292
	//	293
	//	294	Where MOD is the modulo operator (% in C), DIV is integer division (/ in C), and OR is bit-wise or	(| in C).
	//
	private void validateMatch(byte[] targetSpace, int idx, int x) {
		do {
			int encodedByte = x % 128;
			x = x / 128;
			if (x>0) {
				encodedByte |= 128;
			}
			if ((0xFF&targetSpace[idx++]) != encodedByte) {
				Assert.fail("Expected "+encodedByte+" but found "+targetSpace[idx-1]+" at "+(idx-1)+" for value "+x);
			}
		} while (x>0);
	}
	
	
}
