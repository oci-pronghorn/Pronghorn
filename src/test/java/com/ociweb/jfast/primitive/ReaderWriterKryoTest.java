//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.util.Arrays;

import org.junit.Test;

import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class ReaderWriterKryoTest {

	private final int speedTestSize = 30000;
	private final int testCycles = 7;

	//These common test values are used from the smallest test to the largest so results can be compared
	public final static long[] unsignedLongData = new long[] {0,1,63,64,65,126,127,128,8000,16383,16384,16385,16386,2097152,268435456,
		                                                          Integer.MAX_VALUE, Long.MAX_VALUE/2, Long.MAX_VALUE-2, Long.MAX_VALUE-1 
		                                                          };
	public final static int[] unsignedIntData =   new int[]  {0,1,63,64,65,126,127,128,8000,16383,16384,16385,16386,2097152,268435456,
																 Integer.MAX_VALUE-2, Integer.MAX_VALUE-1 //must be 1 less so there is room for the nulled form of uInt
		                                                         }; 
	
	public final static CharSequence[] stringData =   new CharSequence[]  {"","a","ab","abc","abcd","abcde","abcdef","abcdefg",
																  buildString("g",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK-1),
																  buildString("h",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK),
																  buildString("i",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK+1),
																  buildString("j",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK+2),
																  buildString("k",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK*2)};
	
	public final static byte[][] byteData =  new byte[][] {new byte[]{},new byte[]{1},new byte[]{1,2},new byte[]{1,2,3,4},
		                                                       new byte[]{1,2,3,4,5,6,7,8},
		                                                       new byte[]{1,2,3,4,5,6,7,8,9},
		                                                       new byte[]{1,2,3,4,5,6,7,8,9,10},
		                                                       new byte[]{1,2,3,4,5,6,7,8,9,10,11},
		                                                       new byte[]{1,2,3,4,5,6,7,8,9,10,11,12},
		                                                       new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13},
															   new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14},
															   new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15},
															   new byte[]{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16}
	};
	
	//needed for threaded test.
	private float writeDurationIOSpeed;
	public static final int VERY_LONG_STRING_MASK = 0x0F;//0x7F; 
	private Output pwIOSpeed;
	
	
	private static String buildString(String value, int i) {
		StringBuffer result = new StringBuffer();
		while (--i>=0) {
			result.append( ((i&1)==0 ? value : ('0'+(i&0x1F))));
		}
		return result.toString();
	}

	private float min(float a, float b) {
		return a<b ? a : b;
	}
	
	@Test
	public void skipped() {
		
	}
	
//	@Test 
	public void testNulls() {
		
		int fieldSize = 2;
		int nullLoops = 10000;
		int capacity = speedTestSize*fieldSize*nullLoops;
		
		byte[] buffer = new byte[capacity];		
		final Output pw = new Output(buffer);
		
		int i = 0;
		while (i<nullLoops) {
			pw.writeVarInt(0, true);
			i++;
		}
		
		pw.flush();
		
		Input pr = new Input(buffer);
		
		i = 0;
		while (i<nullLoops) {
			assertEquals(0,pr.readVarInt(true));
			i++;
		}
		
		int passes = speedTestSize / nullLoops;

		///////////////////////////////////
		//////////////////////////////////
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			pw.setPosition(0);
			int tp = passes*nullLoops;
			
			long start = System.nanoTime();

			int j = tp;
			while (--j>=0) {				
				pw.writeVarInt(0, true);
			}

			pw.flush();  
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
	
			pr.setPosition(0);
			
			start = System.nanoTime();
			j = tp;
			while (--j>=0) {
				pr.readVarInt(true);					
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println("Kryo null: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte  totalWritten:"+pw.total());
		System.gc();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}
		
	}
	
//	@Test 
	public void testIntegersByteBuffer() {
		int fieldSize = 7;
		
		int intSpeedTestSize = 3000000;
		
		String name = "KryoByteBuffer ";
		
		int passes = intSpeedTestSize / unsignedLongData.length;
		double count = passes*unsignedLongData.length;
		int capacity = intSpeedTestSize*fieldSize;
				
		ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
		
		final ByteBufferOutput pw = new ByteBufferOutput(buffer,0,capacity);
		
		int i = 0;
		while (i<unsignedLongData.length) {
			pw.writeVarLong(unsignedLongData[i],true);
			pw.writeVarLong(unsignedLongData[i],true);
			pw.writeVarLong(-unsignedLongData[i++],false);
		}
		i=0;
		while (i<unsignedIntData.length) {	
			pw.writeVarInt(unsignedIntData[i],true);
			pw.writeVarInt(-unsignedIntData[i],false);			
			pw.writeVarInt(unsignedIntData[i++],true);
		}
		
		pw.flush();
		buffer.flip();
		
		ByteBufferInput pr = new ByteBufferInput(buffer,0,pw.total());
		
		i = 0;
		while (i<unsignedLongData.length) {
			assertEquals(unsignedLongData[i], pr.readVarLong(true));
			assertEquals(unsignedLongData[i], pr.readVarLong(true));
			assertEquals(-unsignedLongData[i++], pr.readVarLong(false));
		}
		i=0;
		while (i<unsignedIntData.length) {	
			assertEquals(unsignedIntData[i], pr.readVarInt(true));
			assertEquals(-unsignedIntData[i], pr.readVarInt(false));
			assertEquals(unsignedIntData[i++], pr.readVarInt(true));
		}
		

		
		///////////////////////////////////
		//////////////////////////////////
		
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(capacity*10);
		pw.setOutputStream(outputStream);
		
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			pw.setPosition(0);
			buffer.clear();
			
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeVarLong(unsignedLongData[i++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			
			buffer.flip();
			pr = new ByteBufferInput(buffer,0,outputStream.size());
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readVarLong(true);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" unsigned long: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte,  iterations "+count);
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			pw.setPosition(0);
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeVarLong(-unsignedLongData[i++],false);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			buffer.flip();
			pr = new ByteBufferInput(buffer,0,outputStream.size());
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readVarLong(false);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed long neg: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeVarLong(unsignedLongData[i++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			buffer.flip();
			pr = new ByteBufferInput(buffer,0,pw.total());
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readVarLong(true);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed long pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			outputStream.reset();
			pw.setPosition(0);
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeVarInt(unsignedIntData[j++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			
			buffer.flip();
			pr = new ByteBufferInput(buffer,0,outputStream.size());
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readVarInt(true);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" unsigned integer: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");		
	
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			pw.setPosition(0);
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeVarInt(-unsignedIntData[j++],false);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			buffer.flip();
			pr = new ByteBufferInput(buffer,0,outputStream.size());
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readVarInt(false);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed integer neg: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte,  iterations "+count);		
	
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			outputStream.reset();
			pw.setPosition(0);
			pw.clear();
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeVarInt(unsignedIntData[j++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			buffer.flip();
			pr = new ByteBufferInput(buffer,0,outputStream.size());
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readVarInt(true);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed integer pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte, total bytes "+pw.total());		
	
		
	}
	
//	@Test 
	public void testIntegersByteArray() {
		int fieldSize = 7;
		
		int intSpeedTestSize = 3000000;
		
		int tempInt = 0;
		
		String name = "KryoByteArray ";
		
		int passes = intSpeedTestSize / unsignedLongData.length;
		double count = passes*unsignedLongData.length;
		int capacity = intSpeedTestSize*fieldSize;
				
		byte[] buffer = new byte[capacity];
		
		final Output pw = new Output(buffer);
		
		int i = 0;
		while (i<unsignedLongData.length) {
			pw.writeVarLong(unsignedLongData[i],true);
			pw.writeVarLong(unsignedLongData[i],true);
			pw.writeVarLong(-unsignedLongData[i++],false);
		}
		i=0;
		while (i<unsignedIntData.length) {	
			pw.writeVarInt(unsignedIntData[i],true);
			pw.writeVarInt(-unsignedIntData[i],false);			
			pw.writeVarInt(unsignedIntData[i++],true);
		}
		
		pw.flush();
		
		Input pr = new Input(buffer);
		
		i = 0;
		while (i<unsignedLongData.length) {
			assertEquals(unsignedLongData[i], pr.readVarLong(true));
			assertEquals(unsignedLongData[i], pr.readVarLong(true));
			assertEquals(-unsignedLongData[i++], pr.readVarLong(false));
		}
		i=0;
		while (i<unsignedIntData.length) {	
			assertEquals(unsignedIntData[i], pr.readVarInt(true));
			assertEquals(-unsignedIntData[i], pr.readVarInt(false));
			assertEquals(unsignedIntData[i++], pr.readVarInt(true));
		}
		

		
		///////////////////////////////////
		//////////////////////////////////
		
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream(capacity*10);
		pw.setOutputStream(outputStream);
		
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			pw.setPosition(0);
			
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeVarLong(unsignedLongData[i++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			
			pr = new Input(buffer);
			
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readVarLong(true);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" unsigned long: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte,  iterations "+count);
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			pw.setPosition(0);
			
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeVarLong(-unsignedLongData[i++],false);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			pr = new Input(buffer);
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readVarLong(false);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed long neg: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeVarLong(unsignedLongData[i++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			pr = new Input(buffer);
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readVarLong(true);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed long pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		
		//////////////////////////////////
		//////////////////////////////////
        
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			outputStream.reset();
			pw.setPosition(0);
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeVarInt(unsignedIntData[j++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			
			pr = new Input(buffer);
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					tempInt |= pr.readVarInt(true);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" unsigned integer: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");		
	
		//////////////////////////////////
		//////////////////////////////////

        
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			pw.clear();
			pw.setPosition(0);
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeVarInt(-unsignedIntData[j++],false);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			pr = new Input(buffer);
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					tempInt |= pr.readVarInt(false);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed integer neg: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte,  iterations "+count);		
	
		//////////////////////////////////
		//////////////////////////////////

		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			outputStream.reset();
			pw.setPosition(0);
			pw.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeVarInt(unsignedIntData[j++],true);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.total());
			pr = new Input(buffer);
			outputStream.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					tempInt |= pr.readVarInt(true);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.total());
		}
		System.out.println(name+" signed integer pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte, total bytes "+pw.total());		
	
		
	}
/*
 * 
	//TODO: add utf8 test here
	//TODO: add utf8 encoder/decoder test here.
	
	@Test 
	public void testStrings() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
	
		int i = 0;
		while (i<stringData.length) {
			pw.writeTextASCII(stringData[i++]);
		}
		
		pw.flush();
		
		
		byte[] byteArray = baost.toByteArray();
		assertEquals(pw.totalWritten(),byteArray.length);
		
		FASTInputByteArray input = new FASTInputByteArray(byteArray);
		final PrimitiveReader pr = new PrimitiveReader(input);
		
		i = 0;
		StringBuilder builder = new StringBuilder();
		while (i<stringData.length) {
			builder.setLength(0);
			pr.readTextASCII(builder);
			assertEquals(stringData[i++],builder.toString());
		}
		
		int passes = speedTestSize / stringData.length;

		///////////////////////////////////
		//////////////////////////////////
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		//limit this down to the size of the other tests to get a better comparison
		//makes the bytesPerWrite about the same size as the others
		int trunkTestLimit = stringData.length;//Much faster with larger strings.
		System.gc();
		
		char[] target = new char[ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK*3];
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			assertTrue(baost.size()==0);
			
			long start = System.nanoTime();
			int p = passes;			
			while (--p>=0) {
				i = trunkTestLimit;
				while (--i>=0) {
					pw.writeTextASCII(stringData[i]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
			
			//new bigger byte array for all the passes.
			input.reset(baost.toByteArray());
			
			start = System.nanoTime();
			p = passes;
			while (--p>=0) {
				i = trunkTestLimit;
				while (--i>=0) {
					pr.readTextASCII(target, 0);
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("ascii: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte ");
		
		
	}
	
	@Test 
	public void testBytes() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
		
		int i = 0;
		while (i<byteData.length) {
			pw.writeByteArrayData(byteData[i++]);
		}
		
		pw.flush();
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		final PrimitiveReader pr = new PrimitiveReader(input);
		
		int largest = 0;
		i = 0;
		while (i<byteData.length) {
			int length = byteData[i].length;
			if (length>largest) {
				largest = length;
			}
			byte[] target = new byte[length];
			pr.readByteData(target, 0, length);
			assertTrue(Arrays.equals(byteData[i++],target));
		}
		
		int passes = speedTestSize / byteData.length;

		byte[] tempTarget = new byte[largest];
		
		///////////////////////////////////
		//////////////////////////////////
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;			
			while (--p>=0) {
				i = 0;
				while (i<byteData.length) {
					pw.writeByteArrayData(byteData[i++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			p = passes;
			while (--p>=0) {
				i = 0;
				while (i<byteData.length) {
					pr.readByteData(tempTarget, 0, byteData[i++].length);
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("byteArray: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		
	}
	
	//  */
	
}
