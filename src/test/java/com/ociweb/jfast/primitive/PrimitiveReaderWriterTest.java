package com.ociweb.jfast.primitive;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.junit.Test;

import com.ociweb.jfast.MyCharSequnce;
import com.ociweb.jfast.field.util.CharSequenceShadow;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;

public class PrimitiveReaderWriterTest {

	private final int speedTestSize = 3000000;
	private final int testCycles = 7;

	//These common test values are used from the smallest test to the largest so results can be compared
	public final static long[] unsignedLongData = new long[] {0,1,63,64,65,126,127,128,8000,16383,16384,16385,16386,2097152,268435456,
		                                                          Integer.MAX_VALUE, Long.MAX_VALUE/2 //TODO: bug in largest large signed value, (Long.MAX_VALUE/4)*3
		                                                          };
	public final static int[] unsignedIntData =   new int[]  {0,1,63,64,65,126,127,128,8000,16383,16384,16385,16386,2097152,268435456,
		                                                         Integer.MAX_VALUE-1 //must be 1 less so there is room for the nulled form of uInt
		                                                         }; 
	public final static int STRING_SPEED_TEST_LIMIT = 8;
	public final static String[] stringData =   new String[]  {"","a","ab","abc","abcd","abcde","abcdef","abcdefg",
																  buildString("g",PrimitiveReader.VERY_LONG_STRING_MASK-1),
																  buildString("h",PrimitiveReader.VERY_LONG_STRING_MASK),
																  buildString("i",PrimitiveReader.VERY_LONG_STRING_MASK+1),
																  buildString("j",PrimitiveReader.VERY_LONG_STRING_MASK+2),
																  buildString("k",PrimitiveReader.VERY_LONG_STRING_MASK*2)}
	;
	public final static byte[][] byteData =  new byte[][] {new byte[]{},new byte[]{1},new byte[]{1,2},new byte[]{1,2,3,4},new byte[]{1,2,3,4,5,6,7,8}};
	
	@Test
	public void testBufferSpeed() {
		//ByteBuffer vs ByteArrayOutputStream
		
		int fieldSize = 5;
		int capacity = speedTestSize*fieldSize;
		
		int passes = speedTestSize / unsignedLongData.length;
		double count = passes*stringData.length;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);		
		PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		PrimitiveReader pr = new PrimitiveReader(input);
		
		double writeDuration = Double.MAX_VALUE;
		double readDuration = Double.MAX_VALUE;
		double avgBytesWritten = 0;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					pw.writeUnsignedLong(unsignedLongData[i++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			
			//must reset stream back to beginning
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					pr.readUnsignedLong();
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("ByteArray I/O StreamBuffer: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
		/////////////////
		/////////////////
		/////////////////		
		
		//ByteBuffer buffer = ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		
		pw = new PrimitiveWriter(new FASTOutputByteBuffer(buffer));
		pr = new PrimitiveReader(new FASTInputByteBuffer(buffer));
		
		writeDuration = Double.MAX_VALUE;
		readDuration = Double.MAX_VALUE;
		avgBytesWritten = 0;
		
		cycles = testCycles;
		while (--cycles>=0) {
			//byte buffer specific clear
			buffer.clear();
			
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					pw.writeUnsignedLong(unsignedLongData[i++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = buffer.position()/(double)count;
			
			//must reset byte buffer back to beginning
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					pr.readUnsignedLong();
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("                ByteBuffer: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
		/////////////////
		/////////////////
		/////////////////		
		
		byte[] bufferArray = new byte[capacity];
		
		FASTOutputByteArray byteArrayOutput = new FASTOutputByteArray(bufferArray);
		pw = new PrimitiveWriter(byteArrayOutput);
		
		FASTInputByteArray byteArrayInput = new FASTInputByteArray(bufferArray);
		pr = new PrimitiveReader(byteArrayInput);
		
		writeDuration = Double.MAX_VALUE;
		readDuration = Double.MAX_VALUE;
		avgBytesWritten = 0;
		
		cycles = testCycles;
		while (--cycles>=0) {
			//specific reset
			byteArrayOutput.reset();
			
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					pw.writeUnsignedLong(unsignedLongData[i++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = byteArrayOutput.position()/(double)count;
			
			//must reset bytes back to beginning
			byteArrayInput.reset();
			
			//TODO: this is NOT clear why this is required!
			pr = new PrimitiveReader(byteArrayInput);
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					pr.readUnsignedLong();
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("                ByteArray: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
	}
	
	
	private static String buildString(String value, int i) {
		StringBuffer result = new StringBuffer();
		while (--i>=0) {
			result.append(value);
		}
		return result.toString();
	}


	@Test 
	public void testNulls() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
		
		int i = 0;
		while (i<stringData.length) {
			pw.writeNull();
			i++;
		}
		
		pw.flush();
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		final PrimitiveReader pr = new PrimitiveReader(input);
		
		i = 0;
		while (i<stringData.length) {
			assertTrue(pr.peekNull());
			pr.incPosition();
			i++;
		}
		
		int passes = speedTestSize / stringData.length;
		double count = passes*stringData.length;
		
		///////////////////////////////////
		//////////////////////////////////
		
		double writeDuration = Double.MAX_VALUE;
		double readDuration = Double.MAX_VALUE;
		double avgBytesWritten = 0;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;			
			while (--p>=0) {
				i = 0;
				while (i<stringData.length) {
					pw.writeNull();
					i++;
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<stringData.length) {
					pr.peekNull();
					pr.incPosition();
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("null: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
		
	}
	
	@Test 
	public void testIntegers() {
		int fieldSize = 5;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
		
		int i = 0;
		while (i<unsignedLongData.length) {
			pw.writeUnsignedLong(unsignedLongData[i]);
			pw.writeSignedLong(unsignedLongData[i]);
			pw.writeSignedLong(-unsignedLongData[i++]);
		}
		i=0;
		while (i<unsignedIntData.length) {	
			pw.writeSignedInteger(unsignedIntData[i]);
			pw.writeSignedInteger(-unsignedIntData[i]);			
			pw.writeUnsignedInteger(unsignedIntData[i++]);
		}
		
		pw.flush();
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		final PrimitiveReader pr = new PrimitiveReader(input);
		
		i = 0;
		while (i<unsignedLongData.length) {
			assertEquals(unsignedLongData[i], pr.readUnsignedLong());
			assertEquals(unsignedLongData[i], pr.readSignedLong());
			assertEquals(-unsignedLongData[i++], pr.readSignedLong());
		}
		i=0;
		while (i<unsignedIntData.length) {	
			assertEquals(unsignedIntData[i], pr.readSignedInteger());
			assertEquals(-unsignedIntData[i], pr.readSignedInteger());
			assertEquals(unsignedIntData[i++], pr.readUnsignedInteger());
		}
		
		int passes = speedTestSize / unsignedLongData.length;
		double count = passes*unsignedLongData.length;
		
		///////////////////////////////////
		//////////////////////////////////
		
		double writeDuration = Double.MAX_VALUE;
		double readDuration = Double.MAX_VALUE;
		double avgBytesWritten = 0;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeUnsignedLong(unsignedLongData[i++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readUnsignedLong();
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("unsigned long: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten+" iterations "+count);
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Double.MAX_VALUE;
		readDuration = Double.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeSignedLong(-unsignedLongData[i++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readSignedLong();
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("signed long neg: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Double.MAX_VALUE;
		readDuration = Double.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeSignedLong(unsignedLongData[i++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readSignedLong();
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("signed long pos: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
		
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Double.MAX_VALUE;
		readDuration = Double.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeUnsignedInteger(unsignedIntData[j++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedLongData.length) {
					pr.readUnsignedInteger();
					j++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("unsigned integer: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);		
	
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Double.MAX_VALUE;
		readDuration = Double.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeSignedInteger(-unsignedIntData[j++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readSignedInteger();
					j++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("signed integer neg: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten+" iterations "+count);		
	
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Double.MAX_VALUE;
		readDuration = Double.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeSignedInteger(unsignedIntData[j++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readSignedInteger();
					j++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("signed integer pos: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);		
	
		
	}
	
	@Test 
	public void testStrings() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
	
		int i = 0;
		while (i<stringData.length) {
			pw.writeASCII(stringData[i++]);
		}
		
		pw.flush();
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		final PrimitiveReader pr = new PrimitiveReader(input);
		
		i = 0;
		CharSequenceShadow shadow = new CharSequenceShadow();
		while (i<stringData.length) {
			pr.readASCII(shadow);
			assertEquals(stringData[i++],shadow.toString());
		}
		
		int passes = speedTestSize / stringData.length;
		double count = passes*stringData.length;
		
		///////////////////////////////////
		//////////////////////////////////
		
		double writeDuration = Double.MAX_VALUE;
		double readDuration = Double.MAX_VALUE;
		double avgBytesWritten = 0;
		
		//limit this down to the size of the other tests to get a better comparison
		//makes the bytesPerWrite about the same size as the others
		int trunkTestLimit = Math.min(STRING_SPEED_TEST_LIMIT,stringData.length);
		
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;			
			while (--p>=0) {
				i = 0;
				while (i<trunkTestLimit) {
					pw.writeASCII(stringData[i++]);
				}
			}
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<trunkTestLimit) {
					pr.readASCII(shadow);
					i++;
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("ascii: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
		
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
		double count = passes*byteData.length;
		
		byte[] tempTarget = new byte[largest];
		
		///////////////////////////////////
		//////////////////////////////////
		
		double writeDuration = Double.MAX_VALUE;
		double readDuration = Double.MAX_VALUE;
		double avgBytesWritten = 0;
		
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
			writeDuration =  Math.min(writeDuration, (System.nanoTime()-start)/count);
			pw.flush();
			avgBytesWritten = baost.size()/(double)count;
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			p = passes;
			while (--p>=0) {
				i = 0;
				while (i<byteData.length) {
					pr.readByteData(tempTarget, 0, byteData[i++].length);
				}
			}
			readDuration = Math.min(readDuration, (System.nanoTime()-start)/count);
		}
		System.out.println("byteArray: write:"+writeDuration+"ns  read:"+readDuration+"ns  bytesPerWrite:"+avgBytesWritten);
		
		
	}
	
}
