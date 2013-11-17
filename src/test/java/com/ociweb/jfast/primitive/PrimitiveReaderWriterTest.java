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

import com.ociweb.jfast.field.util.CharSequenceShadow;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputByteChannel;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteChannel;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;

public class PrimitiveReaderWriterTest {

	private final int speedTestSize = 30000;
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
																  buildString("k",PrimitiveReader.VERY_LONG_STRING_MASK*2)};
	
	public final static byte[][] byteData =  new byte[][] {new byte[]{},new byte[]{1},new byte[]{1,2},new byte[]{1,2,3,4},new byte[]{1,2,3,4,5,6,7,8}};
	
	//needed for threaded test.
	private PrimitiveWriter pwIOSpeed;
	private float writeDurationIOSpeed;
	
	@Test
	public void testBufferSpeed() {
		//ByteBuffer vs ByteArrayOutputStream
		
		int fieldSize = 10;
		int capacity = speedTestSize*fieldSize;
		final int passes = speedTestSize / unsignedLongData.length;
		final double count = passes*unsignedLongData.length;
		final boolean minimizeLatency = false;
				
		long totalBytesWritten = 0;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);	
		pwIOSpeed = new PrimitiveWriter(capacity, new FASTOutputStream(baost),(int) count, minimizeLatency);
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		PrimitiveReader pr = new PrimitiveReader(input);
		
		writeDurationIOSpeed = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					speedWriteTest(i++);
				}
			}
			pwIOSpeed.flush();
			long duration = System.nanoTime()-start;
			totalBytesWritten = baost.toByteArray().length;
			writeDurationIOSpeed =  min(writeDurationIOSpeed, duration/(float)totalBytesWritten);
		
			
			//must reset stream back to beginning
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					speedReadTest(pr);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)totalBytesWritten);
		}
		System.out.println("ByteArray I/O StreamBuffer: write:"+writeDurationIOSpeed+"ns  read:"+readDuration+"ns  per byte");
		
		/////////////////
		/////////////////
		/////////////////		
		
		//ByteBuffer buffer = ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		
		pwIOSpeed = new PrimitiveWriter(capacity, new FASTOutputByteBuffer(buffer), (int) count, minimizeLatency);
		pr = new PrimitiveReader(new FASTInputByteBuffer(buffer));
		
		writeDurationIOSpeed = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		
		cycles = testCycles;
		while (--cycles>=0) {
			//byte buffer specific clear
			buffer.clear();
			
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					speedWriteTest(i++);
				}
			}
			pwIOSpeed.flush();
			long duration = System.nanoTime()-start;
			totalBytesWritten = baost.toByteArray().length;
			writeDurationIOSpeed =  min(writeDurationIOSpeed, duration/(float)totalBytesWritten);
			
			//must reset byte buffer back to beginning
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					speedReadTest(pr);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)totalBytesWritten);
		}
		System.out.println("                ByteBuffer: write:"+writeDurationIOSpeed+"ns  read:"+readDuration+"ns  per byte");
		
		/////////////////
		/////////////////
		/////////////////		
		
		//channel!
		
		
		try {
			Pipe pipe = Pipe.open();
			
			//TODO: should flush rather than run out of space. set buffer size very small here after fix.
			//BUT we must flush between Groups/pmaps because attempting in the middle does not move position!!
			
			pwIOSpeed = new PrimitiveWriter(capacity, new FASTOutputByteChannel(pipe.sink()), testCycles*passes, minimizeLatency);
			pr = new PrimitiveReader(new FASTInputByteChannel(pipe.source()));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return;
		}
		
		writeDurationIOSpeed = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		final long bytesWritten = totalBytesWritten;
		new Thread(new Runnable(){

			@Override
			public void run() {
				
				int cycles = testCycles;
				while (--cycles>=0) {
					
					long start = System.nanoTime();
					int p = passes;
					while (--p>=0) {
						int i = 0;
						while (i<unsignedLongData.length) {
							speedWriteTest(i++);
						}
					}
					pwIOSpeed.flush();
					writeDurationIOSpeed =  min(writeDurationIOSpeed, (System.nanoTime()-start)/(float)bytesWritten);
				}
			}
			
		}).start();
		

			
		cycles = testCycles;
		while (--cycles>=0) {	
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					speedReadTest(pr);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)totalBytesWritten);
		}
		System.out.println("                ByteChannel: write:"+writeDurationIOSpeed+"ns  read:"+readDuration+"ns per byte");
		
		/////////////////
		/////////////////
		/////////////////
		
		byte[] bufferArray = new byte[capacity];
		
		FASTOutputByteArray byteArrayOutput = new FASTOutputByteArray(bufferArray);
		pwIOSpeed = new PrimitiveWriter(capacity, byteArrayOutput, (int) count, minimizeLatency);
		
		FASTInputByteArray byteArrayInput = new FASTInputByteArray(bufferArray);
		pr = new PrimitiveReader(byteArrayInput);
		
		writeDurationIOSpeed = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		
		cycles = testCycles;
		while (--cycles>=0) {
			//specific reset
			byteArrayOutput.reset();
			
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					speedWriteTest(i++);
				}
			}
			pwIOSpeed.flush();
			long duration = System.nanoTime()-start;
			totalBytesWritten = baost.toByteArray().length;
			writeDurationIOSpeed =  min(writeDurationIOSpeed, duration/(float)totalBytesWritten);
			
			//must reset bytes back to beginning
			byteArrayInput.reset();
			
			//TODO: this is NOT clear why this is required!
			pr = new PrimitiveReader(byteArrayInput);
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int i = 0;
				while (i<unsignedLongData.length) {
					speedReadTest(pr);
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)totalBytesWritten);
		}
		System.out.println("                ByteArray: write:"+writeDurationIOSpeed+"ns  read:"+readDuration+"ns  per byte");
		
	}


	private final void speedReadTest(PrimitiveReader pr) {
		pr.readPMap(10);
		pr.readUnsignedLong();
		pr.readSignedLong();
		pr.popPMap();
	}


	private final void speedWriteTest(int i) {
		pwIOSpeed.openPMap(10);
		pwIOSpeed.writeUnsignedLong(unsignedLongData[i]);
		pwIOSpeed.writeSignedLong(-unsignedLongData[i]);		
		pwIOSpeed.closePMap();
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
				while (i<stringData.length) {
					pw.writeNull();
					i++;
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
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
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("null: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		
	}
	
	@Test 
	public void testIntegers() {
		int fieldSize = 5;
		
		int intSpeedTestSize = 3000000;
		
		int passes = intSpeedTestSize / unsignedLongData.length;
		double count = passes*unsignedLongData.length;
		int capacity = intSpeedTestSize*fieldSize;
				
		ByteBuffer buffer = ByteBuffer.allocate(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputByteBuffer(buffer));
		
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
		buffer.flip();
		
		FASTInputByteBuffer input = new FASTInputByteBuffer(buffer);
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
		

		
		///////////////////////////////////
		//////////////////////////////////
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeUnsignedLong(unsignedLongData[i++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readUnsignedLong();
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());
		}
		System.out.println("unsigned long: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte,  iterations "+count);
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeSignedLong(-unsignedLongData[i++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readSignedLong();
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());
		}
		System.out.println("signed long neg: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		///////////////////////////////////
		//////////////////////////////////
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pw.writeSignedLong(unsignedLongData[i++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					pr.readSignedLong();
					i++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());
		}
		System.out.println("signed long pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeUnsignedInteger(unsignedIntData[j++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedLongData.length) {
					pr.readUnsignedInteger();
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());
		}
		System.out.println("unsigned integer: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");		
	
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeSignedInteger(-unsignedIntData[j++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readSignedInteger();
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());
		}
		System.out.println("signed integer neg: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte,  iterations "+count);		
	
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			buffer.clear();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeSignedInteger(unsignedIntData[j++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readSignedInteger();
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());
		}
		System.out.println("signed integer pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");		
	
		
	}
	
	private float min(float a, float b) {
		return a<b ? a : b;
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

		///////////////////////////////////
		//////////////////////////////////
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
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
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
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
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("ascii: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		
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
	
}
