//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputSocketChannel;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputSocketChannel;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;

public class ReaderWriterPrimitiveTest {

	private final int speedTestSize = 30000;
	private final int testCycles = 5;

	//These common test values are used from the smallest test to the largest so results can be compared
	public final static long[] unsignedLongData = new long[] {0,1,63,64,65,126,127,128,8000,16383,16384,16385,16386,
																  1048704, 2097152, 67117056, 268435456, 4295491584l,
																  274911461376l, 17594333528064l, 1126037345796096l,
																  72066390130950144l, //4612248968380809216l,
		                                                          Integer.MAX_VALUE, Long.MAX_VALUE/2 //TODO: bug in largest large signed value, (Long.MAX_VALUE/4)*3
		                                                          };
	public final static int[] unsignedIntData =   new int[]  {0,1,63,64,65,126,127,128,8000,16383,16384,16385,16386,
																 1048704,2097152,268435456, 67117056, 268435456,
		                                                         Integer.MAX_VALUE-1 //must be 1 less so there is room for the nulled form of uInt
		                                                         }; 
	
	public final static CharSequence[] stringData =   new CharSequence[]  {"","","a","a","ab","ab","abc","abc","abcd","abcd",
																	"abcde","abcdef","abcdefg",
																  buildString("g",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK-1),
																  buildString("g",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK-1),
																  buildString("h",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK),
																  buildString("h",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK),
																  buildString("i",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK+1),
																  buildString("i",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK+1),
																  buildString("j",ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK+2),
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
	private PrimitiveWriter pwIOSpeed;
	private float writeDurationIOSpeed;
	public static final int VERY_LONG_STRING_MASK = 0x0F;//0x7F; 
	
	@Test
	public void testBufferSpeed() {
		System.gc();
		Thread.yield();
		
		int fieldSize = 10;
		final int capacity = speedTestSize*fieldSize;
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
		System.gc();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e1) {
		}
		
		/////////////////
		/////////////////
		/////////////////		

		try {			            
			//run this as new thread to block until we connect further down.
			new Thread(new Runnable(){

				@Override
				public void run() {
					ServerSocketChannel serverSocketChannel;
					try {
						serverSocketChannel = ServerSocketChannel.open();
						serverSocketChannel.socket().bind(new InetSocketAddress(8083));
						serverSocketChannel.configureBlocking(true);
						SocketChannel socketChannel = serverSocketChannel.accept();
						pwIOSpeed = new PrimitiveWriter(capacity, new FASTOutputSocketChannel(socketChannel), (int) count, minimizeLatency);
					} catch (IOException e) {
						pwIOSpeed = null;
						e.printStackTrace();
					}
					
				}}).start();
			
			SocketChannel socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);			
			socketChannel.connect(new InetSocketAddress("127.0.0.1", 8083));
			//must loop because we are in NON-blocking mode.
			while (!socketChannel.finishConnect()) {
				Thread.yield();
			}
			
			pr = new PrimitiveReader(new FASTInputSocketChannel(socketChannel));
			
			
		} catch (IOException e) {
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
		System.gc();
		Thread.yield();
	
		
		
		/////////////////
		/////////////////
		/////////////////
		ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
		
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
		System.out.println("    Direct      ByteBuffer: write:"+writeDurationIOSpeed+"ns  read:"+readDuration+"ns  per byte");
		System.gc();
		Thread.yield();
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
		System.gc();
		Thread.yield();
	}


	private final void speedReadTest(PrimitiveReader pr) {
		pr.readPMap(10);
		pr.readLongUnsigned();
		pr.readLongSigned();
		pr.popPMap();
	}


	private final void speedWriteTest(int i) {
		pwIOSpeed.openPMap(10);
		pwIOSpeed.writeLongUnsigned(unsignedLongData[i]);
		pwIOSpeed.writeLongSigned(-unsignedLongData[i]);		
		pwIOSpeed.closePMap();
	}
	
	
	private static String buildString(String value, int i) {
		StringBuffer result = new StringBuffer();
		while (--i>=0) {
			result.append( ((i&1)==0 ? value : ('0'+(i&0x1F))));
		}
		return result.toString();
	}


	@Test 
	public void testNulls() {
		
		int fieldSize = 2;
		int nullLoops = 10000;
		int capacity = speedTestSize*fieldSize*nullLoops;
		
		byte[] buffer = new byte[capacity];		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputByteArray(buffer));
		
		int i = 0;
		while (i<nullLoops) {
			pw.writeNull();
			i++;
		}
		
		pw.flush();
		
		FASTInputByteArray input = new FASTInputByteArray(buffer);
		final PrimitiveReader pr = new PrimitiveReader(input);
		
		i = 0;
		while (i<nullLoops) {
			assertEquals(0,pr.readIntegerUnsigned());
			i++;
		}
		
		int passes = speedTestSize / nullLoops;

		///////////////////////////////////
		//////////////////////////////////
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
			pw.reset();
			int tp = passes*nullLoops;
			
			long start = System.nanoTime();

			int j = tp;
			while (--j>=0) {				
				pw.writeNull();				
			}

			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)pw.totalWritten());
			
			input.reset(buffer);	
			pr.reset();
			
			start = System.nanoTime();
			j = tp;
			while (--j>=0) {
				pr.readIntegerUnsigned();					
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)pw.totalWritten());
		}
		System.out.println("null: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte  totalWritten:"+pw.totalWritten());
		System.gc();
		Thread.yield();
		
	}
	
	@Test 
	public void testIntegers() {
		int fieldSize = 5;
		
		int intSpeedTestSize = 300000;
		
		int passes = intSpeedTestSize / unsignedLongData.length;
		double count = passes*unsignedLongData.length;
		int capacity = intSpeedTestSize*fieldSize;
				
		ByteBuffer buffer = ByteBuffer.allocateDirect(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(capacity, new FASTOutputByteBuffer(buffer),100, false);
		
		int i = 0;
		while (i<unsignedLongData.length) {
			pw.writeLongUnsigned(unsignedLongData[i]);
			pw.writeLongSigned(unsignedLongData[i]);
			pw.writeLongSigned(-unsignedLongData[i++]);
		}
		i=0;
		while (i<unsignedIntData.length) {	
			pw.writeIntegerSigned(unsignedIntData[i]);
			pw.writeIntegerSigned(-unsignedIntData[i]);			
			pw.writeIntegerUnsigned(unsignedIntData[i++]);
		}
		
		pw.flush();
		buffer.flip();
		
		FASTInputByteBuffer input = new FASTInputByteBuffer(buffer);
		final PrimitiveReader pr = new PrimitiveReader(capacity, input, 10);
		
		i = 0;
		while (i<unsignedLongData.length) {
			assertEquals(unsignedLongData[i], pr.readLongUnsigned());
			assertEquals(unsignedLongData[i], pr.readLongSigned());
			assertEquals(-unsignedLongData[i++], pr.readLongSigned());
		}
		i=0;
		while (i<unsignedIntData.length) {	
			assertEquals(unsignedIntData[i], pr.readIntegerSigned());
			assertEquals(-unsignedIntData[i], pr.readIntegerSigned());
			assertEquals(unsignedIntData[i++], pr.readIntegerUnsigned());
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
					pw.writeLongUnsigned(unsignedLongData[i++]);
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
					pr.readLongUnsigned();
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
					pw.writeLongSigned(-unsignedLongData[i++]);
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
					pr.readLongSigned();
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
					pw.writeLongSigned(unsignedLongData[i++]);
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
					pr.readLongSigned();
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
			pr.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeIntegerUnsigned(unsignedIntData[j++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			
			buffer.flip();
			pr.reset();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readIntegerUnsigned();
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
			pw.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeIntegerSigned(-unsignedIntData[j++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			pr.reset();
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readIntegerSigned();
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
			pw.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pw.writeIntegerSigned(unsignedIntData[j++]);
				}
			}
			pw.flush();
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			pr.reset();
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					pr.readIntegerSigned();
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());

		}
		System.out.println("signed integer pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte, total bytes "+buffer.position());		
	
		
	}
	
	private float min(float a, float b) {
		return a<b ? a : b;
	}
	
	@Test 
	public void testASCIIStrings() {
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
					pr.readTextASCII(target, 0, target.length); //TODO: unfair test because we never run like this because it could cause stack overflow.
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("ascii: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte ");
		
	}
	
	@Test 
	public void testUTF8Strings() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
	
		int i = 0;
		while (i<stringData.length) {
			pw.writeIntegerUnsigned(stringData[i].length());
			pw.writeTextUTF(stringData[i++]);
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
			int charCount = pr.readIntegerUnsigned();
			pr.readTextUTF8(charCount, builder);
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
					pw.writeIntegerUnsigned(stringData[i].length());
					pw.writeTextUTF(stringData[i]);
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
					int charCount = pr.readIntegerUnsigned();
					pr.readTextUTF8(target, 0, charCount);
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("utf8: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte ");
		
		
	}
	
	@Test 
	public void testBytes() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter pw = new PrimitiveWriter(new FASTOutputStream(baost));
		
		int i = 0;
		while (i<byteData.length) {
			byte[] data = byteData[i++];
			pw.writeByteArrayData(data, 0, data.length);
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
					byte[] data = byteData[i++];
					pw.writeByteArrayData(data,0,data.length);
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
