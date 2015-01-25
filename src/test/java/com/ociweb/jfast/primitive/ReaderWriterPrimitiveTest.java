//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.primitive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTInputSocketChannel;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputSocketChannel;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.pronghorn.ring.RingBuffer;

public class ReaderWriterPrimitiveTest {

	private final int speedTestSize = 30000;
	private final int testCycles = 5;

	//These common test values are used from the smallest test to the largest so results can be compared
	public final static long[] unsignedLongData = new long[] {0,1,63,64,65,126,127,128,8000,16383,16384,16385,16386,
																  1048704, 2097152, 67117056, 268435456, 4295491584l,
																  274911461376l, 17594333528064l, 1126037345796096l,
																  72066390130950144l, 
		                                                          Integer.MAX_VALUE, Long.MAX_VALUE/2 , 
		                                                          (Long.MAX_VALUE/2)+1  //Max supported positive value
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
	
	public final static byte[][] stringDataBytes = flatASCII(stringData); 
	
	
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
	private PrimitiveWriter writer;
	private float writeDurationIOSpeed;
	public static final int VERY_LONG_STRING_MASK = 0x0F;//0x7F; 
	
	//skipped, causes problems with grabbing the socket
	//not needed for coverage but was helpful speed comparisons
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
		writer = new PrimitiveWriter(capacity, new FASTOutputStream(baost),minimizeLatency);
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		PrimitiveReader pr = new PrimitiveReader(2048, input, 32);
		
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
			PrimitiveWriter.flush(writer);
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
		
		//will increment the port as needed in case test framework is already using this port
		//needed in larger test environments when tests run in parallel.
		final AtomicInteger myTestPort = new AtomicInteger(8083);
		
		try {			            
			//run this as new thread to block until we connect further down.
			new Thread(new Runnable(){

				@Override
				public void run() {
					ServerSocketChannel serverSocketChannel = null;
					try {
						int repeat = 100;//try this many ports
						do {
							try {
								serverSocketChannel = ServerSocketChannel.open();
								serverSocketChannel.socket().bind(new InetSocketAddress(myTestPort.get()));
								repeat = 0;
							} catch(BindException be) {
								if (-1==be.getMessage().indexOf("in use")) {
									throw be;
								} else {
									System.err.println("port:"+myTestPort+" in use. Trying another.");
									myTestPort.incrementAndGet();
								}
							}
						} while (--repeat>0);
						serverSocketChannel.configureBlocking(true);
						SocketChannel socketChannel = serverSocketChannel.accept();
						writer = new PrimitiveWriter(capacity, new FASTOutputSocketChannel(socketChannel), minimizeLatency);
					} catch (IOException e) {
						writer = null;
						e.printStackTrace();
					}
					
				}}).start();
			
			SocketChannel socketChannel = SocketChannel.open();
			socketChannel.configureBlocking(false);			
			//typical setup for low latency will turn on the nodelay option
//			socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
			socketChannel.connect(new InetSocketAddress("127.0.0.1", myTestPort.get()));
			//must loop because we are in NON-blocking mode.
			while (!socketChannel.finishConnect()) {
				Thread.yield();
			}
			
			pr = new PrimitiveReader(2048, new FASTInputSocketChannel(socketChannel), 32);
			
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
					PrimitiveWriter.flush(writer);
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
				    //this test can not support blocking feed data so pre-fetch it now.
				    boolean ok = true;
				    while (ok) {
    				    try {
    				        PrimitiveReader.fetch(pr);
    				        ok = false;
    				    } catch (Exception e) {
    				        ok = true;
    				    }
				    }
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
		
		writer = new PrimitiveWriter(capacity, new FASTOutputByteBuffer(buffer), minimizeLatency);
		pr = new PrimitiveReader(2048, new FASTInputByteBuffer(buffer), 32);
		
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
			PrimitiveWriter.flush(writer);
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
		writer = new PrimitiveWriter(capacity, byteArrayOutput, minimizeLatency);
		
		FASTInputByteArray byteArrayInput = new FASTInputByteArray(bufferArray);
		pr = new PrimitiveReader(2048, byteArrayInput, 32);
		
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
			PrimitiveWriter.flush(writer);
			long duration = System.nanoTime()-start;
			totalBytesWritten = baost.toByteArray().length;
			writeDurationIOSpeed =  min(writeDurationIOSpeed, duration/(float)totalBytesWritten);
			
			//must reset bytes back to beginning
			byteArrayInput.reset();
			
			PrimitiveReader.reset(pr);
			
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
		writer = null;//no longer needed
		System.out.println("                ByteArray: write:"+writeDurationIOSpeed+"ns  read:"+readDuration+"ns  per byte");
		System.gc();
		Thread.yield();
	}


	private static byte[][] flatASCII(CharSequence[] stringdata) {
	    int i = stringdata.length;
	    byte[][] result = new byte[i][];
	    while (--i>=0) {
	        int j = stringdata[i].length();
	        byte[] temp = new byte[stringdata[i].length()];
	        while (--j>=0) {
	            temp[j]=(byte)(stringdata[i].charAt(j));
	        }
	        result[i] = temp;	        
	        
	    }
	    return result;
    }


    private final void speedReadTest(PrimitiveReader reader) {
	    PrimitiveReader.openPMap(10, reader);
	    PrimitiveReader.readLongUnsigned(reader);
	    PrimitiveReader.readLongSigned(reader);
	    PrimitiveReader.closePMap(reader);
	}


	private final void speedWriteTest(int i) {
		PrimitiveWriter.openPMap(10, writer);
		PrimitiveWriter.writeLongUnsigned(unsignedLongData[i], writer);
		PrimitiveWriter.writeLongSigned(-unsignedLongData[i], writer);		
		PrimitiveWriter.closePMap(writer);
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
		int nullLoops = 1000;
		int capacity = speedTestSize*fieldSize*nullLoops;
		
		byte[] buffer = new byte[capacity];		
		final PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputByteArray(buffer), false);
		
		int i = 0;
		while (i<nullLoops) {
			PrimitiveWriter.writeNull(writer);
			i++;
		}
		
		PrimitiveWriter.flush(writer);
		
		FASTInputByteArray input = new FASTInputByteArray(buffer);
		final PrimitiveReader reader = new PrimitiveReader(2048, input, 32);
		
		i = 0;
		while (i<nullLoops) {
			assertEquals(0,PrimitiveReader.readIntegerUnsigned(reader));
			i++;
		}
		
		int passes = speedTestSize / nullLoops;

		///////////////////////////////////
		//////////////////////////////////
		
		float writeDuration = Float.MAX_VALUE;
		float readDuration = Float.MAX_VALUE;
		
		int cycles = testCycles;
		while (--cycles>=0) {
		    PrimitiveWriter.reset(writer);
			int tp = passes*nullLoops;
			
			long start = System.nanoTime();

			int j = tp;
			while (--j>=0) {				
				PrimitiveWriter.writeNull(writer);				
			}

			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)PrimitiveWriter.totalWritten(writer));
			
			input.reset(buffer);	
			PrimitiveReader.reset(reader);
			
			start = System.nanoTime();
			j = tp;
			while (--j>=0) {
				PrimitiveReader.readIntegerUnsigned(reader);					
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)PrimitiveWriter.totalWritten(writer));
		}
		System.out.println("null: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte  totalWritten:"+PrimitiveWriter.totalWritten(writer));
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
		
		final PrimitiveWriter writer = new PrimitiveWriter(capacity, new FASTOutputByteBuffer(buffer),false);
		
		int i = 0;
		while (i<unsignedLongData.length) {
			PrimitiveWriter.writeLongUnsigned(unsignedLongData[i], writer);
			PrimitiveWriter.writeLongSigned(unsignedLongData[i], writer);
			PrimitiveWriter.writeLongSigned(-unsignedLongData[i], writer);
			
			PrimitiveWriter.writeLongSignedOptional(unsignedLongData[i], writer);
			if (0!=unsignedLongData[i]) {
				PrimitiveWriter.writeLongSignedOptional(-unsignedLongData[i], writer);
			}
			i++;
		}
		i=0;
		while (i<unsignedIntData.length) {	
			PrimitiveWriter.writeIntegerSigned(unsignedIntData[i], writer);
			PrimitiveWriter.writeIntegerSigned(-unsignedIntData[i], writer);			
			PrimitiveWriter.writeIntegerUnsigned(unsignedIntData[i], writer);
			
			PrimitiveWriter.writeIntegerSignedOptional(unsignedIntData[i], writer);
			if (0!=unsignedIntData[i]) {
				PrimitiveWriter.writeIntegerSignedOptional(-unsignedIntData[i], writer);
			}
			i++;
		}
		
		PrimitiveWriter.flush(writer);
		buffer.flip();
		
		FASTInputByteBuffer input = new FASTInputByteBuffer(buffer);
		final PrimitiveReader reader = new PrimitiveReader(capacity, input, 10);
		
		i = 0;
		while (i<unsignedLongData.length) {
			assertEquals(unsignedLongData[i], PrimitiveReader.readLongUnsigned(reader));
			assertEquals(unsignedLongData[i], PrimitiveReader.readLongSigned(reader));
			assertEquals(-unsignedLongData[i], PrimitiveReader.readLongSigned(reader));
			
			assertEquals(unsignedLongData[i], PrimitiveReader.readLongSigned(reader)-1);
			if (0!=unsignedLongData[i]) {
				assertEquals(-unsignedLongData[i], PrimitiveReader.readLongSigned(reader));
			}
			i++;
		}
		i=0;
		while (i<unsignedIntData.length) {	
			assertEquals(unsignedIntData[i], PrimitiveReader.readIntegerSigned(reader));
			assertEquals(-unsignedIntData[i], PrimitiveReader.readIntegerSigned(reader));
			assertEquals(unsignedIntData[i], PrimitiveReader.readIntegerUnsigned(reader));
			
			assertEquals(unsignedIntData[i], PrimitiveReader.readIntegerSigned(reader)-1);
			if (0!=unsignedIntData[i]) {
				assertEquals(-unsignedIntData[i], PrimitiveReader.readIntegerSigned(reader));
			}
			i++;
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
					PrimitiveWriter.writeLongUnsigned(unsignedLongData[i++], writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					PrimitiveReader.readLongUnsigned(reader);
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
					PrimitiveWriter.writeLongSigned(-unsignedLongData[i++], writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					PrimitiveReader.readLongSigned(reader);
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
					PrimitiveWriter.writeLongSigned(unsignedLongData[i++], writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				i = 0;
				while (i<unsignedLongData.length) {
					PrimitiveReader.readLongSigned(reader);
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
			PrimitiveReader.reset(reader);
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveWriter.writeIntegerUnsigned(unsignedIntData[j++], writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			
			buffer.flip();
			PrimitiveReader.reset(reader);
			
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveReader.readIntegerUnsigned(reader);
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
			PrimitiveWriter.reset(writer);
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveWriter.writeIntegerSigned(-unsignedIntData[j++], writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			PrimitiveReader.reset(reader);
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveReader.readIntegerSigned(reader);
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
			PrimitiveWriter.reset(writer);
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveWriter.writeIntegerSigned(unsignedIntData[j++], writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			PrimitiveReader.reset(reader);
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveReader.readIntegerSigned(reader);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());

		}
		System.out.println("signed integer pos: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte, total bytes "+buffer.position());		
		
		//////////////////////////////////
		//////////////////////////////////

		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		cycles = testCycles;
		while (--cycles>=0) {
			buffer.clear();
			PrimitiveWriter.reset(writer);
			long start = System.nanoTime();
			int p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveWriter.writeIntegerSigned(unsignedIntData[j++], writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)buffer.position());
			buffer.flip();
			PrimitiveReader.reset(reader);
			start = System.nanoTime();
			 p = passes;
			while (--p>=0) {
				int j = 0;
				while (j<unsignedIntData.length) {
					PrimitiveReader.readSkipByStop(reader);
					j++;
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)buffer.position());

		}
		System.out.println("skipByStop                 read:"+readDuration+"ns per byte, total bytes "+buffer.position());		
	
		
	}
	
	private float min(float a, float b) {
		return a<b ? a : b;
	}
	
	@Test 
	public void testASCIIStrings() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputStream(baost), false);
	
		int i = 0;
		while (i<stringData.length) {
		    byte[] value = stringDataBytes[i++];
			PrimitiveWriter.writeTextASCII(value, 0, value.length, 0xFFFFFF, writer);
		}
		
		PrimitiveWriter.flush(writer);
		
		
		byte[] byteArray = baost.toByteArray();
		assertEquals(PrimitiveWriter.totalWritten(writer),byteArray.length);
		
		FASTInputByteArray input = new FASTInputByteArray(byteArray);
		final PrimitiveReader reader = new PrimitiveReader(2048, input, 32);
		
		i = 0;
		StringBuilder builder = new StringBuilder();
		while (i<stringData.length) {
			builder.setLength(0);
			PrimitiveReader.readTextASCII(builder, reader);
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
		
		byte[] target = new byte[ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK*3];
		int cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			assertTrue(baost.size()==0);
			
			long start = System.nanoTime();
			int p = passes;			
			while (--p>=0) {
				i = trunkTestLimit;
				while (--i>=0) {
					byte[] value = stringDataBytes[i];
		            PrimitiveWriter.writeTextASCII(value, 0, value.length, 0xFFFFFF, writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
			
			//new bigger byte array for all the passes.
			input.reset(baost.toByteArray());
			
			start = System.nanoTime();
			p = passes;
			while (--p>=0) {
				i = trunkTestLimit;
				while (--i>=0) {
					PrimitiveReader.readTextASCII(target, 0, target.length, reader);
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
		
		final PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputStream(baost), false);
	
		int i = 0;
		while (i<stringData.length) {
			PrimitiveWriter.writeIntegerUnsigned(stringData[i].length(), writer);
			CharSequence temp = stringData[i++];
			PrimitiveWriter.ensureSpace(temp.length(),writer);
            
            //convert from chars to bytes
            //writeByteArrayData()
            int len = temp.length();
            int limit = writer.limit;
            int c = 0;
            while (c < len) {
                limit = RingBuffer.encodeSingleChar((int) temp.charAt(c++), writer.buffer, 0xFFFFFFFF, limit);
            }
            writer.limit = limit;
		}
		
		PrimitiveWriter.flush(writer);
		
		
		byte[] byteArray = baost.toByteArray();
		assertEquals(PrimitiveWriter.totalWritten(writer),byteArray.length);
		
		FASTInputByteArray input = new FASTInputByteArray(byteArray);
		final PrimitiveReader reader = new PrimitiveReader(2048, input, 32);
		
		i = 0;
		StringBuilder builder = new StringBuilder();
		while (i<stringData.length) {
			builder.setLength(0);
			int len = PrimitiveReader.readIntegerUnsigned(reader);
            {
                byte[] temp = new byte[len];
                
                //read bytes into temp array
                PrimitiveReader.readByteData(temp,0,len,reader);//read bytes
                
                //convert bytes into chars
                long charAndPos = 0;        
                while (charAndPos>>32 < len  ) {
                    charAndPos = RingBuffer.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                    builder.append((char)charAndPos);

                }
            }
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
					PrimitiveWriter.writeIntegerUnsigned(stringData[i].length(), writer);
					CharSequence temp = stringData[i];
					PrimitiveWriter.ensureSpace(temp.length(),writer);
                    
                    //convert from chars to bytes
                    //writeByteArrayData()
                    int len = temp.length();
                    int limit = writer.limit;
                    int c = 0;
                    while (c < len) {
                        limit = RingBuffer.encodeSingleChar((int) temp.charAt(c++), writer.buffer, 0xFFFFFFFF, limit);
                    }
                    writer.limit = limit;
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
			
			//new bigger byte array for all the passes.
			input.reset(baost.toByteArray());
			
			start = System.nanoTime();
			p = passes;
			while (--p>=0) {
				i = trunkTestLimit;
				while (--i>=0) {
					int len = PrimitiveReader.readIntegerUnsigned(reader);
                    int offset = 0;
					{ 
                        byte[] temp = new byte[len];
                        
                        PrimitiveReader.readByteData(temp,0,len,reader); //read bytes
                        
                        long charAndPos = 0;        
                        while (charAndPos>>32 < len  ) {//convert bytes to chars
                            charAndPos = RingBuffer.decodeUTF8Fast(temp, charAndPos, Integer.MAX_VALUE);
                            target[offset++]=(char)charAndPos;
                        }
                    }
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("utf8: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte ");
		
		///////////////////////////////////
		//////////////////////////////////
		input.reset();
		PrimitiveReader.reset(reader);
		PrimitiveWriter.reset(writer);
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;
		
		//limit this down to the size of the other tests to get a better comparison
		//makes the bytesPerWrite about the same size as the others
		trunkTestLimit = stringData.length;//Much faster with larger strings.
		
		target = new char[ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK*3];
		cycles = testCycles;
		while (--cycles>=0) {
			baost.reset();
			assertTrue(baost.size()==0);
			
			long start = System.nanoTime();
			int p = passes;			
			while (--p>=0) {
				i = trunkTestLimit;
				while (--i>=0) {
					PrimitiveWriter.writeIntegerUnsigned(stringData[i].length(), writer);
					CharSequence temp = stringData[i];
					PrimitiveWriter.ensureSpace(temp.length(),writer);
                    
                    //convert from chars to bytes
                    //writeByteArrayData()
                    int len = temp.length();
                    int limit = writer.limit;
                    int c = 0;
                    while (c < len) {
                        limit = RingBuffer.encodeSingleChar((int) temp.charAt(c++), writer.buffer, 0xFFFFFFFF, limit);
                    }
                    writer.limit = limit;
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
			
			//new bigger byte array for all the passes.
			input.reset(baost.toByteArray());
			
			start = System.nanoTime();
			p = passes;
			while (--p>=0) {
				i = trunkTestLimit;
				while (--i>=0) {
					int len = PrimitiveReader.readIntegerUnsigned(reader);
					PrimitiveReader.readSkipByLengthByt(len, reader);
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("readSkipByLengthUTF          read:"+readDuration+"ns per byte ");
		
	}
	
	@Test 
	public void testBytes() {
		int fieldSize = 3;
		int capacity = speedTestSize*fieldSize;
		
		ByteArrayOutputStream baost = new ByteArrayOutputStream(capacity);
		
		final PrimitiveWriter writer = new PrimitiveWriter(4096, new FASTOutputStream(baost), false);
		
		int i = 0;
		while (i<byteData.length) {
			byte[] data = byteData[i++];
			PrimitiveWriter.writeByteArrayData(data, 0, data.length, writer);
		}
		
		PrimitiveWriter.flush(writer);
		
		FASTInputStream input = new FASTInputStream(new ByteArrayInputStream(baost.toByteArray()));
		final PrimitiveReader reader = new PrimitiveReader(2048, input, 32);
		
		int largest = 0;
		i = 0;
		while (i<byteData.length) {
			int length = byteData[i].length;
			if (length>largest) {
				largest = length;
			}
			byte[] target = new byte[length];
			PrimitiveReader.readByteData(target, 0, length, reader);
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
					PrimitiveWriter.writeByteArrayData(data,0,data.length, writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration =  min(writeDuration, (System.nanoTime()-start)/(float)baost.size());
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));
			
			start = System.nanoTime();
			p = passes;
			while (--p>=0) {
				i = 0;
				while (i<byteData.length) {
					PrimitiveReader.readByteData(tempTarget, 0, byteData[i++].length, reader);
				}
			}
			readDuration = min(readDuration, (System.nanoTime()-start)/(float)baost.size());
		}
		System.out.println("byteArray: write:"+writeDuration+"ns  read:"+readDuration+"ns per byte");
		
		///////////////////////////////////
		//////////////////////////////////
		PrimitiveWriter.reset(writer);
		PrimitiveReader.reset(reader);
		
		writeDuration = Float.MAX_VALUE;
		readDuration = Float.MAX_VALUE;

		cycles = testCycles;
		while (--cycles >= 0) {
			baost.reset();
			long start = System.nanoTime();
			int p = passes;
			while (--p >= 0) {
				i = 0;
				while (i < byteData.length) {
					byte[] data = byteData[i++];
					PrimitiveWriter.writeByteArrayData(data, 0, data.length, writer);
				}
			}
			PrimitiveWriter.flush(writer);
			writeDuration = min(writeDuration, (System.nanoTime() - start) / (float) baost.size());
			input.replaceStream(new ByteArrayInputStream(baost.toByteArray()));

			start = System.nanoTime();
			p = passes;
			while (--p >= 0) {
				PrimitiveReader.readSkipByLengthByt(byteData.length, reader);
			}
			readDuration = min(readDuration, (System.nanoTime() - start) / (float) baost.size());
		}
		System.out.println("skipByLengthByt   read:" + readDuration + "ns per byte");
		
	}
	
}
