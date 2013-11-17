package com.ociweb.jfast.stream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import org.junit.Test;
import static org.junit.Assert.*;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveReaderWriterTest;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;



public class IntegerStreamingTest {

	
	@Test
	public void integerUnsignedNoOpTest() {
		
		int fields         = 30000;
		int fieldsPerGroup = 10;
		int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
		int operationIters = 7;
		int warmup         = 50;
		int sampleSize     = 1000;
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*4));
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
		
		int[] types = new int[] {
				                  TypeMask.IntegerUnSigned,
				                  TypeMask.IntegerSigned,
								 // TypeMask.IntegerSignedOptional,
								  };
		
		int[] operators = new int[] {
				                      OperatorMask.None, 
									 // OperatorMask.Constant,
									 // OperatorMask.Copy,
									 // OperatorMask.Delta,
									 // OperatorMask.Default,
				                     // OperatorMask.Increment,
				                      };
		
		
		int[] tokenLookup = buildTokens(fields, types, operators);
		int[] testData    = buildTestDataUnsigned(fields);
		
		long maxOverhead = Long.MIN_VALUE;
		long totalOverhead = 0;
		long minOverhead = Long.MAX_VALUE;
		
		long maxDuration = Long.MIN_VALUE;
		long totalDuration = 0;
		long minDuration = Long.MAX_VALUE;
		
		long byteCount=0;
		ByteArrayOutputStream baost = new ByteArrayOutputStream(streamByteSize);
		byte[] writtenData = null;
		
		try {
			FASTOutputStream output = new FASTOutputStream(baost);
			PrimitiveWriter pw = new PrimitiveWriter(streamByteSize, output, maxGroupCount, false);
			
			int w = warmup+sampleSize;
			while (--w>=0) {
			
				baost.reset();
				pw.reset();
				
				FASTWriter fw = new FASTWriter(pw, fields, tokenLookup);
				
				//compute overhead
				long overhead = emptyLoop(operationIters, fields, fieldsPerGroup, testData);
				
				//run test			
				long start = System.nanoTime();
				testingWriteLoop(fields, fieldsPerGroup, maxMPapBytes, operationIters, tokenLookup, testData, fw);
				long duration = System.nanoTime() - start;
			
				pw.flush();
				if (w<sampleSize) {
					if (0==totalDuration) {
						System.out.println("finished warmup...");
						writtenData = baost.toByteArray();
						byteCount = pw.totalWritten();
						assertEquals(byteCount,writtenData.length);
					}
					
					maxOverhead = Math.max(overhead, maxOverhead);
					totalOverhead += overhead;
					minOverhead = Math.min(overhead, minOverhead);
					
					maxDuration = Math.max(duration, maxDuration);
					totalDuration += duration;
					minDuration = Math.min(duration, minDuration);
					
				}			
			}
			
			printResults(sampleSize, maxOverhead, totalOverhead, minOverhead, 
					                 maxDuration, totalDuration, minDuration,
					                 byteCount, "Write Signed/UnsignedInteger NoOpp in groups of "+fieldsPerGroup);
		} finally {
			System.out.println("finished test after:"+baost.size()+" bytes");
		}
		///////////////////////////////
		//test the reading performance.
		//////////////////////////////
		
		maxOverhead = Long.MIN_VALUE;
		totalOverhead = 0;
		minOverhead = Long.MAX_VALUE;
		
		maxDuration = Long.MIN_VALUE;
		totalDuration = 0;
		minDuration = Long.MAX_VALUE;
		
		ByteArrayInputStream baist = new ByteArrayInputStream(writtenData);
		FASTInputStream input = new FASTInputStream(baist);
		
		
		PrimitiveReader pr = new PrimitiveReader(streamByteSize*10, input, maxGroupCount*10);
		
		assertEquals(0,pr.totalRead());
		try {
			int w = warmup+sampleSize;
			while (--w>=0) {
			
				baist.reset();
				
				FASTReader fr = new FASTReader(pr, fields, tokenLookup);
	
				//compute overhead
				long overhead = emptyLoop(operationIters, fields, fieldsPerGroup, testData);
				
				//run test			
				long start = System.nanoTime();
				testingReadLoop(fields, fieldsPerGroup, maxMPapBytes, operationIters, tokenLookup, testData, fr);
				long duration = System.nanoTime() - start;
			
				if (w<sampleSize) {
					if (0==totalDuration) {
						System.out.println("finished warmup...");
					}
					
					maxOverhead = Math.max(overhead, maxOverhead);
					totalOverhead += overhead;
					minOverhead = Math.min(overhead, minOverhead);
					
					maxDuration = Math.max(duration, maxDuration);
					totalDuration += duration;
					minDuration = Math.min(duration, minDuration);
					
				}	
				
			}
			
			printResults(sampleSize, maxOverhead, totalOverhead, minOverhead, 
	                maxDuration, totalDuration, minDuration,
	                byteCount, "Read Signed/UnsignedInteger NoOpp in groups of "+fieldsPerGroup);
		} finally {
			System.out.println("finished test after:"+pr.totalRead()+" bytes");
		}
		
		
	}

	protected void testingWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
			int[] testData, FASTWriter fw) {
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		fw.openGroup(maxMPapBytes);
		
		while (--i>=0) {
			int f = fields;

			while (--f>=0) {
				
				//write field data here
				//all tests will use the token because that bit data will 
				//be needed to properly test each case.  The id to token lookup inside
				//accept appears to only add 1 or 2 ns 
				fw.accept(tokenLookup[f], testData[f]); 
				
				//TODO: how is optional null to be tested.
				//pick a number from the middle and when it matches send null instead.
				//for sending null but need to know that field is nullable.
				//fw.accept(f);
				
			
				if (--g<0) {
					//close group
					fw.closeGroup();
					
					g = fieldsPerGroup;
					if (f>0 || i>0) {
			
						//open new group
						fw.openGroup(maxMPapBytes);
						
					}				
				}				
			}			
		}
		if (fw.isGroupOpen()) {
			fw.closeGroup();
		}
		fw.flush();
	}
	
	protected void testingReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
			int[] testData, FASTReader fr) {
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		
		fr.openGroup(maxMPapBytes);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				//System.err.println(i+" "+f);
				int value = fr.provideInt(tokenLookup[f]);
				
				if (testData[f]!=value) {
					assertEquals(testData[f], value);
				}
				
				//TODO: how is optional null detected and used here?
			
				if (--g<0) {
					//close group
					fr.closeGroup();
					
					g = fieldsPerGroup;
					if (f>0 || i>0) {
			
						//open new group
						fr.openGroup(maxMPapBytes);
						
					}				
				}				
			}			
		}
		if (fr.isGroupOpen()) {
			fr.closeGroup();
		}

	}


	protected void printResults(int sampleSize, long maxOverhead, long totalOverhead, long minOverhead,
			long maxDuration, long totalDuration, long minDuration, long byteCount, String label) {
		System.out.println("---------"+label);
		float avgOverhead = totalOverhead/(float)sampleSize;
		System.out.println("Overhead Min:"+minOverhead+" Max:"+maxOverhead+" Avg:"+avgOverhead);
		float avgDuration = totalDuration/(float)sampleSize;
		System.out.println("Duration Min:"+minDuration+" Max:"+maxDuration+" Avg:"+avgDuration);
		System.out.println("PerByte  Min:"+((minDuration-minOverhead)/(float)byteCount)+
									"ns Avg:"+((avgDuration-avgOverhead)/(float)byteCount)+
								    "ns Max:"+((maxDuration-maxOverhead)/(float)byteCount)+"ns");
	}

	private int[] buildTestDataUnsigned(int count) {
		
		int[] seedData = PrimitiveReaderWriterTest.unsignedIntData;
		int s = seedData.length;
		int i = count;
		int[] target = new int[count];
		while (--i>=0) {
			target[i] = seedData[--s];
			if (0==s) {
				s=seedData.length;
			}
		}
		return target;
	}

	//All tests must copy this block of code so the computation of the overhead is correct
	private long emptyLoop(int iterations, int fields, int fieldsPerGroup, int[] testData) {
		
		long start = System.nanoTime();
		int i = iterations;
		int g = fieldsPerGroup;
		int v;
		//open new group
		boolean isGroupOpen = false;
		while (--i>=0) {
			int f = fields;
					
			while (--f>=0) {
				
				//write field data here
				v = testData[f];
				
				if (--g<0) {
					
					//close group
					
					g = fieldsPerGroup;
					if (f>0 || i>0) {
						
						//open new group
						
					}				
				}				
			}			
		}
		if (isGroupOpen) {
			//close group
		}
		return System.nanoTime() - start;
	}

	private int[] buildTokens(int count, int[] types, int[] operators) {
		int[] lookup = new int[count];
		int typeIdx = types.length-1;
		int opsIdx = operators.length-1;
		while (--count>=0) {
			//two high bits set
			//  6 bit type (must match method)
			//  4 bit operation (must match method)
			// 20 bit instance (MUST be lowest for easy mask and frequent use)
			
			lookup[count] = 0xC0000000 |  
					       (types[typeIdx]<<24) |
					       (operators[opsIdx]<<20) |
					       count;
					
			//find next pattern to be used, rotating over them all.
			
			if (--typeIdx<0) {
				if (--opsIdx<0) {
					opsIdx = operators.length-1;
				}
				typeIdx = types.length-1;
			}
		}
		return lookup;
		
	}
	
}
