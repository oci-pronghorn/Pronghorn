package com.ociweb.jfast.stream;

import java.io.ByteArrayOutputStream;

import org.junit.Test;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveWriter;
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
		
		int[] types = new int[] {
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
		

		
		long maxOverhead = Long.MIN_VALUE;
		long totalOverhead = 0;
		long minOverhead = Long.MAX_VALUE;
		
		long maxDuration = Long.MIN_VALUE;
		long totalDuration = 0;
		long minDuration = Long.MAX_VALUE;
		
		long byteCount=0;
		ByteArrayOutputStream baost = new ByteArrayOutputStream(streamByteSize);
		FASTOutputStream output = new FASTOutputStream(baost);
		PrimitiveWriter pw = new PrimitiveWriter(streamByteSize, output, operationIters*fields/fieldsPerGroup, false);

		
		int w = warmup+sampleSize;
		while (--w>=0) {
		
			baost.reset();
			pw.reset();
			FASTWriter fw = new FASTWriter(pw, fields, tokenLookup);
			
			//compute overhead
			long overhead = emptyLoop(operationIters, fields, fieldsPerGroup);
			
			//run test			
			long start = System.nanoTime();
			int i = operationIters;
			if (i<3) {
				throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
			}
			int g = fieldsPerGroup;
			fw.openGroup(maxMPapBytes);
			
			while (--i>=0) {
				int f = fields;
				
				//open new group
				//fw.openGroup(maxMPapBytes);
				
				//System.out.println("top "+i+" "+pw.totalWritten()+" f:"+f);
				while (--f>=0) {
					
					//write field data here
					fw.accept(f, f); //TODO: need better test data, also how is optional null to be tested.
					
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
			fw.flush();
			long duration = System.nanoTime() - start;
		
			if (w<sampleSize) {
				if (0==totalDuration) {
					System.out.println("finished warmup...");
					byteCount = baost.toByteArray().length;
					System.out.println(byteCount+" vs "+pw.totalWritten());
				}
				
				maxOverhead = Math.max(overhead, maxOverhead);
				totalOverhead += overhead;
				minOverhead = Math.min(overhead, minOverhead);
				
				maxDuration = Math.max(duration, maxDuration);
				totalDuration += duration;
				minDuration = Math.min(duration, minDuration);
				
			}
			
		}
		
		double avgOverhead = totalOverhead/(double)sampleSize;
		System.out.println("Overhead Min:"+minOverhead+" Max:"+maxOverhead+" Avg:"+avgOverhead);
		double avgDuration = totalDuration/(double)sampleSize;
		System.out.println("Duration Min:"+minDuration+" Max:"+maxDuration+" Avg:"+avgDuration);
		System.out.println("ByteCount:"+byteCount+" max size:"+streamByteSize);
		System.out.println("PerByte  Min:"+((minDuration-minOverhead)/(float)byteCount)+
								   "ns Max:"+((maxDuration-maxOverhead)/(float)byteCount)+"ns"+
								   "ns Avg:"+((avgDuration-avgOverhead)/(float)byteCount)+"ns");
		
		
	}

	//All tests must copy this block of code so the computation of the overhead is correct
	private long emptyLoop(int iterations, int fields, int fieldsPerGroup) {
		
		long start = System.nanoTime();
		int i = iterations;
		int g = fieldsPerGroup;
		while (--i>=0) {
			int f = fields;
			
			//open new group
			
			while (--f>=0) {
				
				//write field data here
				
				if (--g<0) {
					
					//close group
					
					g = fieldsPerGroup;
					if (f>0 || i>0) {
						
						//open new group
						
					}				
				}				
			}			
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
