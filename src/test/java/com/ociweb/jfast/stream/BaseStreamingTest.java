package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import static org.junit.Assert.*;

public abstract class BaseStreamingTest {

	
	private final float PCT_LIMIT = 80; //if avg is 80 pct above min then fail
	private final float MAX_X_LIMIT = 20f;//if max is 20x larger than avg then fail
	
	
	protected long emptyLoop(int iterations, int fields, int fieldsPerGroup) {
		
		long start = System.nanoTime();
		int i = iterations;
		int g = fieldsPerGroup;
		
		//open new group
		boolean isGroupOpen = false;
		while (--i>=0) {
			int f = fields;
					
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
		if (isGroupOpen) {
			//close group
		}
		return System.nanoTime() - start;
	}


	protected void printResults(int sampleSize, long maxOverhead, long totalOverhead, long minOverhead, long maxDuration, long totalDuration,
			long minDuration, long byteCount, String label, long totalWritten) {
		
				assertTrue(label+" did not write any bytes "+byteCount+" vs "+totalWritten,byteCount>0);
		
				float avgOverhead = totalOverhead/(float)sampleSize;
				//System.out.println("Overhead Min:"+minOverhead+" Max:"+maxOverhead+" Avg:"+avgOverhead);
				float avgDuration = totalDuration/(float)sampleSize;
				//System.out.println("Duration Min:"+minDuration+" Max:"+maxDuration+" Avg:"+avgDuration);
								
				float perByteMin = (minDuration-minOverhead)/(float)byteCount;
				float perByteAvg = (avgDuration-avgOverhead)/(float)byteCount;
				float perByteMax = (maxDuration-maxOverhead)/(float)byteCount;
				float pctAvgVsMin = 100f*((perByteAvg/perByteMin)-1);
				String msg = "  PerByte  Min:"+perByteMin+"ns Avg:"+perByteAvg+"ns  <"+pctAvgVsMin+" pct>   Max:"+perByteMax+"ns ";
				String writtenBytes = "  finished after:"+totalWritten+" bytes";
				System.out.println(label+msg+writtenBytes);

				if (!Double.isNaN(pctAvgVsMin)) {
					assertTrue("Avg is too large vs min:"+pctAvgVsMin+" "+msg,pctAvgVsMin<PCT_LIMIT);
				}
				assertTrue("Max is too large vs avg: "+msg,perByteMax <= (MAX_X_LIMIT*perByteAvg));
								
			}

	public int buildGroupToken(int maxPMapBytes, int repeat) {
		
		return 	0x80000000 |
				maxPMapBytes<<20 |
	            (repeat&0xFFFFF);
		
	}

	protected int groupManagementRead(int fieldsPerGroup, FASTStaticReader fr, int i, int g, int groupToken, int f) {
		if (--g<0) {
			//close group
			fr.closeGroup(groupToken);
			
			g = fieldsPerGroup;
			if (f>0 || i>0) {
	
				//open new group
				fr.openGroup(groupToken);
				
			}				
		}
		return g;
	}

	protected int groupManagementWrite(int fieldsPerGroup, FASTStaticWriter fw, int i, int g, int groupToken, int f) {
		if (--g<0) {
			//close group
			fw.closeGroup(groupToken);
			
			g = fieldsPerGroup;
			if (f>0 || i>0) {
	
				//open new group
				fw.openGroup(groupToken);
				
			}				
		}
		return g;
	}

	protected abstract long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
									FASTStaticReader fr);

	protected void performanceReadTest(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int warmup, int sampleSize,
			String label, int streamByteSize, int maxGroupCount, int[] tokenLookup,
			 long byteCount, byte[] writtenData) {
				

	    		DictionaryFactory dcr = new DictionaryFactory(fields, fields, fields, fields, fields);
	    
				long maxOverhead;
				long totalOverhead;
				long minOverhead;
				long maxDuration;
				long totalDuration;
				long minDuration;
				maxOverhead = Long.MIN_VALUE;
				totalOverhead = 0;
				minOverhead = Long.MAX_VALUE;
				
				maxDuration = Long.MIN_VALUE;
				totalDuration = 0;
				minDuration = Long.MAX_VALUE;
				
				FASTInputByteArray input = new FASTInputByteArray(writtenData);
				
				PrimitiveReader pr = new PrimitiveReader(streamByteSize*10, input, maxGroupCount*10);
				
				assertEquals(0,pr.totalRead());
				try {
					int w = warmup+sampleSize;
					while (--w>=0) {

						input.reset();
						pr.reset();
						
						FASTStaticReader fr = new FASTStaticReader(pr, dcr, tokenLookup);
						
			
						//compute overhead
						long overhead = emptyLoop(operationIters, fields, fieldsPerGroup);
						
						//run test, note that timer does not cross virtual call boundary			
						long duration = timeReadLoop(fields, fieldsPerGroup, maxMPapBytes, operationIters,
														tokenLookup,
														fr);
					
						if (w<sampleSize) {
							
							maxOverhead = Math.max(overhead, maxOverhead);
							totalOverhead += overhead;
							minOverhead = Math.min(overhead, minOverhead);
							
							maxDuration = Math.max(duration, maxDuration);
							totalDuration += duration;
							minDuration = Math.min(duration, minDuration);
							
						}	
					}
					
					
				} finally {
					printResults(sampleSize, maxOverhead, totalOverhead, minOverhead, 
							maxDuration, totalDuration, minDuration,
							byteCount, label, pr.totalRead());
				}
			}

	protected abstract long timeWriteLoop(int fields, 
			int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
			FASTStaticWriter fw);

	protected long performanceWriteTest(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int warmup,
			int sampleSize, String writeLabel, int streamByteSize, int maxGroupCount, int[] tokenLookup, byte[] writeBuffer
			) {
				
		    DictionaryFactory dcr = new DictionaryFactory(fields, fields, fields, fields, fields);
		
		    long byteCount=0;
		
				long maxOverhead = Long.MIN_VALUE;
				long totalOverhead = 0;
				long minOverhead = Long.MAX_VALUE;
				
				long maxDuration = Long.MIN_VALUE;
				long totalDuration = 0;
				long minDuration = Long.MAX_VALUE;
		
		
				FASTOutputByteArray output = new FASTOutputByteArray(writeBuffer);
				
				PrimitiveWriter pw = new PrimitiveWriter(streamByteSize, output, maxGroupCount, false);
				try {
					
					int w = warmup+sampleSize;
					while (--w>=0) {
					
						output.reset();
						pw.reset();
						
						FASTStaticWriter fw = new FASTStaticWriter(pw, dcr, tokenLookup);
						
						//compute overhead
						long overhead = emptyLoop(operationIters, fields, fieldsPerGroup);
						
						//run test			
						long duration = timeWriteLoop(fields, fieldsPerGroup, maxMPapBytes, 
								                       operationIters, tokenLookup,
								                       fw);
						
						if (w<sampleSize) {
							if (0==totalDuration) {
								byteCount = pw.totalWritten();
							}
							
							maxOverhead = Math.max(overhead, maxOverhead);
							totalOverhead += overhead;
							minOverhead = Math.min(overhead, minOverhead);
							
							maxDuration = Math.max(duration, maxDuration);
							totalDuration += duration;
							minDuration = Math.min(duration, minDuration);
							
						}			
					}
					
				} catch (Throwable t) {
					t.printStackTrace();
				} finally {
					printResults(sampleSize, maxOverhead, totalOverhead, minOverhead, 
							maxDuration, totalDuration, minDuration,
							byteCount, writeLabel, pw.totalWritten());
				}
				return byteCount;
			}

	
	
}
