//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertTrue;

import java.nio.MappedByteBuffer;

import com.ociweb.jfast.benchmark.TestUtil;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;

public abstract class BaseStreamingTest {

	
	private final float PCT_LIMIT = 200; //if avg is 200 pct above min then fail
	private final float MAX_X_LIMIT = 40f;//if max is 20x larger than avg then fail
	
	private final int sampleSize       = 100000;
	protected final int fields         = 100;
	protected final int fieldsPerGroup = 10;
	protected final int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
	
	protected final int ID_TOKEN_TOGGLE = 0x1;
	protected int maxGroupCount;
	
	protected void tester(int[] types, int[] operators, String label, int charFields, int byteFields) {	
		
		int operationIters = 7;
		int warmup         = 10000;
		int singleCharLength = 128;
		String readLabel = "Read "+label+" groups of "+fieldsPerGroup+" ";
		String writeLabel = "Write "+label+" groups of "+fieldsPerGroup;
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*4));
		maxGroupCount = operationIters*fields/fieldsPerGroup;
		
		
		int[] tokenLookup = TestUtil.buildTokens(fields, types, operators);
		
		byte[] writeBuffer = new byte[streamByteSize];

		///////////////////////////////
		//test the writing performance.
		//////////////////////////////
		
		long byteCount = performanceWriteTest(fields, charFields, byteFields, singleCharLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize,
				writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

		///////////////////////////////
		//test the reading performance.
		//////////////////////////////
		
		performanceReadTest(fields, charFields, byteFields, singleCharLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize, readLabel,
				streamByteSize, maxGroupCount, tokenLookup, byteCount, writeBuffer);
		
	}
	
	
	protected long emptyLoop(int iterations, int fields, int fieldsPerGroup) {
		
		long start = System.nanoTime();
		int i = iterations;
		int g = fieldsPerGroup;
		
		//open new group
		boolean isGroupOpen = false;
		boolean sendNulls = true;
		int token = 0;
		int id = 1;
		while (--i>=0) {
			int f = fields;
					
			while (--f>=0) {
				
				//write field data here
				
				//similar logic to what is found in test cases in order to 
				//process each of the cases in the implementation.  This extra work
				//should be counted as overhead for the test not against the real 
				//performance of the implementation.
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
					
					//special test with constant value.
					if (sendNulls && ((i&0xF)==0) && TokenBuilder.isOptional(token)) {
						id |= (i&ID_TOKEN_TOGGLE)==0?token:f;//nothing
					} else {
						id |= (i&ID_TOKEN_TOGGLE)==0?token:f;//nothing
					}
				} else {
					if (sendNulls && ((f&0xF)==0) && TokenBuilder.isOptional(token)) {
						//System.err.println("write null");
						id |= (i&ID_TOKEN_TOGGLE)==0?token:f;//nothing
					} else {
						id |= (i&ID_TOKEN_TOGGLE)==0?token:f;//nothing
					}
				}
				
				
				
				if (--g<0) {
					
					//close group
					
					g = fieldsPerGroup;
					if (f>0 || i>0) {
						id = id | 1;
						//open new group
						
					}				
				}				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			id = id|0xFF;
		}
		if (id==0) {
			return Long.MIN_VALUE;
		}
		return System.nanoTime() - start;
	}


	protected void printResults(int sampleSize, long maxOverhead, long totalOverhead, long minOverhead, long maxDuration, long totalDuration,
			long minDuration, long byteCount, String label) {
		
		
				float avgOverhead = totalOverhead/(float)sampleSize;
				//System.out.println("Overhead Min:"+minOverhead+" Max:"+maxOverhead+" Avg:"+avgOverhead);
				float avgDuration = totalDuration/(float)sampleSize;
				//System.out.println("Duration Min:"+minDuration+" Max:"+maxDuration+" Avg:"+avgDuration);
								
				float perByteMin = (minDuration-minOverhead)/(float)byteCount;
				float perByteAvg = (avgDuration-avgOverhead)/(float)byteCount;
				float perByteMax = (maxDuration-maxOverhead)/(float)byteCount;
				float pctAvgVsMin = 100f*((perByteAvg/perByteMin)-1);
				String msg = "  PerByte  Min:"+perByteMin+"ns Avg:"+perByteAvg+"ns  <"+pctAvgVsMin+" pct>   Max:"+perByteMax+"ns Total:"+totalDuration+" Overhead:"+totalOverhead;

				System.out.println(label+msg);

			
			}

	protected int groupManagementRead(int fieldsPerGroup, FASTReaderInterpreterDispatch fr, int i, int g, int groupToken, int f, int pmapSize, PrimitiveReader reader) {
		if (--g<0) {
			//close group 
		    int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			fr.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
			
			g = fieldsPerGroup;
			if (f>0 || i>0) {
	
				//open new group
				fr.openGroup(groupToken, pmapSize, reader);
				
			}				
		}
		return g;
	}

	protected int groupManagementWrite(int fieldsPerGroup, FASTWriterInterpreterDispatch fw, int i, int g,
			                             int groupOpenToken, int groupCloseToken, int f, int pmapSize, PrimitiveWriter writer) {
		if (--g<0) {
			//close group
			fw.closeGroup(groupOpenToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
			
			g = fieldsPerGroup;
			if (f>0 || i>0) {
	
				//open new group
				fw.openGroup(groupOpenToken, pmapSize, writer);
				
			}				
		}
		return g;
	}

	protected abstract long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
									DictionaryFactory dcr);


	
	protected void performanceReadTest(int fields, int charFields, int byteFields,  int singleCharLength, int fieldsPerGroup, int maxMPapBytes, int operationIters, int warmup, int sampleSize,
			String label, int streamByteSize, int maxGroupCount, int[] tokenLookup,
			 long byteCount, byte[] writtenData) {


	    		
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
				
				buildInputReader(maxGroupCount, writtenData, (int)byteCount);
				DictionaryFactory dcr;
				
				try {
					int w = warmup+sampleSize;
					while (--w>=0) {
			    
					    dcr = new DictionaryFactory();
			            dcr.setTypeCounts(fields, fields, charFields, byteFields);

						resetInputReader();
						
						//compute overhead
						long overhead = emptyLoop(operationIters, fields, fieldsPerGroup);
						
						//run test, note that timer does not cross virtual call boundary			
						long duration = timeReadLoop(fields, fieldsPerGroup, maxMPapBytes, operationIters,
														tokenLookup, dcr);
					
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
							byteCount, label);
					if (byteCount<totalRead()) {
						System.err.println("warning: reader pulled in more bytes than needed: "+byteCount+" vs "+totalRead());
					}
					
				}
			}

	protected abstract long totalRead();
	protected abstract void resetInputReader();
	protected abstract void buildInputReader(int maxGroupCount, byte[] writtenData, int writtenBytes);
	
	protected  abstract long totalWritten();
	protected abstract void resetOutputWriter();
	protected abstract void buildOutputWriter(int maxGroupCount, byte[] writeBuffer);
	
	
	protected abstract long timeWriteLoop(int fields, 
			int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
			DictionaryFactory dcr);

	
	protected long performanceWriteTest(int fields, int charFields, int byteFields, int singleLength,  int fieldsPerGroup, int maxMPapBytes, int operationIters, int warmup,
			int sampleSize, String writeLabel, int streamByteSize, int maxGroupCount, int[] tokenLookup, byte[] writeBuffer
			) {
				

		
		        long byteCount=0;
		
				long maxOverhead = Long.MIN_VALUE;
				long totalOverhead = 0;
				long minOverhead = Long.MAX_VALUE;
				
				long maxDuration = Long.MIN_VALUE;
				long totalDuration = 0;
				long minDuration = Long.MAX_VALUE;
				
				buildOutputWriter(maxGroupCount, writeBuffer);
				
				try {					
					int w = warmup+sampleSize;
					while (--w>=0) {
					
					    DictionaryFactory dcr = new DictionaryFactory();
					    dcr.setTypeCounts(fields, fields, charFields, byteFields);
					    
						resetOutputWriter();
						
						//compute overhead
						long overhead = emptyLoop(operationIters, fields, fieldsPerGroup);
						
						//run test			
						long duration = timeWriteLoop(fields, fieldsPerGroup, maxMPapBytes, 
								                       operationIters, tokenLookup, dcr);
						
						if (w<sampleSize) {
							if (0==totalDuration) {
								byteCount = totalWritten();
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
							byteCount, writeLabel);
				}
				return byteCount;
			}



	public int[] buildTestDataUnsigned(int count) {
		
		int[] seedData = ReaderWriterPrimitiveTest.unsignedIntData;
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
	

	public long[] buildTestDataUnsignedLong(int count) {
		
		long[] seedData = ReaderWriterPrimitiveTest.unsignedLongData;
		int s = seedData.length;
		int i = count;
		long[] target = new long[count];
		while (--i>=0) {
			target[i] = seedData[--s];
			if (0==s) {
				s=seedData.length;
			}
		}
		return target;
	}


    /**
     * Write null value, must only be used if the field id is one of optional
     * type.
     * @param fw TODO
     */
    public static void write(int token, PrimitiveWriter writer, FASTWriterInterpreterDispatch fw) {
    
        // only optional field types can use this method.
        assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))); 
       // TODO: T, in testing assert(failOnBadArg())
    
        // select on type, each dictionary will need to remember the null was
        // written
        if (0 == (token & (8 << TokenBuilder.SHIFT_TYPE))) {
            // int long
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // int
                int idx = token & fw.intInstanceMask;
                
                //temp solution as the ring buffer is introduce into all the APIs
                FASTRingBuffer.dump(fw.rbRingBufferLocal);
                FASTRingBuffer.addValue(fw.rbRingBufferLocal.buffer, fw.rbRingBufferLocal.mask, fw.rbRingBufferLocal.addPos, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
                FASTRingBuffer.unBlockFragment(fw.rbRingBufferLocal);
                int rbPos = 0;
    
                // hack until all the classes no longer need this method.
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    fw.acceptIntegerUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, fw.rbRingBufferLocal, writer);
                } else {
                    fw.acceptIntegerSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, fw.rbRingBufferLocal, writer);
                }
            } else {
                // long
                int idx = token & fw.longInstanceMask;
                
                fw.writeNullLong(token, idx, writer, fw.longValues);
            }
        } else {
            // text decimal bytes
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // text
                int idx = token & fw.TEXT_INSTANCE_MASK;
                
                fw.writeNullText(token, idx, writer, fw.textHeap);
            } else {
                // decimal bytes
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // decimal
                    int idx = token & fw.intInstanceMask;
                    
                    //temp solution as the ring buffer is introduce into all the APIs     
                    FASTRingBuffer.dump(fw.rbRingBufferLocal);
                    FASTRingBuffer.addValue(fw.rbRingBufferLocal.buffer, fw.rbRingBufferLocal.mask, fw.rbRingBufferLocal.addPos, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
                    FASTRingBuffer.unBlockFragment(fw.rbRingBufferLocal);
                    int rbPos = 0;
                                        // hack until all the classes no longer need this method.
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        fw.acceptIntegerUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, fw.rbRingBufferLocal, writer);
                    } else {
                        fw.acceptIntegerSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, fw.rbRingBufferLocal, writer);
                    } 
    
                    int idx1 = token & fw.longInstanceMask;
                    
                    fw.writeNullLong(token, idx1, writer, fw.longValues);
                } else {
                    // byte
                    fw.writeNullBytes(token, writer, fw.byteHeap, fw.instanceBytesMask);
                }
            }
        }
    
    }
	
}
