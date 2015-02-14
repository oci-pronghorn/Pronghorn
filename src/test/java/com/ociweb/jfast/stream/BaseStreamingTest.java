//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.benchmark.TestUtil;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.field.LocalHeap;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;

public abstract class BaseStreamingTest {

	
	private final float PCT_LIMIT = 200; //if avg is 200 pct above min then fail
	private final float MAX_X_LIMIT = 40f;//if max is 20x larger than avg then fail
	
	protected int sampleSize           = 100000;
	protected int warmup               = 10000;
	protected int fields               = 100;
	
	protected final int fieldsPerGroup = 10;
	protected final int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
	
	protected final int ID_TOKEN_TOGGLE = 0x1;
	protected int maxGroupCount;
	
	protected void tester(int[] types, int[] operators, String label, int byteFields) {	
		
		int operationIters = 7;
		int warmup         = this.warmup;
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
		
		long byteCount = performanceWriteTest(fields, byteFields, singleCharLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize,
				writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

		///////////////////////////////
		//test the reading performance.
		//////////////////////////////
		
		performanceReadTest(fields, byteFields, singleCharLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize, readLabel,
				streamByteSize, maxGroupCount, tokenLookup, byteCount, writeBuffer);
		
	}
	
	@Deprecated
	public static byte[] byteMe(char[] chars) {
	    int i = chars.length;
	    byte[] result = new byte[i];
	    while (--i>=0) {
	        result[i] = (byte)chars[i];
	    }
	    return result;
	}
	
   @Deprecated
    public static byte[] byteMe(CharSequence chars) {
        int i = chars.length();
        byte[] result = new byte[i];
        while (--i>=0) {
            result[i] = (byte)chars.charAt(i);
        }
        return result;
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
		    
			RingBuffer ringBuffer = RingBuffers.get( fr.ringBuffers, 0);			
			fr.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
			
	    	if (ringBuffer.writeTrailingCountOfBytesConsumed) {
				RingBuffer.writeTrailingCountOfBytesConsumed(ringBuffer, ringBuffer.workingHeadPos.value++); //increment because this is the low-level API calling
				//this updated the head so it must repositioned
			} //MUST be before the assert.
	    	//single length field still needs to move this value up, so this is always done
	    	ringBuffer.bytesWriteLastConsumedBytePos = ringBuffer.byteWorkingHeadPos.value;
	    	
			g = fieldsPerGroup;
			if (f>0 || i>0) {
	
				//open new group
				fr.openGroup(groupToken, pmapSize, reader);
				
			}				
		}
		return g;
	}

	protected int groupManagementWrite(int fieldsPerGroup, FASTWriterInterpreterDispatch fw, int i, int g,
			                             int groupOpenToken, int groupCloseToken, int f, int pmapSize, PrimitiveWriter writer, RingBuffer ring) {
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


	
	protected void performanceReadTest(int fields, int byteFields,  int singleCharLength, int fieldsPerGroup, int maxMPapBytes, int operationIters, int warmup, int sampleSize,
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
			            dcr.setTypeCounts(fields, fields, byteFields, 16, 128);

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

	
	protected long performanceWriteTest(int fields, int byteFields, int byteNominalLength,  int fieldsPerGroup, int maxMPapBytes, int operationIters, int warmup,
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
					    dcr.setTypeCounts(fields, fields, byteFields, 16, byteNominalLength);
					    
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

    public static void writeNullBytes(int token, PrimitiveWriter writer, LocalHeap byteHeap, int instanceMask, FASTWriterInterpreterDispatch disp) {
        
        if (0==(token&(2<<TokenBuilder.SHIFT_OPER))) {
            if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
                //None and Delta and Tail
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(token & instanceMask, byteHeap);              //no pmap, yes change to last value
            } else {
                //Copy and Increment
                int idx = token & instanceMask;
                if (LocalHeap.isNull(idx,byteHeap)) { //stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                    LocalHeap.setNull(idx, byteHeap);
                }  //yes pmap, yes change to last value 
            }
        } else {
            if (0==(token&(1<<TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                
                PrimitiveWriter.writePMapBit((byte)0, writer);  // null for const optional
            } else {    
                //default
                if (LocalHeap.isNull(token & instanceMask,byteHeap)) { //stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                }  //yes pmap,  no change to last value
            }   
        }
        
    }

    @Deprecated
    public static void writeNullText(int token, int idx, PrimitiveWriter writer, LocalHeap byteHeap, FASTWriterInterpreterDispatch disp) {
        if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                // None and Delta and Tail
                PrimitiveWriter.writeNull(writer);
                LocalHeap.setNull(idx, byteHeap); // no pmap, yes change to last value
            } else {
                // Copy and Increment
                if (LocalHeap.isNull(idx,byteHeap)) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                    LocalHeap.setNull(idx, byteHeap);
                } // yes pmap, yes change to last value
            }
        } else {
            if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                
                PrimitiveWriter.writePMapBit((byte)0, writer);  // null for const optional
            } else {
                // default
                if (LocalHeap.isNull(idx,byteHeap)) { // stored value was null;
                    PrimitiveWriter.writePMapBit((byte)0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte)1, writer);
                    PrimitiveWriter.writeNull(writer);
                } // yes pmap, no change to last value
            }
        }
    }

    /**
     * Write null value, must only be used if the field id is one of optional
     * type.
     */
    @Deprecated //each write does its  own null now.
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
                RingBuffer rbRingBufferLocal = new RingBuffer(new RingBufferConfig((byte)2, (byte)2, null, FieldReferenceOffsetManager.RAW_BYTES));
                RingBuffer.dump(rbRingBufferLocal);
                RingBuffer.addValue(rbRingBufferLocal.buffer, rbRingBufferLocal.mask, rbRingBufferLocal.workingHeadPos, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
                RingBuffer ringBuffer = rbRingBufferLocal;
                RingBuffer.publishWrites(ringBuffer);
                int rbPos = 0;
    
                // hack until all the classes no longer need this method.
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    fw.acceptIntegerUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                } else {
                    fw.acceptIntegerSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                }
            } else {
                // long
                int idx = token & fw.longInstanceMask;
                long[] dictionary = fw.rLongDictionary;
                
                if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                    if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                        // None and Delta (both do not use pmap)
                        dictionary[idx] = 0;
                        PrimitiveWriter.writeNull(writer);  
                        // no pmap, yes change to last value
                    } else {
                        // Copy and Increment
                        if (0 == dictionary[idx]) { // stored value was null;
                            PrimitiveWriter.writePMapBit((byte) 0, writer);
                        } else {
                            dictionary[idx] = 0;
                            PrimitiveWriter.writePMapBit((byte) 1, writer);
                            PrimitiveWriter.writeNull(writer);
                        } // yes pmap, yes change to last value
                    }
                } else {
                    if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                        assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                        PrimitiveWriter.writePMapBit((byte) 0, writer);  // null for const optional
                    } else {
                        // default
                        if (dictionary[idx] == 0) { // stored value was null;
                            PrimitiveWriter.writePMapBit((byte) 0, writer);
                        } else {
                            PrimitiveWriter.writePMapBit((byte) 1, writer);
                            PrimitiveWriter.writeNull(writer);
                        } 
                    }
                }
            }
        } else {
            // text decimal bytes
            if (0 == (token & (4 << TokenBuilder.SHIFT_TYPE))) {
                // text
                int idx = token & fw.TEXT_INSTANCE_MASK;
                
               // fw.acceptByteArray(token, writer, fw.byteHeap, rbPos, rbRingBuffer);
                BaseStreamingTest.writeNullText(token, idx, writer, fw.byteHeap, fw);
            } else {
                // decimal bytes
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    // decimal
                    int idx = token & fw.intInstanceMask;
                    
                    //temp solution as the ring buffer is introduce into all the APIs   
                    RingBuffer rbRingBufferLocal = new RingBuffer(new RingBufferConfig((byte)2, (byte)2, null, FieldReferenceOffsetManager.RAW_BYTES));
                    RingBuffer.dump(rbRingBufferLocal);
                    RingBuffer.addValue(rbRingBufferLocal.buffer, rbRingBufferLocal.mask, rbRingBufferLocal.workingHeadPos, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
                    RingBuffer ringBuffer = rbRingBufferLocal;
                    RingBuffer.publishWrites(ringBuffer);
                    int rbPos = 0;
                 
                    // hack until all the classes no longer need this method.
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        fw.acceptIntegerUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                    } else {
                        fw.acceptIntegerSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
                    } 
    
                    int idx1 = token & fw.longInstanceMask;
                    long[] dictionary = fw.rLongDictionary;
                    
                    if (0 == (token & (2 << TokenBuilder.SHIFT_OPER))) {
                        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                            // None and Delta (both do not use pmap)
                            dictionary[idx1] = 0;
                            PrimitiveWriter.writeNull(writer);  
                            // no pmap, yes change to last value
                        } else {
                            // Copy and Increment
                            if (0 == dictionary[idx1]) { // stored value was null;
                                PrimitiveWriter.writePMapBit((byte) 0, writer);
                            } else {
                                dictionary[idx1] = 0;
                                PrimitiveWriter.writePMapBit((byte) 1, writer);
                                PrimitiveWriter.writeNull(writer);
                            } // yes pmap, yes change to last value
                        }
                    } else {
                        if (0 == (token & (1 << TokenBuilder.SHIFT_OPER))) {
                            assert (0 != (token & (1 << TokenBuilder.SHIFT_TYPE))) : "Sending a null constant is not supported";
                            PrimitiveWriter.writePMapBit((byte) 0, writer);  // null for const optional
                        } else {
                            // default
                            if (dictionary[idx1] == 0) { // stored value was null;
                                PrimitiveWriter.writePMapBit((byte) 0, writer);
                            } else {
                                PrimitiveWriter.writePMapBit((byte) 1, writer);
                                PrimitiveWriter.writeNull(writer);
                            } 
                        }
                    }
                } else {
                    // byte
                    BaseStreamingTest.writeNullBytes(token, writer, fw.byteHeap, fw.instanceBytesMask, fw);
                }
            }
        }
    
    }
	
}
