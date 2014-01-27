package com.ociweb.jfast.stream;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingBytesTest extends BaseStreamingTest {

	final int fields      		      = 30000;
	final ByteBuffer[] testData    = buildTestData(fields);
	
	final byte[] testConst  = new byte[0];
	final ByteBuffer testContByteBuffer = ByteBuffer.wrap(testConst);
	
	boolean sendNulls        = true;
	
	final byte[][] testDataBytes = buildTestDataBytes(testData);
	
	FASTOutputByteArray output;
	PrimitiveWriter pw;
	
	FASTInputByteArray input;
	PrimitiveReader pr;
	
	@AfterClass
	public static void cleanup() {
		System.gc();
	}
	
	private byte[][] buildTestDataBytes(ByteBuffer[] source) {
		int i = source.length;
		byte[][] result = new byte[i][];
		while (--i>=0) {
			result[i]=  testData[i].array(); 
		}
		return result;
	}

	@Test
	public void bytesTest() {
		int[] types = new int[] {
                   TypeMask.ByteArray,
                   TypeMask.ByteArrayOptional,
				 };
		int[] operators = new int[] {
             //     OperatorMask.None,   
				  OperatorMask.Constant, 
		//		  OperatorMask.Copy,    
		//		  OperatorMask.Default,  
			//	  OperatorMask.Delta,    
           //       OperatorMask.Tail,     
                };

		byteTester(types,operators,"Bytes");
	}
	
	private void byteTester(int[] types, int[] operators, String label) {
		
		int singleCharLength = 1024;
		int fieldsPerGroup = 10;
		int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
		int operationIters = 7;
		int warmup         = 50;
		int sampleSize     = 100;
		int avgFieldSize   = ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK*2+1;
		String readLabel = "Read "+label+" NoOpp in groups of "+fieldsPerGroup;
		String writeLabel = "Write "+label+" NoOpp in groups of "+fieldsPerGroup;
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*avgFieldSize));
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
				
		int[] tokenLookup = HomogeniousRecordWriteReadLongBenchmark.buildTokens(fields, types, operators);
		byte[] writeBuffer = new byte[streamByteSize];
		

		///////////////////////////////
		//test the writing performance.
		//////////////////////////////
		
		long byteCount = performanceWriteTest(fields, singleCharLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize,
				writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

		///////////////////////////////
		//test the reading performance.
		//////////////////////////////
		
		performanceReadTest(fields, singleCharLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize, readLabel,
				streamByteSize, maxGroupCount, tokenLookup, byteCount, writeBuffer);
		
		int i = 0;
		for(ByteBuffer d: testData) {
			i+=d.remaining();
		}
		
		long dataCount = (operationIters * (long)fields * (long)i)/testData.length; 
		
		System.out.println("FullData:"+dataCount+" XmitData:"+byteCount+" compression:"+(byteCount/(float)dataCount));
		
	}

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTWriterDispatch fw = new FASTWriterDispatch(pw, dcr);
		
		long start = System.nanoTime();
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		
		int groupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple, maxMPapBytes, 0);//TODO: repeat still unsupported
		
		fw.openGroup(groupToken, 0);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Constant)) {
					if (sendNulls && ((f&0xF)==0) && (0!=(token&0x1000000))) {
						fw.write(token);
					} else {
						if ((i&1)==0) {
							fw.write(token,testContByteBuffer);
						} else {
							byte[] array = testConst;
							fw.write(token, array, 0 , array.length); 
						}
					}
				} else {
					if (sendNulls && ((f&0xF)==0) && (0!=(token&0x1000000))) {
						fw.write(token);
					} else {
						if ((i&1)==0) {
							fw.write(token,testData[f]);
						} else {
							byte[] array = testDataBytes[f];
							fw.write(token, array, 0 , array.length); 
						}
					}
				}
							
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fw.closeGroup(groupToken);
		}
		fw.flush();
		fw.flush();
		long duration = System.nanoTime() - start;
		return duration;
	}

	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
									DictionaryFactory dcr) {
		
		pr.reset();
		FASTReaderDispatch fr = new FASTReaderDispatch(pr, dcr);
		ByteHeap byteHeap = fr.byteHeap();
		
		long start = System.nanoTime();
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		int groupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple, maxMPapBytes, 0);//TODO: repeat still unsupported
		
		fr.openGroup(groupToken);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				if (TokenBuilder.isOpperator(token, OperatorMask.Constant)) {
					if (sendNulls && (f&0xF)==0 && (0!=(token&0x1000000))) {
						//TODO: why is this int must be CHAR?
	//		     		int value = fr.readInt(tokenLookup[f], Integer.MIN_VALUE);
	//					if (Integer.MIN_VALUE!=value) {
	//						assertEquals(Integer.MIN_VALUE, value);
	//					}
					} else { 
						try {
							int textIdx = fr.readBytes(tokenLookup[f]);						
							
							byte[] tdc = testConst;

							assertTrue("Error:"+TokenBuilder.tokenToString(tokenLookup[f]),
									byteHeap.equals(textIdx, tdc, 0, tdc.length));
				
							
						} catch (Exception e) {
							System.err.println("expected text; "+testData[f]);
							e.printStackTrace();
							throw new FASTException(e);
						}
					}
				} else {
					if (sendNulls && (f&0xF)==0 && (0!=(token&0x1000000))) {
						//TODO: why is this int must be CHAR?
	//		     		int value = fr.readInt(tokenLookup[f], Integer.MIN_VALUE);
	//					if (Integer.MIN_VALUE!=value) {
	//						assertEquals(Integer.MIN_VALUE, value);
	//					}
					} else { 
						try {
							int textIdx = fr.readBytes(tokenLookup[f]);						
							
							byte[] tdc = testDataBytes[f];
							
							assertTrue("Error:"+TokenBuilder.tokenToString(tokenLookup[f]),
									  byteHeap.equals(textIdx, tdc, 0, tdc.length)
									);
													
							
						} catch (Exception e) {
							System.err.println("expected text; "+testData[f]);
							e.printStackTrace();
							throw new FASTException(e);
						}
					}
				}
			
				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0 ) {
			fr.closeGroup(groupToken);
		}
		long duration = System.nanoTime() - start;
		return duration;
	}


	private ByteBuffer[] buildTestData(int count) {
		
		byte[][] seedData = ReaderWriterPrimitiveTest.byteData;
		int s = seedData.length;
		int i = count;
		ByteBuffer[] target = new ByteBuffer[count];
		while (--i>=0) {
			target[i] = ByteBuffer.wrap(seedData[--s]);
			if (0==s) {
				s=seedData.length;
			}
		}
		return target;
	}
	
	public long totalWritten() {
		return pw.totalWritten();
	}
	
	protected void resetOutputWriter() {
		output.reset();
		pw.reset();
	}

	protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
		output = new FASTOutputByteArray(writeBuffer);
		//TODO: this hack is not right
		pw = new PrimitiveWriter(writeBuffer.length, output, maxGroupCount, false);
	}
	
	protected long totalRead() {
		return pr.totalRead();
	}
	
	protected void resetInputReader() {
		input.reset();
		pr.reset();
	}

	protected void buildInputReader(int maxGroupCount, byte[] writtenData, int writtenBytes) {
		input = new FASTInputByteArray(writtenData, writtenBytes);
		//TODO: bug here requires larger buffer.
		pr = new PrimitiveReader(writtenData.length*10, input, maxGroupCount*10);
	}
	
}
