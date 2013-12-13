package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveReaderWriterTest;



public class TextStreamingTest extends BaseStreamingTest {

	final int fields         = 30000;
	final CharSequence[] testData    = buildTestData(fields);
	
	@Test
	public void asciiTest() {
		int[] types = new int[] {
                   TypeMask.TextASCII,
                   TypeMask.TextASCIIOptional,
				 };
		tester(types,"ASCII");
	}
	
	@Test
	public void utf8Test() {
		int[] types = new int[] {
				  TypeMask.TextUTF8,
				  TypeMask.TextUTF8Optional,
				 };
		tester(types,"UTF8");
	}
	
	private void tester(int[] types, String label) {
		
		int fieldsPerGroup = 10;
		int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
		int operationIters = 7;
		int warmup         = 50;
		int sampleSize     = 100;
		int avgFieldSize   = PrimitiveReader.VERY_LONG_STRING_MASK*2+1;
		String readLabel = "Read "+label+"Text NoOpp in groups of "+fieldsPerGroup;
		String writeLabel = "Write "+label+"Text NoOpp in groups of "+fieldsPerGroup;
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*avgFieldSize));
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
		
		int[] operators = new int[] {
				                      OperatorMask.None, 
									 // OperatorMask.Constant,
									 // OperatorMask.Copy,
									 // OperatorMask.Delta,
									 // OperatorMask.Default,
				                     // OperatorMask.Increment,
				                      };
		
		
		int[] tokenLookup = HomogeniousRecordWriteReadBenchmark.buildTokens(fields, types, operators);
		byte[] writeBuffer = new byte[streamByteSize];
		

		///////////////////////////////
		//test the writing performance.
		//////////////////////////////
		
		long byteCount = performanceWriteTest(fields, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize,
				writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

		///////////////////////////////
		//test the reading performance.
		//////////////////////////////
		
		performanceReadTest(fields, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize, readLabel,
				streamByteSize, maxGroupCount, tokenLookup, byteCount, writeBuffer);
		
	}

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, FASTStaticWriter fw) {
		long start = System.nanoTime();
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		
		int groupToken = buildGroupToken(maxMPapBytes,0);//TODO: repeat still unsupported
		
		fw.openGroup(groupToken);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (false && ((f&0xF)==0) && (0!=(token&0x1000000))) {
					fw.write(token);
				} else {
					fw.write(token, testData[f]); 
				}
							
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, f);				
			}			
		}
		if (fw.isGroupOpen()) {
			fw.closeGroup(groupToken);
		}
		fw.flush();
		fw.flush();
		long duration = System.nanoTime() - start;
		return duration;
	}

	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters, int[] tokenLookup,
								 FASTStaticReader fr) {
		long start = System.nanoTime();
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		int groupToken = buildGroupToken(maxMPapBytes,0);//TODO: repeat still unsupported
		
		fr.openGroup(groupToken);
		
		char[] target = new char[PrimitiveReader.VERY_LONG_STRING_MASK*3];
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				if (false && (f&0xF)==0 && (0!=(token&0x1000000))) {
					//TODO: why is this int must be CHAR?
//		     		int value = fr.readInt(tokenLookup[f], Integer.MIN_VALUE);
//					if (Integer.MIN_VALUE!=value) {
//						assertEquals(Integer.MIN_VALUE, value);
//					}
				} else { 
					try {
						int len = fr.readChars(tokenLookup[f], target, 0);
						CharSequence expected = testData[f];
						if (len!=testData[f].length()) {
							assertEquals(expected,new String(target,0,len));
						} else {
							int j = len;
							while (--j>=0) {
								if (expected.charAt(j)!=target[j]) {
									assertEquals(expected,new String(target,0,len));
								}
							}						
						}	
					} catch (Exception e) {
						System.err.println("xxx: expected "+testData[f]);
						throw new FASTException(e);
					}
				}

			
				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f);				
			}			
		}
		if (fr.isGroupOpen()) {
			fr.closeGroup(groupToken);
		}
		long duration = System.nanoTime() - start;
		return duration;
	}


	private CharSequence[] buildTestData(int count) {
		
		CharSequence[] seedData = PrimitiveReaderWriterTest.stringData;
		int s = seedData.length;
		int i = count;
		CharSequence[] target = new CharSequence[count];
		while (--i>=0) {
			target[i] = seedData[--s];
			if (0==s) {
				s=seedData.length;
			}
		}
		return target;
	}
	
}
