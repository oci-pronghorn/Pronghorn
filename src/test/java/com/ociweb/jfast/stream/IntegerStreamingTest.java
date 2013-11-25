package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReaderWriterTest;



public class IntegerStreamingTest extends BaseStreamingTest {

	final int fields         = 30000;
	final int[] testData    = buildTestDataUnsigned(fields);
	
	@Test
	public void integerUnsignedNoOpTest() {
		int[] types = new int[] {
                  TypeMask.IntegerUnSigned,
				  TypeMask.IntegerUnSignedOptional,
				  };
		tester(types,"UnsignedInteger");
	}
	
	@Test
	public void integerSignedNoOpTest() {
		int[] types = new int[] {
                  TypeMask.IntegerSigned,
				  TypeMask.IntegerSignedOptional,
				  };
		tester(types,"SignedInteger");
	}
	
	
	private void tester(int[] types, String label) {	
		
		int fieldsPerGroup = 10;
		int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
		int operationIters = 7;
		int warmup         = 50;
		int sampleSize     = 1000;
		String readLabel = "Read "+label+" NoOpp in groups of "+fieldsPerGroup;
		String writeLabel = "Write "+label+" NoOpp in groups of "+fieldsPerGroup;
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*4));
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
		

		
		int[] operators = new int[] {
				                      OperatorMask.None, 
									 // OperatorMask.Constant,
									 // OperatorMask.Copy,
									 // OperatorMask.Delta,
									 // OperatorMask.Default,
				                     // OperatorMask.Increment,
				                      };
		
		
		int[] tokenLookup = buildTokens(fields, types, operators);
		
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
				
				if (((f&0xF)==0) && (0!=(token&0x1000000))) {
					fw.write(token);
				} else {
					fw.write(token, testData[f]); 
				}
							
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, f);				
			}			
		}
		if (fw.isGroupOpen()) {
			fw.closeGroup();
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
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				if ((f&0xF)==0 && (0!=(token&0x1000000))) {
		     		int value = fr.readInt(tokenLookup[f], Integer.MIN_VALUE);
					if (Integer.MIN_VALUE!=value) {
						assertEquals(Integer.MIN_VALUE, value);
					}
				} else { 
					int value = fr.readInt(tokenLookup[f], Integer.MAX_VALUE);
					if (testData[f]!=value) {
						assertEquals(testData[f], value);
					}
				}

			
				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f);				
			}			
		}
		if (fr.isGroupOpen()) {
			fr.closeGroup();
		}
		long duration = System.nanoTime() - start;
		return duration;
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
	
}
