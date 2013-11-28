package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveReaderWriterTest;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class IntegerStreamingTest extends BaseStreamingTest {

	final int fields         = 1000;
	final int[] testData    = buildTestDataUnsigned(fields);
	final int fieldsPerGroup = 10;
	final int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
	final int groupToken = buildGroupToken(maxMPapBytes,0);//TODO: repeat still unsupported
	
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
									//  OperatorMask.Copy,
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
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
				
		writeData(fields, fieldsPerGroup, operationIters, tokenLookup, fw, groupToken);
				
		return System.nanoTime() - start;
	}

	protected void writeData(int fields, int fieldsPerGroup, int operationIters,
								int[] tokenLookup,
								FASTStaticWriter fw, int groupToken) {
		int i = operationIters;
		int g = fieldsPerGroup;
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
	}

	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup,
								  FASTStaticReader fr) {
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
			
		readData(fields, fieldsPerGroup, operationIters, tokenLookup, fr);
			
		long duration = System.nanoTime() - start;
		return duration;
	}

	protected void readData(int fields, int fieldsPerGroup, int operationIters,
			                  int[] tokenLookup, FASTStaticReader fr) {
		int i = operationIters;
		int g = fieldsPerGroup;
		
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
	}

	public void caliperRun() {
		
		int operationIters = fields;
		int[] types = new int[] {
				  TypeMask.IntegerUnSigned,
				  TypeMask.IntegerUnSignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None, 
				 // OperatorMask.Constant,
				//  OperatorMask.Copy,
				 // OperatorMask.Delta,
				 // OperatorMask.Default,
               // OperatorMask.Increment,
                };

		int[] tokenLookup = buildTokens(fields, types, operators);
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*4));
		
		byte[] writeBuffer = new byte[streamByteSize];
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
		
		FASTOutputByteArray output = new FASTOutputByteArray(writeBuffer);
		PrimitiveWriter pw = new PrimitiveWriter(streamByteSize, output, maxGroupCount, false);
		FASTStaticWriter fw = new FASTStaticWriter(pw, fields, tokenLookup);
		
		FASTInputByteArray input = new FASTInputByteArray(writeBuffer);
		PrimitiveReader pr = new PrimitiveReader(streamByteSize*10, input, maxGroupCount*10);
		FASTStaticReader fr = new FASTStaticReader(pr, fields, tokenLookup);
				
		writeReadTest(fields, fieldsPerGroup, operationIters, tokenLookup, output, pw, fw, input, pr, fr);
		
	}

	protected void writeReadTest(int fields, int fieldsPerGroup, int operationIters, int[] tokenLookup, FASTOutputByteArray output, PrimitiveWriter pw,
			FASTStaticWriter fw, FASTInputByteArray input, PrimitiveReader pr, FASTStaticReader fr) {
		output.reset();
		pw.reset();
		input.reset();
		pr.reset();
		
		writeData(fields, fieldsPerGroup, operationIters, tokenLookup, fw, groupToken);
		readData(fields, fieldsPerGroup, operationIters, tokenLookup, fr);
		//System.err.println("wrote/read size:"+pr.totalRead());
	}
	

	int[] buildTestDataUnsigned(int count) {
		
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
