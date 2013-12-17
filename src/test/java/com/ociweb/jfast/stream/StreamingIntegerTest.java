package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingIntegerTest extends BaseStreamingTest {

	final int fields         = 1000;
	final int[] testData     = buildTestDataUnsigned(fields);
	final int fieldsPerGroup = 10;
	final int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
	final int groupToken 	  = buildGroupToken(maxMPapBytes,0);//TODO: repeat still unsupported

	boolean sendNulls = false;
	
	
	
	FASTOutputByteArray output;
	PrimitiveWriter pw;
	
	FASTInputByteArray input;
	PrimitiveReader pr;
	
	//NO PMAP
	//NONE, DELTA, and CONSTANT(non-optional)
	
	//Constant can never be optional but can have pmap.
	
	@Test
	public void integerUnsignedTest() {
		int[] types = new int[] {
                  TypeMask.IntegerUnsigned,
		    	  TypeMask.IntegerUnsignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None,  //no need for pmap
                OperatorMask.Delta, //no need for pmap
                OperatorMask.Copy,
                OperatorMask.Increment,
                OperatorMask.Constant, //test runner knows not to use with optional
                OperatorMask.Default
                };
				
		tester(types, operators, "UnsignedInteger");
	}
	
	@Test
	public void integerSignedTest() {
		int[] types = new int[] {
                  TypeMask.IntegerSigned,
				  TypeMask.IntegerSignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None,  //no need for pmap
                OperatorMask.Delta, //no need for pmap
                OperatorMask.Copy,
                OperatorMask.Increment,
                OperatorMask.Constant, //test runner knows not to use with optional
                OperatorMask.Default
                };
		tester(types, operators, "SignedInteger");
	}
	

	
	private void tester(int[] types, int[] operators, String label) {	
		
		int operationIters = 7;
		int warmup         = 50;
		int sampleSize     = 1000;
		String readLabel = "Read "+label+" groups of "+fieldsPerGroup+" ";
		String writeLabel = "Write "+label+" groups of "+fieldsPerGroup;
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*4));
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
		
		
		int[] tokenLookup = HomogeniousRecordWriteReadLongBenchmark.buildTokens(fields, types, operators);
		
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
			int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTStaticWriter fw = new FASTStaticWriter(pw, dcr, tokenLookup);
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
				
		int i = operationIters;
		int g = fieldsPerGroup;
		fw.openGroup(groupToken);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (sendNulls && ((f&0xF)==0) && (0!=(token&0x1000000))) {
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
				
		return System.nanoTime() - start;
	}
	
	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTStaticReader fr = new FASTStaticReader(pr, dcr, tokenLookup);
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
			
		int i = operationIters;
		int g = fieldsPerGroup;
		
		fr.openGroup(groupToken);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				
				if (sendNulls && (f&0xF)==0 && (0!=(token&0x1000000))) {
		     		int value = fr.readInt(tokenLookup[f], Integer.MIN_VALUE);
					if (Integer.MIN_VALUE!=value) {
						assertEquals(Integer.MIN_VALUE, value);
					}
				} else { 
					int value = fr.readInt(tokenLookup[f], Integer.MAX_VALUE);
					if (testData[f]!=value) {
						System.err.println(TokenBuilder.tokenToString(tokenLookup[f]));
						assertEquals(testData[f], value);
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

	public long totalWritten() {
		return pw.totalWritten();
	}
	
	protected void resetOutputWriter() {
		output.reset();
		pw.reset();
	}

	protected void buildOutputWriter(int streamByteSize, int maxGroupCount, byte[] writeBuffer) {
		output = new FASTOutputByteArray(writeBuffer);
		pw = new PrimitiveWriter(streamByteSize, output, maxGroupCount, false);
	}


	protected long totalRead() {
		return pr.totalRead();
	}
	
	protected void resetInputReader() {
		input.reset();
		pr.reset();
	}

	protected void buildInputReader(int streamByteSize, int maxGroupCount, byte[] writtenData) {
		input = new FASTInputByteArray(writtenData);
		pr = new PrimitiveReader(streamByteSize*10, input, maxGroupCount*10);
	}

	
}
