package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingIntegerTest extends BaseStreamingTest {
	
	final int groupToken 	  = buildGroupToken(maxMPapBytes,0);//TODO: repeat still unsupported
	final int[] testData     = buildTestDataUnsigned(fields);
	boolean sendNulls = false;
		
	FASTOutputByteArray output;
	PrimitiveWriter pw;
	
	FASTInputByteArray input;
	PrimitiveReader pr;
	
	@AfterClass
	public static void cleanup() {
		System.gc();
	}
	
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
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
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
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
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

	protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
		output = new FASTOutputByteArray(writeBuffer);
		pw = new PrimitiveWriter(4096, output, maxGroupCount, false);
	}


	protected long totalRead() {
		return pr.totalRead();
	}
	
	protected void resetInputReader() {
		input.reset();
		pr.reset();
	}

	protected void buildInputReader(int maxGroupCount, byte[] writtenData, int writtenBytes) {
		input = new FASTInputByteArray(writtenData,writtenBytes);
		pr = new PrimitiveReader(4096, input, maxGroupCount*10);
	}

	
}
