package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingDecimalTest extends BaseStreamingTest {

	final long[] testData     = buildTestDataUnsignedLong(fields);
	//Must double because we may need 1 bit for exponent and another for mantissa
	final int groupToken = buildGroupToken(maxMPapBytes*2,0);//TODO: repeat still unsupported

	boolean sendNulls = true;
	
	
	FASTOutputByteArray output;
	PrimitiveWriter pw;
		
	FASTInputByteArray input;
	PrimitiveReader pr;
	
	//NO PMAP
	//NONE, DELTA, and CONSTANT(non-optional)
	
	//Constant can never be optional but can have pmap.
		
	@Test
	public void decimalTest() {
		int[] types = new int[] {
                  TypeMask.Decimal,
		    	  TypeMask.DecimalOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None,  //no need for pmap
                OperatorMask.Delta, //no need for pmap
                OperatorMask.Copy,
                OperatorMask.Increment,
     //           OperatorMask.Constant, //test runner knows not to use with optional
                OperatorMask.Default
                };
				
		tester(types, operators, "Decimal");
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
					fw.write(token, 1, testData[f]); 
				}
							
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, f);				
			}			
		}
		if (fw.isGroupOpen()) {
			fw.closeGroup(groupToken);
		}
		fw.flush();
		fw.flush();
				
		return System.nanoTime() - start;
	}
	

	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup,
			                      DictionaryFactory dcr) {
		
		FASTStaticReader fr = new FASTStaticReader(pr, dcr, tokenLookup);
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
			
		long none = Integer.MIN_VALUE/2;
		
		int i = operationIters;
		int g = fieldsPerGroup;
		
		fr.openGroup(groupToken);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				if (sendNulls && (f&0xF)==0 && (0!=(token&0x1000000))) {
					int exp = fr.readDecimalExponent(tokenLookup[f], 0);
		     		long man = fr.readDecimalMantissa(tokenLookup[f], none);
					if (none!=man) {
						assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),none, man);
					}
				} else { 
					int exp = fr.readDecimalExponent(tokenLookup[f], 0);
					long man = fr.readDecimalMantissa(tokenLookup[f], none);
					if (testData[f]!=man) {
						assertEquals(testData[f], man);
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

	protected void buildInputReader(int maxGroupCount, byte[] writtenData) {
		input = new FASTInputByteArray(writtenData);
		pr = new PrimitiveReader(4096, input, maxGroupCount*10);
	}
	
}
