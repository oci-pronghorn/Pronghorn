//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingDecimalTest extends BaseStreamingTest {

	final long[] testData     = buildTestDataUnsignedLong(fields);
	final int   testExpConst = 0;
	final long   testMantConst = 0;
	
	//Must double because we may need 1 bit for exponent and another for mantissa
	final int groupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple, maxMPapBytes*2, 0);//TODO: repeat still unsupported

	boolean sendNulls = true;
	
	
	FASTOutputByteArray output;
	PrimitiveWriter pw;
		
	FASTInputByteArray input;
	PrimitiveReader pr;
	
	//NO PMAP
	//NONE, DELTA, and CONSTANT(non-optional)
	
	//Constant can never be optional but can have pmap.

	@AfterClass
	public static void cleanup() {
		System.gc();
	}
	
	@Test
	public void decimalTest() {
		System.gc();
		
		int[] types = new int[] {
                  TypeMask.Decimal,
		    	  TypeMask.DecimalOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None,  //no need for pmap
                OperatorMask.Delta, //no need for pmap
                OperatorMask.Copy,
                OperatorMask.Increment,
                OperatorMask.Constant, 
                OperatorMask.Default
                };
				
		tester(types, operators, "Decimal");
	}
	
	
	

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTWriterDispatch fw = new FASTWriterDispatch(pw, dcr);
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
				
		int i = operationIters;
		int g = fieldsPerGroup;
		fw.openGroup(groupToken, 0);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Constant)) {
					if (sendNulls && ((i&0xF)==0) && TokenBuilder.isOptional(token)) {
						fw.write(token);
					} else {
						fw.write(token, testExpConst, testMantConst); 
					}
				} else {
					if (sendNulls && ((f&0xF)==0) && TokenBuilder.isOptional(token)) {
						fw.write(token);
					} else {
						fw.write(token, 1, testData[f]); 
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
				
		return System.nanoTime() - start;
	}
	

	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup,
			                      DictionaryFactory dcr) {
		
		FASTReaderDispatch fr = new FASTReaderDispatch(pr, dcr);
		
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
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Constant)) {
					readDecimalConstant(tokenLookup, fr, none, f, token, i);
					
				} else {
					readDecimalOthers(tokenLookup, fr, none, f, token);
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

	private void readDecimalOthers(int[] tokenLookup, FASTReaderDispatch fr, long none, int f, int token) {
		if (sendNulls && (f&0xF)==0 && TokenBuilder.isOptional(token)) {
			int exp = fr.readDecimalExponent(tokenLookup[f], -1);
			if (exp<0) {
				assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),-1, exp);
			}
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
	}

	private void readDecimalConstant(int[] tokenLookup, FASTReaderDispatch fr, long none, int f, int token, int i) {
		if (sendNulls && (i&0xF)==0 && TokenBuilder.isOptional(token)) {
			int exp = fr.readDecimalExponent(tokenLookup[f], -1);
			if (exp<0) {
				assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),-1, exp);
			}
			long man = fr.readDecimalMantissa(tokenLookup[f], none);
			if (none!=man) {
				assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),none, man);
			}
		} else { 
			int exp = fr.readDecimalExponent(tokenLookup[f], 0);
			long man = fr.readDecimalMantissa(tokenLookup[f], none);
			if (testMantConst!=man) {
				assertEquals(testMantConst, man);
			}
			if (testExpConst!=exp) {
				assertEquals(testExpConst, exp);
			}
		}
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
		input = new FASTInputByteArray(writtenData, writtenBytes);
		pr = new PrimitiveReader(4096, input, maxGroupCount*10);
	}
	
}
