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
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingDecimalTest extends BaseStreamingTest {

	final long[] testData     = buildTestDataUnsignedLong(fields);
	final int   testExpConst = 0;
	final long   testMantConst = 0;
	
	//Must double because we may need 1 bit for exponent and another for mantissa
	final int pmapSize = maxMPapBytes*2;
	final int groupToken = TokenBuilder.buildToken(TypeMask.Group,maxMPapBytes>0?OperatorMask.Group_Bit_PMap:0,pmapSize);

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
                OperatorMask.Field_None,  //no need for pmap
                OperatorMask.Field_Delta, //no need for pmap
                OperatorMask.Field_Copy,
                OperatorMask.Field_Increment,
                OperatorMask.Field_Constant, 
                OperatorMask.Field_Default
                };
				
		int i = 1;//set to large value for profiling
		while (--i>=0) {
			
			tester(types, operators, "Decimal", 0 ,0);
		}
		
	}
	
	
	FASTWriterDispatch fw = null;

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		//TODO: determine how to cache this to speed up the testing.
		//if (null==fw) {
			fw = new FASTWriterDispatch(pw, dcr, 100);
		//} else {
		//	fw.flush();
		//	resetOutputWriter();
		//	fw.reset(dcr);
		//}
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
				
		int i = operationIters;
		int g = fieldsPerGroup;
		fw.openGroup(groupToken,pmapSize);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
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
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f, pmapSize);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fw.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER));
		}
		fw.flush();
		fw.flush();
				
		return System.nanoTime() - start;
	}
	
	FASTReaderDispatch fr;
	
	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup,
			                      DictionaryFactory dcr) {
		
		//if (null==fr) {
			fr = new FASTReaderDispatch(pr, dcr, 100, 3, fields);
		//} else {
		//	//pr.reset();
		//	fr.reset();
		//}
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
			
		long none = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
		
		int i = operationIters;
		int g = fieldsPerGroup;
		
		fr.openGroup(groupToken, pmapSize);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
					readDecimalConstant(tokenLookup, fr, none, f, token, i);
					
				} else {
					readDecimalOthers(tokenLookup, fr, none, f, token);
				}
				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f, pmapSize);				
			}			
		}
		
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fr.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER));
		}
			
		long duration = System.nanoTime() - start;
		return duration;
	}

	private void readDecimalOthers(int[] tokenLookup, FASTReaderDispatch fr, long none, int f, int token) {
		if (sendNulls && (f&0xF)==0 && TokenBuilder.isOptional(token)) {
			int exp = fr.readDecimalExponent(tokenLookup[f]);
			if (exp!=TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT) {
				assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, exp);
			}
			long man = fr.readDecimalMantissa(tokenLookup[f]);
			if (none!=man) {
				assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),none, man);
			}
		} else { 
			int exp = fr.readDecimalExponent(tokenLookup[f]);
			long man = fr.readDecimalMantissa(tokenLookup[f]);
			if (testData[f]!=man) {
				assertEquals(testData[f], man);
			}
		}
	}

	private void readDecimalConstant(int[] tokenLookup, FASTReaderDispatch fr, long none, int f, int token, int i) {
		if (sendNulls && (i&0xF)==0 && TokenBuilder.isOptional(token)) {
			int exp = fr.readDecimalExponent(tokenLookup[f]);
			if (exp!=TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT) {
				assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, exp);
			}
			long man = fr.readDecimalMantissa(tokenLookup[f]);
			if (none!=man) {
				assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),none, man);
			}
		} else { 
			int exp = fr.readDecimalExponent(tokenLookup[f]);
			long man = fr.readDecimalMantissa(tokenLookup[f]);
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
