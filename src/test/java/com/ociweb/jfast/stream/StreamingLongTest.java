//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingLongTest extends BaseStreamingTest {

	final int groupToken = TokenBuilder.buildToken(TypeMask.Group,maxMPapBytes>0?OperatorMask.Group_Bit_PMap:0,maxMPapBytes);
	final long[] testData     = buildTestDataUnsignedLong(fields);
	final long   testConst    = 0; //must be zero because Dictionary was not init with anything else
	
	FASTOutputByteArray output;
	PrimitiveWriter pw;
		
	FASTInputByteArray input;
	PrimitiveReader pr;

	boolean sendNulls = true;
	
	//NO PMAP
	//NONE, DELTA, and CONSTANT(non-optional)
	
	//Constant can never be optional but can have pmap.
		
	@Test
	public void longUnsignedTest() {
		int[] types = new int[] {
                  TypeMask.LongUnsigned,
		    	  TypeMask.LongUnsignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.Field_None,  //no need for pmap
                OperatorMask.Field_Delta, //no need for pmap
                OperatorMask.Field_Copy,
                OperatorMask.Field_Increment,
                OperatorMask.Field_Constant, 
                OperatorMask.Field_Default
                };
				
		tester(types, operators, "UnsignedLong",0 ,0);
	}
	
	@Test
	public void longSignedTest() {
		int[] types = new int[] {
                  TypeMask.LongSigned,
				  TypeMask.LongSignedOptional,
				};
		
		int[] operators = new int[] {
                OperatorMask.Field_None,  //no need for pmap
                OperatorMask.Field_Delta, //no need for pmap
                OperatorMask.Field_Copy,
                OperatorMask.Field_Increment,
                OperatorMask.Field_Constant, 
                OperatorMask.Field_Default
                };
		tester(types, operators, "SignedLong",0 ,0);
	}
	

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTWriterDispatch fw = new FASTWriterDispatch(pw, dcr, 100);
		
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
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
					
					//special test with constant value.
					if (sendNulls && ((i&0xF)==0) && TokenBuilder.isOptional(token)) {
						fw.write((i&ID_TOKEN_TOGGLE)==0?token:f);//nothing
					} else {
						fw.write((i&ID_TOKEN_TOGGLE)==0?token:f, testConst); 
					}
				} else {
					if (sendNulls && ((f&0xF)==0) && TokenBuilder.isOptional(token)) {
						fw.write((i&ID_TOKEN_TOGGLE)==0?token:f);
					} else {
						fw.write((i&ID_TOKEN_TOGGLE)==0?token:f, testData[f]); 
					}
				}	
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fw.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER));
		}
		fw.flush();
		fw.flush();
				
		return System.nanoTime() - start;
	}
	

	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup,
			                      DictionaryFactory dcr) {
		FASTReaderDispatch fr = new FASTReaderDispatch(pr, dcr, 100);
		
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
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
						if (sendNulls && (i&0xF)==0 && TokenBuilder.isOptional(token)) {
				     		long value = fr.readLong((i&ID_TOKEN_TOGGLE)==0?tokenLookup[f]:f, none);
							if (none!=value) {
								assertEquals(TokenBuilder.tokenToString(tokenLookup[f]), none, value);
							}
						} else { 
							long value = fr.readLong((i&ID_TOKEN_TOGGLE)==0?tokenLookup[f]:f, none);
							if (testConst!=value) {
								assertEquals(testConst, value);
							}
						}
					
				} else {
				
						if (sendNulls && (f&0xF)==0 && TokenBuilder.isOptional(token)) {
				     		long value = fr.readLong((i&ID_TOKEN_TOGGLE)==0?tokenLookup[f]:f, none);
							if (none!=value) {
								assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),none, value);
							}
						} else { 
							long value = fr.readLong((i&ID_TOKEN_TOGGLE)==0?tokenLookup[f]:f, none);
							if (testData[f]!=value) {
								assertEquals(testData[f], value);
							}
						}
					
				}
				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f);				
			}	
			
		//	System.err.println("TST:"+Long.toBinaryString(pr.getFingerprint()));
			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fr.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER));
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
		input = new FASTInputByteArray(writtenData, writtenBytes);
		pr = new PrimitiveReader(4096, input, maxGroupCount*10);
	}
}
