package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TextHeap;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class TextStreamingTest extends BaseStreamingTest {

	final int fields      		      = 30000;
	final CharSequence[] testData    = buildTestData(fields);
	final char[][] testDataChars = buildTestDataChars(testData);
	
	FASTOutputByteArray output;
	PrimitiveWriter pw;
	
	FASTInputByteArray input;
	PrimitiveReader pr;
	
	@AfterClass
	public static void cleanup() {
		System.gc();
	}
	
	private char[][] buildTestDataChars(CharSequence[] source) {
		int i = source.length;
		char[][] result = new char[i][];
		while (--i>=0) {
			result[i]= seqToArray(testData[i]);
		}
		return result;
	}

	@Test
	public void asciiTest() {
		int[] types = new int[] {
                   TypeMask.TextASCII,
                   TypeMask.TextASCIIOptional,
				 };
		int[] operators = new int[] {
                  OperatorMask.None,   //W5 R16 w/o equals
				  OperatorMask.Constant, //W6 R16 w/o equals
				  OperatorMask.Copy,     //W84 R31 w/o equals ****fix test data was not letting any copy happen so it was send every time.
				  OperatorMask.Default,  //W6 R16 
//				  OperatorMask.Delta,
                 OperatorMask.Tail,     //W46 R15 w/o equals
                };

		textTester(types,operators,"ASCII");
	}
	
	@Test
	public void utf8Test() {
		int[] types = new int[] {
				  TypeMask.TextUTF8,
				  TypeMask.TextUTF8Optional,
				 };
		int[] operators = new int[] {
                OperatorMask.None, 
				OperatorMask.Constant,
			    OperatorMask.Copy,
				OperatorMask.Default,
			//	OperatorMask.Delta,
                OperatorMask.Tail,
                };

		textTester(types,operators,"UTF8");
	}
	
	//TODO: note; what about undo operations going back feed?
	//TODO: note: use protol for archive format when monitoring.
	
	private void textTester(int[] types, int[] operators, String label) {
		
		int singleCharLength = 1024;
		int fieldsPerGroup = 10;
		int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
		int operationIters = 7;
		int warmup         = 50;
		int sampleSize     = 100;
		int avgFieldSize   = ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK*2+1;
		String readLabel = "Read "+label+"Text NoOpp in groups of "+fieldsPerGroup;
		String writeLabel = "Write "+label+"Text NoOpp in groups of "+fieldsPerGroup;
		
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
		
	}

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTStaticWriter fw = new FASTStaticWriter(pw, dcr, tokenLookup);
		
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
					char[] array = testDataChars[f];
					fw.write(token, array, 0 , array.length); 
				}
							
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, f);				
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
		FASTStaticReader fr = new FASTStaticReader(pr, dcr, tokenLookup);
		TextHeap textHeap = fr.textHeap();
		
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
				if (false && (f&0xF)==0 && (0!=(token&0x1000000))) {
					//TODO: why is this int must be CHAR?
//		     		int value = fr.readInt(tokenLookup[f], Integer.MIN_VALUE);
//					if (Integer.MIN_VALUE!=value) {
//						assertEquals(Integer.MIN_VALUE, value);
//					}
				} else { 
					try {
						int textIdx = fr.readChars(tokenLookup[f]);						
						
						char[] tdc = testDataChars[f];
						if (!textHeap.equals(textIdx, tdc, 0, tdc.length)) {
							
							assertEquals("Error:"+TokenBuilder.tokenToString(tokenLookup[f]),
									testData[f],
									     textHeap.get(textIdx,new StringBuilder()).toString());
						}
					
						
					} catch (Exception e) {
						System.err.println("expected text; "+testData[f]);
						throw new FASTException(e);
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

	private char[] seqToArray(CharSequence seq) {
		char[] result = new char[seq.length()];
		int i = seq.length();
		while (--i>=0) {
			result[i] = seq.charAt(i);
		}
		return result;
	}

	private CharSequence[] buildTestData(int count) {
		
		CharSequence[] seedData = ReaderWriterPrimitiveTest.stringData;
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
