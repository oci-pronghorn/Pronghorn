//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.benchmark.HomogeniousRecordWriteReadLongBenchmark;
import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.ByteHeap;
import com.ociweb.jfast.field.FieldReaderBytes;
import com.ociweb.jfast.field.FieldWriterBytes;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.FASTInput;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.ReaderWriterPrimitiveTest;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingBytesTest extends BaseStreamingTest {

	final int fields      		      = 2000;
	final ByteBuffer[] testData    = buildTestData(fields);
	
	final byte[] testConst  = new byte[0];
	final ByteBuffer testContByteBuffer = ByteBuffer.wrap(testConst);
	
	boolean sendNulls        = true;
	
	final byte[][] testDataBytes = buildTestDataBytes(testData);
	
	FASTOutputByteArray output;
	PrimitiveWriter pw;
	
	FASTInputByteArray input;
	PrimitiveReader pr;
	
	@AfterClass
	public static void cleanup() {
		System.gc();
	}
	
	private byte[][] buildTestDataBytes(ByteBuffer[] source) {
		int i = source.length;
		byte[][] result = new byte[i][];
		while (--i>=0) {
			result[i]=  testData[i].array(); 
		}
		return result;
	}
	
	@Test
	public void bytesTest() {
		int[] types = new int[] {
                   TypeMask.ByteArray,
                   TypeMask.ByteArrayOptional,
		};
		int[] operators = new int[] {
                  OperatorMask.Field_None,   
				  OperatorMask.Field_Constant, 
				  OperatorMask.Field_Copy,    
				  OperatorMask.Field_Default,  
				  OperatorMask.Field_Delta,    
                  OperatorMask.Field_Tail,     
        };

		byteTester(types,operators,"Bytes");
	}
	
	@Test
	public void TailTest() {
		
		byte[] buffer = new byte[2048];
		FASTOutput output = new FASTOutputByteArray(buffer);
		PrimitiveWriter writer = new PrimitiveWriter(output);
		
		int singleSize=14;
		int singleGapSize=8;
		int fixedTextItemCount=16; //must be power of two
		
		ByteHeap dictionaryWriter = new ByteHeap(singleSize, singleGapSize, fixedTextItemCount);
		FieldWriterBytes byteWriter = new FieldWriterBytes(writer, dictionaryWriter);
		
		int token = TokenBuilder.buildToken(TypeMask.ByteArray, 
				                            OperatorMask.Field_Tail, 
				                            0);
		byte[] value = new byte[]{1,2,3};
		int offset = 0;
		int length = value.length;
		byteWriter.writeBytesTail(token, value, offset, length);
		byteWriter.writeBytesDelta(token, value, offset, length);
		byteWriter.writeBytesConstant(token);

		writer.openPMap(1);
			byteWriter.writeBytesCopy(token, value, offset, length);
			byteWriter.writeBytesDefault(token, value, offset, length);
		writer.closePMap();
		writer.flush();
		
		FASTInput input = new FASTInputByteArray(buffer);
		PrimitiveReader reader = new PrimitiveReader(input);
		
		ByteHeap dictionaryReader = new ByteHeap(singleSize, singleGapSize, fixedTextItemCount);
		FieldReaderBytes byteReader = new FieldReaderBytes(reader, dictionaryReader);
		
		
		//read value back
		int id;
		id = byteReader.readBytesTail(token);
		assertTrue(dictionaryReader.equals(id, value, offset, length));

		id = byteReader.readBytesDelta(token);
		assertTrue(dictionaryReader.equals(id, value, offset, length));

		id = byteReader.readBytesConstant(token);
		assertTrue(dictionaryReader.equals(id, value, offset, length));
		
		reader.openPMap(1);
		
			id = byteReader.readBytesCopy(token);
			assertTrue(dictionaryReader.equals(id, value, offset, length));
			
			id = byteReader.readBytesDefault(token);
			assertTrue(dictionaryReader.equals(id, value, offset, length));
			
		reader.closePMap();
		
		
		
	}
	
	private void byteTester(int[] types, int[] operators, String label) {
		
		int singleBytesLength = 2048;
		int fieldsPerGroup = 10;
		int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
		int operationIters = 7;
		int warmup         = 20;
		int sampleSize     = 40;
		int avgFieldSize   = ReaderWriterPrimitiveTest.VERY_LONG_STRING_MASK*2+1;
		String readLabel = "Read "+label+" NoOpp in groups of "+fieldsPerGroup;
		String writeLabel = "Write "+label+" NoOpp in groups of "+fieldsPerGroup;
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*avgFieldSize));
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
				
		int[] tokenLookup = HomogeniousRecordWriteReadLongBenchmark.buildTokens(fields, types, operators);
		byte[] writeBuffer = new byte[streamByteSize];
		

		///////////////////////////////
		//test the writing performance.
		//////////////////////////////
		
		long byteCount = performanceWriteTest(fields, fields, fields, singleBytesLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize,
				writeLabel, streamByteSize, maxGroupCount, tokenLookup, writeBuffer);

		///////////////////////////////
		//test the reading performance.
		//////////////////////////////
		
		performanceReadTest(fields, fields, fields, singleBytesLength, fieldsPerGroup, maxMPapBytes, operationIters, warmup, sampleSize, readLabel,
				streamByteSize, maxGroupCount, tokenLookup, byteCount, writeBuffer);
		
		int i = 0;
		for(ByteBuffer d: testData) {
			i+=d.remaining();
		}
		
		long dataCount = (operationIters * (long)fields * (long)i)/testData.length; 
		
		System.out.println("FullData:"+dataCount+" XmitData:"+byteCount+" compression:"+(byteCount/(float)dataCount));
		
	}

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTWriterDispatch fw = new FASTWriterDispatch(pw, dcr, 100);
		
		long start = System.nanoTime();
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		
		int groupToken = TokenBuilder.buildToken(TypeMask.Group,maxMPapBytes>0?OperatorMask.Group_Bit_PMap:0,maxMPapBytes);
		
		fw.openGroup(groupToken, maxMPapBytes);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
					if (sendNulls && ((i&0xF)==0)  && TokenBuilder.isOptional(token)) {
						fw.write(token);
					} else {
						if ((i&1)==0) {
							testContByteBuffer.mark();
							fw.write(token,testContByteBuffer); //write byte buffer
							testContByteBuffer.reset();
							
						} else {
							byte[] array = testConst;
							fw.write(token, array, 0, array.length); 
						}
					}
				} else {
					if (sendNulls && ((f&0xF)==0)  && TokenBuilder.isOptional(token)) {
						fw.write(token);
					} else {
						if ((i&1)==0) {
							//first failing test
							testData[f].mark();
							fw.write(token,testData[f]); //write byte buffer
							testData[f].reset();
						} else {
							byte[] array = testDataBytes[f];
							fw.write(token, array, 0, array.length); 
						}
					}
				}
							
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f, maxMPapBytes);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fw.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER));
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
		FASTReaderDispatch fr = new FASTReaderDispatch(pr, dcr, 100, 3, new int[0][0], 0,128);
		ByteHeap byteHeap = fr.byteHeap();
		
		int token = 0;
		int prevToken = 0;
		
		long start = System.nanoTime();
		int i = operationIters;
		if (i<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+i);
		}
		int g = fieldsPerGroup;
		int groupToken = TokenBuilder.buildToken(TypeMask.Group,maxMPapBytes>0?OperatorMask.Group_Bit_PMap:0,maxMPapBytes);
		
		fr.openGroup(groupToken, maxMPapBytes);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				prevToken = token;
				token = tokenLookup[f]; 	
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
					if (sendNulls && (i&0xF)==0  && TokenBuilder.isOptional(token)) {
						
						int idx = fr.readBytes(tokenLookup[f]);		
						if (!byteHeap.isNull(idx)) {
							assertEquals("Error:"+TokenBuilder.tokenToString(token),
									     true, byteHeap.isNull(idx));
						}	
						
					} else { 
						try {
							int textIdx = fr.readBytes(tokenLookup[f]);						
							
							byte[] tdc = testConst;
							assertTrue("Error:"+TokenBuilder.tokenToString(token),
									byteHeap.equals(textIdx, tdc, 0, tdc.length));
							
						} catch (Exception e) {
							System.err.println("expected text; "+testData[f]);
							e.printStackTrace();
							throw new FASTException(e);
						}
					}
				} else {
					if (sendNulls && (f&0xF)==0  && TokenBuilder.isOptional(token)) {
						
						int idx = fr.readBytes(tokenLookup[f]);		
						if (!byteHeap.isNull(idx)) {
							assertEquals("Error:"+TokenBuilder.tokenToString(token)+ 
									    "Expected null found len "+byteHeap.length(idx),
									     true, byteHeap.isNull(idx));
						}	
						
					} else { 
						try {
							int textIdx = fr.readBytes(tokenLookup[f]);						
														
							if ((1&i) == 0) {
								assertTrue("Error: Token:"+TokenBuilder.tokenToString(token)+
										    " PrevToken:"+TokenBuilder.tokenToString(prevToken),
										  byteHeap.equals(textIdx, testData[f])
										);  
							} else {
								byte[] tdc = testDataBytes[f];
								assertTrue("Error:"+TokenBuilder.tokenToString(token),
										  byteHeap.equals(textIdx, tdc, 0, tdc.length)
										);
								
							}
													
							
						} catch (Exception e) {
							System.err.println("expected text; "+testData[f]);
							System.err.println("PrevToken:"+TokenBuilder.tokenToString(prevToken));
							System.err.println("token:"+TokenBuilder.tokenToString(token));
							e.printStackTrace();
							throw new FASTException(e);
						}
					}
				}
			
				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f, maxMPapBytes);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0 ) {
			fr.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER));
		}
		long duration = System.nanoTime() - start;
		return duration;
	}


	private ByteBuffer[] buildTestData(int count) {
		
		byte[][] seedData = ReaderWriterPrimitiveTest.byteData;
		int s = seedData.length;
		int i = count;
		ByteBuffer[] target = new ByteBuffer[count];
		while (--i>=0) {
			target[i] = ByteBuffer.wrap(seedData[--s]);
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
		pr = new PrimitiveReader(writtenData.length, input, maxGroupCount);
	}
	
}
