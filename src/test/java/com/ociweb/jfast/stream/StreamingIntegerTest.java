//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.Test;

import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;



public class StreamingIntegerTest extends BaseStreamingTest {
	
	final int groupToken 	  = TokenBuilder.buildToken(TypeMask.Group,maxMPapBytes>0?OperatorMask.Group_Bit_PMap:0,maxMPapBytes);
	final int[] testData     = buildTestDataUnsigned(fields);
	final int   testConst    = 0; //must be zero because Dictionary was not init with anything else
	boolean sendNulls        = true;
		
	FASTOutputByteArray output;
	PrimitiveWriter writer;
	
	FASTInputByteArray input;
	PrimitiveReader reader;

	int MASK = 0xF;
	
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
                OperatorMask.Field_None,  //no need for pmap
                OperatorMask.Field_Delta, //no need for pmap
                OperatorMask.Field_Copy,
                OperatorMask.Field_Increment,
                OperatorMask.Field_Constant, 
                OperatorMask.Field_Default
                };
				
		tester(types, operators, "UnsignedInteger", 0);
	}
	
	@Test
	public void integerSignedTest() {
		int[] types = new int[] {
                  TypeMask.IntegerSigned,
				  TypeMask.IntegerSignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.Field_None,  //no need for pmap
                OperatorMask.Field_Delta, //no need for pmap
                OperatorMask.Field_Copy,
                OperatorMask.Field_Increment,
                OperatorMask.Field_Constant, 
                OperatorMask.Field_Default
                };
		tester(types, operators, "SignedInteger",0);
	}

	@Test
	public void integerFullTest() {
		int[] types = new int[] {
                  TypeMask.IntegerSigned,
				  TypeMask.IntegerSignedOptional,
                  TypeMask.IntegerUnsigned,
		    	  TypeMask.IntegerUnsignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.Field_None,  //no need for pmap
                OperatorMask.Field_Delta, //no need for pmap
                OperatorMask.Field_Copy,
                OperatorMask.Field_Increment,
                OperatorMask.Field_Constant, 
                OperatorMask.Field_Default
                };
		int repeat = 1;//bump up for profiler
		while (--repeat>=0) {
			tester(types, operators, "SignedInteger",0);
		}
	}

	
	
	
	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
				
		TemplateCatalogConfig catalog = new TemplateCatalogConfig(dcr, 3, new int[0][0], null,	64,4, 100, new ClientConfig(8 ,7) );
		
		FASTWriterInterpreterDispatch fw = FASTWriterInterpreterDispatch
				.createFASTWriterInterpreterDispatch(catalog);
		
		
		long start = System.nanoTime();
		assert(operationIters>3) : "must allow operations to have 3 data points but only had "+operationIters;
				
		int i = operationIters;
		int g = fieldsPerGroup;
		fw.openGroup(groupToken, maxMPapBytes, writer);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
							
				if (((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER)==OperatorMask.Field_Constant) {
					
					//special test with constant value.
					if (sendNulls && ((i&MASK)==0) && TokenBuilder.isOptional(token)) {
						writeInteger(fw, token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, writer);
					} else {
						writeInteger(fw, token, testConst, writer); 
					}
				} else {
					if (sendNulls && ((f&MASK)==0) && TokenBuilder.isOptional(token)) {
						writeInteger(fw, token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, writer);
					} else {
					    writeInteger(fw, token, testData[f], writer); 
					}
				}
							
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f, maxMPapBytes, writer,rbRingBufferLocal);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fw.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
		}
		fw.flush(writer);
				
		return System.nanoTime() - start;
	}

	static RingBuffer rbRingBufferLocal = new RingBuffer(new RingBufferConfig((byte)2, (byte)2, null, FieldReferenceOffsetManager.RAW_BYTES));

    public static void writeInteger(FASTWriterInterpreterDispatch fw, int token, int value, PrimitiveWriter writer) {
        //temp solution as the ring buffer is introduce into all the APIs
        RingBuffer.dump(rbRingBufferLocal);
        RingBuffer.addValue(rbRingBufferLocal.buffer,rbRingBufferLocal.mask,rbRingBufferLocal.workingHeadPos,value);
        RingBuffer.publishWrites(rbRingBufferLocal);
        int rbPos = 0;

        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {            
                
                fw.acceptIntegerUnsigned(token, rbPos, rbRingBufferLocal, writer);
            } else {
                fw.acceptIntegerSigned(token, rbPos, rbRingBufferLocal, writer);
            }
        } else {
            // optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                fw.acceptIntegerUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
            } else {
                fw.acceptIntegerSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, rbPos, rbRingBufferLocal, writer);
            }
        }
    }
	
	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup, DictionaryFactory dcr) {
		
	    TemplateCatalogConfig testCatalog = new TemplateCatalogConfig(dcr, 3, new int[0][0], null, 64,maxGroupCount * 10, -1, new ClientConfig(8 ,7));
		testCatalog.clientConfig();
		testCatalog.clientConfig();
		FASTReaderInterpreterDispatch fr = new FASTReaderInterpreterDispatch(testCatalog, RingBuffers.buildNoFanRingBuffers(new RingBuffer(new RingBufferConfig((byte)15, (byte)7, testCatalog.ringByteConstants(), testCatalog.getFROM()))));
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
			
		int i = operationIters;
		int g = fieldsPerGroup;
		
		fr.openGroup(groupToken, maxMPapBytes, reader);
		
		RingBuffer ringBuffer = RingBuffers.get(fr.ringBuffers,0);
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
														
				if (((token>>TokenBuilder.SHIFT_OPER)&TokenBuilder.MASK_OPER)==OperatorMask.Field_Constant) {
					if (sendNulls && (i&MASK)==0 && TokenBuilder.isOptional(token)) {
						int value = TestHelper.readInt(tokenLookup[f], reader, ringBuffer, fr);
						if (TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT!=value) {
							assertEquals(TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, value);
						}
					} else { 
						int value = TestHelper.readInt(tokenLookup[f], reader, ringBuffer, fr);
						if (testConst!=value) {
							System.err.println(TokenBuilder.tokenToString(tokenLookup[f]));
							assertEquals(testConst, value);
						}
					}
				
				} else {	
				
					if (sendNulls && (f&MASK)==0 && TokenBuilder.isOptional(token)) {
			     		int value = TestHelper.readInt(tokenLookup[f], reader, ringBuffer, fr);
						if (TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT!=value) {
							assertEquals(TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT, value);
						}
					} else { 
						int value = TestHelper.readInt(tokenLookup[f], reader, ringBuffer, fr);
						if (testData[f]!=value) {
							System.err.println(TokenBuilder.tokenToString(tokenLookup[f]));
							assertEquals(testData[f], value);
						}
					}
				}
				
				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f, maxMPapBytes, reader);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
		    int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			fr.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
		}
			
		long duration = System.nanoTime() - start;
		return duration;
	}

	public long totalWritten() {
		return writer.totalWritten(writer);
	}
	
	protected void resetOutputWriter() {
		output.reset();
		writer.reset(writer);
	}

	protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
		output = new FASTOutputByteArray(writeBuffer);
		writer = new PrimitiveWriter(4096, output, false);
	}


	protected long totalRead() {
		return PrimitiveReader.totalRead(reader);
	}
	
	protected void resetInputReader() {
		input.reset();
		PrimitiveReader.reset(reader);
	}

	protected void buildInputReader(int maxGroupCount, byte[] writtenData, int writtenBytes) {
		input = new FASTInputByteArray(writtenData,writtenBytes);
		reader = new PrimitiveReader(4096, input, maxGroupCount*10);
	}

	
}
