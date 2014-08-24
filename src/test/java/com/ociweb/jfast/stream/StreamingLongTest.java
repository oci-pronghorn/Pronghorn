//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingLongTest extends BaseStreamingTest {

	final int groupToken = TokenBuilder.buildToken(TypeMask.Group,maxMPapBytes>0?OperatorMask.Group_Bit_PMap:0,maxMPapBytes, TokenBuilder.MASK_ABSENT_DEFAULT);
	final long[] testData     = buildTestDataUnsignedLong(fields);
	final long   testConst    = 0; //must be zero because Dictionary was not init with anything else
	
	FASTOutputByteArray output;
	PrimitiveWriter writer;
		
	FASTInputByteArray input;
	PrimitiveReader reader;

	boolean sendNulls = true;

	int bufferSize = 512;
	
	static FASTRingBuffer rbRingBufferLocal = new FASTRingBuffer((byte)2,(byte)2,null, null);
	
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
				
		tester(types, operators, "UnsignedLong" ,0);
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
		tester(types, operators, "SignedLong" ,0);
	}
	

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		FASTWriterInterpreterDispatch fw = new FASTWriterInterpreterDispatch(new TemplateCatalogConfig(dcr, 3, new int[0][0], null,
        64,4, 100, new ClientConfig(8 ,7) ));
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
				
		int i = operationIters;
		int g = fieldsPerGroup;
		fw.openGroup(groupToken, maxMPapBytes, writer);
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
					
					//special test with constant value.
					if (sendNulls && ((i&0xF)==0) && TokenBuilder.isOptional(token)) {
						writeLong(fw, token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG, writer);
					} else {
						writeLong(fw, token, testConst, writer); 
					}
				} else {
					if (sendNulls && ((f&0xF)==0) && TokenBuilder.isOptional(token)) {
						writeLong(fw, token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG, writer);
					} else {
					    writeLong(fw, token, testData[f], writer); 
					}
				}	
				g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, groupToken, f, maxMPapBytes, writer);				
			}			
		}
		if ( ((fieldsPerGroup*fields)%fieldsPerGroup) == 0  ) {
			fw.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
		}
		fw.flush(writer);
		fw.flush(writer);
				
		return System.nanoTime() - start;
	}

    public static void writeLong(FASTWriterInterpreterDispatch fw, int token, long value, PrimitiveWriter writer) {
        assert (0 != (token & (4 << TokenBuilder.SHIFT_TYPE)));
        //  solution as the ring buffer is introduce into all the APIs
        FASTRingBuffer.dump(rbRingBufferLocal);            
        FASTRingBuffer.addValue(rbRingBufferLocal.buffer,rbRingBufferLocal.mask,rbRingBufferLocal.workingHeadPos,(int) (value >>> 32));
        FASTRingBuffer.addValue(rbRingBufferLocal.buffer,rbRingBufferLocal.mask,rbRingBufferLocal.workingHeadPos,(int) (value & 0xFFFFFFFF)); 
        FASTRingBuffer.unBlockFragment(rbRingBufferLocal.headPos,rbRingBufferLocal.workingHeadPos);
        int rbPos = 0;                    
        
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            // not optional
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {                    
                fw.acceptLongUnsigned(token, rbPos, rbRingBufferLocal, writer);
            } else {
                fw.acceptLongSigned(token, rbPos, rbRingBufferLocal, writer);
            }
        } else {
            if (value == TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG) {
                BaseStreamingTest.write(token, writer, fw);
                //writeLong(fw, token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG, writer);
                
            } else {
                // optional
                if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                    fw.acceptLongUnsignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG, rbPos, rbRingBufferLocal, writer);
                } else {
                    fw.acceptLongSignedOptional(token, TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG, rbPos, rbRingBufferLocal, writer);
                }
            }
        }
    }
	

	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup,
			                      DictionaryFactory dcr) {
	    
	    TemplateCatalogConfig testCatalog = new TemplateCatalogConfig(dcr, 3, new int[0][0], null, 64,maxGroupCount * 10, -1,  new ClientConfig(8 ,7));
		FASTReaderInterpreterDispatch fr = new FASTReaderInterpreterDispatch(testCatalog);
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
			
		final long none = TemplateCatalogConfig.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
		
		int i = operationIters;
		int g = fieldsPerGroup;
		
		fr.openGroup(groupToken, maxMPapBytes, reader);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				
				if (TokenBuilder.isOpperator(token, OperatorMask.Field_Constant)) {
						if (sendNulls && (i&0xF)==0 && TokenBuilder.isOptional(token)) {
				     		long value = TestHelper.readLong(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0), fr);
							if (none!=value) {
								assertEquals(TokenBuilder.tokenToString(tokenLookup[f]), none, value);
							}
						} else { 
							long value = TestHelper.readLong(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0), fr);
							if (testConst!=value) {
								assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),testConst, value);
							}
						}
					
				} else {
				
						if (sendNulls && (f&0xF)==0 && TokenBuilder.isOptional(token)) {
				     		long value = TestHelper.readLong(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0), fr);
							if (none!=value) {
								assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),none, value);
							}
						} else { 
							long value = TestHelper.readLong(tokenLookup[f], reader, RingBuffers.get(fr.ringBuffers,0), fr);
							if (testData[f]!=value) {
								assertEquals(TokenBuilder.tokenToString(tokenLookup[f]),testData[f], value);
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
		return PrimitiveWriter.totalWritten(writer);
	}
	
	protected void resetOutputWriter() {
		output.reset();
		PrimitiveWriter.reset(writer);
	}

	
	protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
		output = new FASTOutputByteArray(writeBuffer);
		writer = new PrimitiveWriter(bufferSize, output, maxGroupCount, false);
	}
	
	protected long totalRead() {
		return PrimitiveReader.totalRead(reader);
	}
	
	protected void resetInputReader() {
		input.reset();
		PrimitiveReader.reset(reader);
	}

	protected void buildInputReader(int maxGroupCount, byte[] writtenData, int writtenBytes) {
		input = new FASTInputByteArray(writtenData, writtenBytes);
		reader = new PrimitiveReader(bufferSize, input, maxGroupCount*10);
	}
}
