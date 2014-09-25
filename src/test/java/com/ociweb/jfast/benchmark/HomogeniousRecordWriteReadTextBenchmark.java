//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.benchmark;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.FieldReferenceOffsetManager;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.stream.BaseStreamingTest;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTRingBuffer;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.stream.RingBuffers;

public class HomogeniousRecordWriteReadTextBenchmark extends Benchmark {

	//Caliper tests///////////////
	//Write and read 1 record, this will time the duration of sending a fixed value end to end.
	//--The records contain 10 fields of the same type and compression operation
	//--This is an estimate of what kind of throughput can be achieved given some test data.
	//--Both the static and dynamic readers/writers will be tested for full comparisons
	//--lowLatency vs bandwidth optimized should both be tested
	//	
	//This is NOT the same as the other tests which measure the duration to produce 1 byte on the stream.
	//--The ns/byte tests are an estimate of how much bandwidth can be saturated given the CPU available.
	
	static final int internalBufferSize = 4096;
	static final int maxGroupCount = 10;
	static final int fields = 10;
	static final int singleCharLength = 256; 
	
	
	//list all types
	static final int[] types = new int[] {
			  TypeMask.TextASCII,
			  TypeMask.TextASCIIOptional,
			  TypeMask.TextUTF8,
			  TypeMask.TextUTF8Optional,
		  };
	
	//list all operators
	static final int[] operators = new int[] {
	          OperatorMask.Field_None, 
			  OperatorMask.Field_Constant,
			  OperatorMask.Field_Copy,
			  OperatorMask.Field_Delta,
			  OperatorMask.Field_Default,
	          OperatorMask.Field_Increment,
	          OperatorMask.Field_Tail
          };

	static final int[] tokenLookup = buildTokens(fields, types, operators);
	static final DictionaryFactory dictionaryFactory = new DictionaryFactory();
	static {
		dictionaryFactory.setTypeCounts(fields,fields,fields);
	}
	
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(internalBufferSize);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter writer = new PrimitiveWriter(internalBufferSize, output, false);
	static final PrimitiveReader reader = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final CharSequence[] textTestData = new CharSequence[]{"","","a","a","ab","ab","abcd","abcd","abcdefgh","abcdefgh"};
	
		
	static final FASTWriterInterpreterDispatch staticWriter = new FASTWriterInterpreterDispatch(new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null,
    64,4, 100, new ClientConfig(8 ,7) ));
	static final TemplateCatalogConfig testCatalog = new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null, 64,maxGroupCount * 10, -1,  new ClientConfig(8 ,7));
	static final FASTReaderInterpreterDispatch staticReader = new FASTReaderInterpreterDispatch(testCatalog);
	
	static final int groupTokenMap = TokenBuilder.buildToken(TypeMask.Group,OperatorMask.Group_Bit_PMap,2, TokenBuilder.MASK_ABSENT_DEFAULT);
	static final int groupTokenNoMap = TokenBuilder.buildToken(TypeMask.Group,0,0, TokenBuilder.MASK_ABSENT_DEFAULT);
		
	public static int[] buildTokens(int count, int[] types, int[] operators) {
		int[] lookup = new int[count];
		int typeIdx = types.length-1;
		int opsIdx = operators.length-1;
		while (--count>=0) {
			//high bit set
			//  7 bit type (must match method)
			//  4 bit operation (must match method)
			// 20 bit instance (MUST be lowest for easy mask and frequent use)

			//find next pattern to be used, rotating over them all.
			do {
				if (--typeIdx<0) {
					if (--opsIdx<0) {
						opsIdx = operators.length-1;
					}
					typeIdx = types.length-1;
				}
			} while (isInValidCombo(types[typeIdx],operators[opsIdx]));
			
			int tokenType = types[typeIdx];
			int tokenOpp = operators[opsIdx];
			lookup[count] = TokenBuilder.buildToken(tokenType, tokenOpp, count, TokenBuilder.MASK_ABSENT_DEFAULT);
					
		}
		return lookup;
		
	}

	public static boolean isInValidCombo(int type, int operator) {
		boolean isOptional = 1==(type&0x01);
		
		if (OperatorMask.Field_Constant==operator & isOptional) {
			//constant operator can never be of type optional
			return true;
		}
		
		if (type>=0 && type<=TypeMask.LongSignedOptional) {
			//integer/long types do not support tail
			if (OperatorMask.Field_Tail==operator) {
				return true;
			}
		}		
		
		return false;
	}

	//
	///
	////
	///
	//
	
	public int timeStaticOverhead(int reps) {
		return staticWriteReadOverheadGroup(reps);
	}

	//
	////
	/////
	////
	//
	
	
	public long timeStaticTextASCIINone(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Field_None, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticTextASCIINoneOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Field_None, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticTextUTF8None(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Field_None, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticTextUTF8NoneOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Field_None, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	////
	
	public long timeStaticTextASCIIConstant(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Field_Constant, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextASCIIConstantOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Field_Constant, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8Constant(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Field_Constant, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8ConstantOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Field_Constant, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	////
	
	public long timeStaticTextASCIICopy(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Field_Copy, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextASCIICopyOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Field_Copy, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8Copy(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Field_Copy, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8CopyOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Field_Copy, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	/////
		
	public long timeStaticTextASCIIDefault(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Field_Default, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextASCIIDefaultOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Field_Default, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8Default(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Field_Default, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8DefaultOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Field_Default, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	////
	
	@Test
	public void testThis() {
		assertTrue(0==timeStaticTextASCIIDelta(20));
	}
	
	
	public long timeStaticTextASCIIDelta(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Field_Delta, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticTextASCIIDeltaOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Field_Delta, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticTextUTF8Delta(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Field_Delta, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticTextUTF8DeltaOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Field_Delta, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	////
	
	
	public long timeStaticTextASCIITail(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Field_Tail, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextASCIITailOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Field_Tail, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8Tail(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Field_Tail, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticTextUTF8TailOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Field_Tail, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	
	//
	////
	//////
	////
	//
	
	protected int staticWriteReadOverheadGroup(int reps) {
		int result = 0;
		int pmapSize = 0;
		int groupToken = groupTokenNoMap;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			writer.reset(writer); //clear any values found in writer
			dictionaryFactory.reset(staticWriter.rIntDictionary);
            dictionaryFactory.reset(staticWriter.rLongDictionary);
            dictionaryFactory.reset(staticWriter.byteHeap); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize, writer);
			int j = textTestData.length;
			while (--j>=0) {
				result |= textTestData[j].length();//do nothing
			}
			staticWriter.closeGroup(groupToken, writer);
			staticWriter.flush(writer);

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			FASTDecoder.reset(dictionaryFactory, staticReader); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken, pmapSize, reader);
			j = textTestData.length;
			while (--j>=0) {
				result |= j;//doing more nothing.
			}
			int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			staticReader.closeGroup(groupToken,idx, reader);
		}
		return result;
	}
	
	static FASTRingBuffer rbRingBufferLocal = new FASTRingBuffer((byte)7,(byte)7,null, FieldReferenceOffsetManager.TEST);
	
	protected long staticWriteReadTextGroup(int reps, int token, int groupToken, int pmapSize) {
		long result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			writer.reset(writer); //clear any values found in writer
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize, writer);
			int j = textTestData.length;
			while (--j>=0) {
			    
                FASTRingBuffer.dump(rbRingBufferLocal);
                byte[] data = BaseStreamingTest.byteMe(textTestData[j]);
                FASTRingBuffer.addByteArray(data, 0, data.length, rbRingBufferLocal);
                FASTRingBuffer.unBlockFragment(rbRingBufferLocal.headPos,rbRingBufferLocal.workingHeadPos);
			    
				assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
                assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));
                
                
                if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                                    // the work.
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // ascii
                        staticWriter.acceptCharSequenceASCII(token, writer, staticWriter.byteHeap, 0, rbRingBufferLocal);
                    } else {                                
                        // utf8
                        staticWriter.acceptByteArray(token, writer, staticWriter.byteHeap, 0, rbRingBufferLocal);
                    }
                } else {
                    if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                        // ascii optional
                        staticWriter.acceptCharSequenceASCIIOptional(token, writer, staticWriter.byteHeap, 0, rbRingBufferLocal);
                    } else {
                        // utf8 optional
                        staticWriter.acceptByteArrayOptional(token, writer, staticWriter.byteHeap, 0, rbRingBufferLocal);
                    }
                }
			}
			staticWriter.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
			staticWriter.flush(writer);

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticReader.reset(); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken, pmapSize, reader);
			j = textTestData.length;
			while (--j>=0) {
				result |= readText(token, reader, staticReader);
			}
			int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			staticReader.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
		}
		return result;
	}
	

    
    public int readText(int token, PrimitiveReader reader, FASTReaderInterpreterDispatch decoder) {
        assert (0 == (token & (4 << TokenBuilder.SHIFT_TYPE)));
        assert (0 != (token & (8 << TokenBuilder.SHIFT_TYPE)));
        FASTRingBuffer rbRingBuffer = RingBuffers.get(decoder.ringBuffers,0);
        if (0 == (token & (1 << TokenBuilder.SHIFT_TYPE))) {// compiler does all
                                                            // the work.
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii
                decoder.readTextASCII(token, reader, RingBuffers.get(decoder.ringBuffers,0));
            } else {
                // utf8
                decoder.readTextUTF8(token, reader, RingBuffers.get(decoder.ringBuffers,0));
            }
        } else {
            if (0 == (token & (2 << TokenBuilder.SHIFT_TYPE))) {
                // ascii optional
                decoder.readTextASCIIOptional(token, reader, RingBuffers.get(decoder.ringBuffers,0));
            } else {
                // utf8 optional
                decoder.readTextUTF8Optional(token, reader, RingBuffers.get(decoder.ringBuffers,0));
            }
        }
        
        //NOTE: for testing we need to check what was written
        int value = FASTRingBuffer.peek(rbRingBuffer.buffer, rbRingBuffer.workingHeadPos.value-2, rbRingBuffer.mask);
        //if the value is positive it no longer points to the byteHeap so we need
        //to make a replacement here for testing.
        return value<0? value : token & decoder.MAX_BYTE_INSTANCE_MASK;
    }
    
    
	
}
