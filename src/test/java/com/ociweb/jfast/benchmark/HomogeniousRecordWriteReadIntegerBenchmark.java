//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.benchmark;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.catalog.loader.ClientConfig;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.catalog.loader.TemplateCatalogConfig;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.stream.FASTDecoder;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;
import com.ociweb.jfast.stream.StreamingIntegerTest;
import com.ociweb.jfast.stream.TestHelper;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class HomogeniousRecordWriteReadIntegerBenchmark extends Benchmark {

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
	static final int singleCharLength = 128;
	
	//list all types
	static final int[] types = new int[] {
			  TypeMask.IntegerUnsigned,
			  TypeMask.IntegerUnsignedOptional,
			  TypeMask.IntegerSigned,
			  TypeMask.IntegerSignedOptional,
			  TypeMask.LongUnsigned,
			  TypeMask.LongUnsignedOptional,
			  TypeMask.LongSigned,
			  TypeMask.LongSignedOptional,
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
		dictionaryFactory.setTypeCounts(fields,fields,fields, 16, 128);
	}
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(4096);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter writer = new PrimitiveWriter(internalBufferSize, output, false);
	static final PrimitiveReader reader = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final int[] intTestData = new int[] {0,0,1,1,2,2,2000,2002,10000,10001};
	static final long[] longTestData = new long[] {0,0,1,1,2,2,2000,2002,10000,10001};
	

		
	static final FASTWriterInterpreterDispatch staticWriter = FASTWriterInterpreterDispatch
			.createFASTWriterInterpreterDispatch(new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null,
			64,4, 100, new ClientConfig(8 ,7) ));
	
	static final TemplateCatalogConfig testCatalog = new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null, 64,maxGroupCount * 10, -1, new ClientConfig(8 ,7));
	private static final RingBuffer RB = new RingBuffer(new RingBufferConfig((byte)15, (byte)7, testCatalog.ringByteConstants(), testCatalog.getFROM()));
	static final FASTReaderInterpreterDispatch staticReader = new FASTReaderInterpreterDispatch(testCatalog, RingBuffers.buildNoFanRingBuffers(RB));
	
	static final int groupTokenMap = TokenBuilder.buildToken(TypeMask.Group,OperatorMask.Group_Bit_PMap,2);
	static final int groupTokenNoMap = TokenBuilder.buildToken(TypeMask.Group,0,0);
	
	
//	@Param(value = "0")
//	int ttoken = 0;
//	
//	public Iterable<Integer> ttokenValues() {
//		
//		List<Integer> vals = new ArrayList<Integer>();
//		for(int x: tokenLookup) {
//			vals.add(x);
//		}
//		return vals;
//	}
//	
//	
//	public int timeStaticIntegerUnsignedALLWR(int reps) {
//		return staticWriteReadIntegerGroup(reps, ttoken, zeroGroupToken);
//	}
//	
//	public int timeStaticIntegerUnsignedALLW(int reps) {
//		return staticWriteIntegerGroup(reps, ttoken, zeroGroupToken);
//	}
	
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
			lookup[count] = TokenBuilder.buildToken(tokenType, tokenOpp, count);
					
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
		return staticWriteReadOverheadGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerUnsigned,
						OperatorMask.Field_None, 
						0), groupTokenNoMap, 0);
	}
	
		
	@Test
	public void testThis() {
		int rep = 20;
		timeStaticIntegerUnsignedNoneW(rep);
		
		//total byte for one pass times the reps
		long totalWritten = rep*this.writer.totalWritten(writer);
		
		float bytesPerValue = totalWritten/(float)(rep*intTestData.length);
		
		System.out.println("IntegerUnsignedNone  avg bytes per value "+bytesPerValue);
		
	}
	
	@Test
	public void testThis2() {
		int rep = 20;
		timeStaticIntegerSignedCopyOptionalW(rep);
		
		//total byte for one pass times the reps
		long totalWritten = rep*this.writer.totalWritten(writer);
		
		float bytesPerValue = totalWritten/(float)(rep*intTestData.length);
		
		System.err.println("IntegerSignedCopyOptional  avg bytes per value "+bytesPerValue);
		
	}
	
	//
	////
	//////
	////
	//  

	public int timeStaticIntegerUnsignedNoneWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}
	
	public int timeStaticIntegerUnsignedNoneOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}

	
	public int timeStaticIntegerUnsignedCopyWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedCopyOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedConstantWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerUnsigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedDefaultWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedDefaultOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedDeltaWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerUnsigned, 
						OperatorMask.Field_Delta, 
						0), groupTokenNoMap, 0);
	}

	public int timeStaticIntegerUnsignedDeltaOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Field_Delta, 
						     0), groupTokenNoMap, 0);
	}
	
	
	public int timeStaticIntegerUnsignedIncrementWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedIncrementOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	//Integer does not support Tail operator

	public int timeStaticIntegerSignedNoneWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}
	
	public int timeStaticIntegerSignedNoneOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}

	
	public int timeStaticIntegerSignedCopyWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedCopyOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedConstantWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerSigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedDefaultWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedDefaultOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedDeltaWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerSigned, 
						OperatorMask.Field_Delta, 
						0), groupTokenNoMap, 0);
	}

	public int timeStaticIntegerSignedDeltaOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Field_Delta, 
						     0), groupTokenNoMap, 0);
	}
	
	
	public int timeStaticIntegerSignedIncrementWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedIncrementOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	
	public int timeStaticIntegerUnsignedNoneW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}
	
	public int timeStaticIntegerUnsignedNoneOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}

	
	public int timeStaticIntegerUnsignedCopyW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedCopyOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedConstantW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerUnsigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedDefaultW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedDefaultOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedDeltaW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerUnsigned, 
						OperatorMask.Field_Delta, 
						0), groupTokenNoMap, 0);
	}

	public int timeStaticIntegerUnsignedDeltaOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Field_Delta, 
						     0), groupTokenNoMap, 0);
	}
	
	
	public int timeStaticIntegerUnsignedIncrementW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerUnsignedIncrementOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	//Integer does not support Tail operator

	public int timeStaticIntegerSignedNoneW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}
	
	public int timeStaticIntegerSignedNoneOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}

	
	public int timeStaticIntegerSignedCopyW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedCopyOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedConstantW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerSigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedDefaultW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedDefaultOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedDeltaW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerSigned, 
						OperatorMask.Field_Delta, 
						0), groupTokenNoMap, 0);
	}

	public int timeStaticIntegerSignedDeltaOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Field_Delta, 
						     0), groupTokenNoMap, 0);
	}
	
	
	public int timeStaticIntegerSignedIncrementW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	public int timeStaticIntegerSignedIncrementOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	//
	////
	//////
	////
	//
	
	protected int staticWriteReadOverheadGroup(int reps, int token, int groupToken, int pmapSize) {
		int result = 0;
		RingBuffer ringBuffer = RingBuffers.get(staticReader.ringBuffers,staticReader.activeScriptCursor);
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			PrimitiveWriter.reset(writer); //clear any values found in writer
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize, writer);
			int j = intTestData.length;
			while (--j>=0) {
				result |= intTestData[j];//do nothing
			}
			staticWriter.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
			staticWriter.flush(writer);

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			FASTDecoder.reset(dictionaryFactory, staticReader); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken, pmapSize, reader);
			j = intTestData.length;
			while (--j>=0) {
				result |= j;//pr.readIntegerUnsigned();////j;//doing more nothing.
			}
			int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			staticReader.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
		}
		return result;
	}
	
	
	protected int staticWriteReadIntegerGroup(int reps, int token, int groupToken, int pmapSize) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			PrimitiveWriter.reset(writer); //clear any values found in writer
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize, writer);
			int j = intTestData.length;
			while (--j>=0) {
			    StreamingIntegerTest.writeInteger(staticWriter, token, intTestData[j],writer);
			}
			RingBuffer ringBuffer = RingBuffers.get(staticReader.ringBuffers,staticReader.activeScriptCursor);
			staticWriter.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
			staticWriter.flush(writer);
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticReader.reset(); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken, pmapSize, reader);
			j = intTestData.length;
			while (--j>=0) {
				result |= TestHelper.readInt(token, reader, ringBuffer, staticReader);
			}
			int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			staticReader.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
		}
		return result;
	}
	
	protected int staticWriteIntegerGroup(int reps, int token, int groupToken, int pmapSize) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			PrimitiveWriter.reset(writer); //clear any values found in writer
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize, writer);
			int j = intTestData.length;
			while (--j>=0) {
			    StreamingIntegerTest.writeInteger(staticWriter, token, intTestData[j], writer);
			}
			
			staticWriter.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
			staticWriter.flush(writer);
			
		}
		return result;
	}
	
}
