//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.benchmark;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import javax.swing.text.MaskFormatter;

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
import com.ociweb.jfast.stream.StreamingLongTest;
import com.ociweb.jfast.stream.TestHelper;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.ring.RingBuffers;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

public class HomogeniousRecordWriteReadLongBenchmark extends Benchmark {

	//Caliper tests///////////////
	//Write and read 1 record, this will time the duration of sending a fixed value end to end.
	//--The records contain 10 fields of the same type and compression operation
	//--This is an estimate of what kind of throughput can be achieved given some test data.
	//--Both the static and dynamic readers/writers will be tested for full comparisons
	//--lowLatency vs bandwidth optimized should both be tested
	//	
	//This is NOT the same as the other tests which measure the duration to produce 1 byte on the stream.
	//--The ns/byte tests are an estimate of how much bandwidth can be saturated given the CPU available.
	
	static final int internalBufferSize = 2048;
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

	static final int[] tokenLookup = TestUtil.buildTokens(fields, types, operators);
	
	static final DictionaryFactory dictionaryFactory = new DictionaryFactory();
	static {
		dictionaryFactory.setTypeCounts(fields,fields,fields, 16, 128);
	}
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(internalBufferSize);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter writer = new PrimitiveWriter(internalBufferSize, output, false);
	static final PrimitiveReader reader = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final long[] longTestData = new long[] {0,0,1,1,2,2,2000,2002,10000,10001};
	

		
	static final FASTWriterInterpreterDispatch staticWriter = FASTWriterInterpreterDispatch
			.createFASTWriterInterpreterDispatch(new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null, 64,4, 100, new ClientConfig(8 ,7) ));
	
	static final TemplateCatalogConfig testCatalog = new TemplateCatalogConfig(dictionaryFactory, 3, new int[0][0], null, 64,maxGroupCount * 10, -1,  new ClientConfig(8 ,7));
	private static final RingBuffer RB = new RingBuffer(new RingBufferConfig((byte)15, (byte)7, testCatalog.ringByteConstants(), testCatalog.getFROM()));
	static final FASTReaderInterpreterDispatch staticReader = new FASTReaderInterpreterDispatch(testCatalog, RingBuffers.buildNoFanRingBuffers(RB));
	
	static final int groupTokenNoMap = TokenBuilder.buildToken(TypeMask.Group, 0, 0);
	static final int groupTokenMap = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_PMap, 0);
	
	

	//
	///
	////
	///
	//
	
	public int timeStaticOverhead(int reps) {
		return staticWriteReadOverheadGroup(reps);
	}
	
	@Test
	public void testThis() {
		assertTrue(0!=timeStaticLongUnsignedNone(20));
	}
	
	//
	////
	/////
	////
	//
	
	
	public long timeStaticLongUnsignedNone(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsigned,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}
	
	public long timeStaticLongUnsignedNoneOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}

	
	public long timeStaticLongUnsignedCopy(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsigned,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedCopyOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedConstant(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.LongUnsigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedDefault(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsigned, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedDefaultOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedDelta(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.LongUnsigned, 
						OperatorMask.Field_Delta, 
						0), groupTokenNoMap, 0);
	}

	public long timeStaticLongUnsignedDeltaOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional, 
						    OperatorMask.Field_Delta, 
						     0), groupTokenNoMap, 0);
	}
	
	
	public long timeStaticLongUnsignedIncrement(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsigned, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedIncrementOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	//Long does not support Tail operator

	public long timeStaticLongSignedNone(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}
	
	public long timeStaticLongSignedNoneOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional,
							OperatorMask.Field_None, 
							0), groupTokenNoMap, 0);
	}

	
	public long timeStaticLongSignedCopy(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedCopyOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional,
						    OperatorMask.Field_Copy, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedConstant(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.LongSigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedDefault(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedDefaultOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional, 
						    OperatorMask.Field_Default, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedDelta(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.LongSigned, 
						OperatorMask.Field_Delta, 
						0), groupTokenNoMap, 0);
	}

	public long timeStaticLongSignedDeltaOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional, 
						    OperatorMask.Field_Delta, 
						     0), groupTokenNoMap, 0);
	}
	
	
	public long timeStaticLongSignedIncrement(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedIncrementOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional, 
						    OperatorMask.Field_Increment, 
						     0), groupTokenMap, 2);
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
			PrimitiveWriter.reset(writer); //clear any values found in writer
			dictionaryFactory.reset(staticWriter.rIntDictionary);
            dictionaryFactory.reset(staticWriter.rLongDictionary);
            dictionaryFactory.reset(staticWriter.byteHeap); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize, writer);
			int j = longTestData.length;
			while (--j>=0) {
				result |= longTestData[j];//do nothing
			}
			RingBuffer ringBuffer = RingBuffers.get(staticReader.ringBuffers,0);
			staticWriter.closeGroup(groupToken, writer);
			staticWriter.flush(writer);

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			FASTDecoder.reset(dictionaryFactory, staticReader); //reset message to clear the previous values
			
			
			staticReader.openGroup(groupToken, pmapSize, reader);
			j = longTestData.length;
			while (--j>=0) {
				result |= j;//doing more nothing.
			}
			int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			staticReader.closeGroup(groupToken,idx, reader);
		}
		return result;
	}
	
	
	
	protected long staticWriteReadLongGroup(int reps, int token, int groupToken, int pmapSize) {
		long result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			PrimitiveWriter.reset(writer); //clear any values found in writer
			
			///Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize, writer);
			int j = longTestData.length;
			while (--j>=0) {
			    StreamingLongTest.writeLong(staticWriter, token, longTestData[j], writer);
			}
			RingBuffer ringBuffer = RingBuffers.get(staticReader.ringBuffers,0);
			
			staticWriter.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER), writer);
			staticWriter.flush(writer);

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticReader.reset(); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken, pmapSize, reader);
			j = longTestData.length;
			while (--j>=0) {
				result |= TestHelper.readLong(token, reader, ringBuffer, staticReader);
			}
			int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			staticReader.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
		}
		return result;
	}
	

	
}
