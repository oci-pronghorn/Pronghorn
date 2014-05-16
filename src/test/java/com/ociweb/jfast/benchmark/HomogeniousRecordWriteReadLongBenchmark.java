//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.benchmark;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import javax.swing.text.MaskFormatter;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.jfast.stream.FASTReaderInterpreterDispatch;
import com.ociweb.jfast.stream.FASTWriterInterpreterDispatch;

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

	static final int[] tokenLookup = buildTokens(fields, types, operators);
	
	static final DictionaryFactory dcr = new DictionaryFactory();
	static {
		dcr.setTypeCounts(fields,fields,fields,fields);
	}
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(internalBufferSize);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter writer = new PrimitiveWriter(internalBufferSize, output, maxGroupCount, false);
	static final PrimitiveReader reader = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final long[] longTestData = new long[] {0,0,1,1,2,2,2000,2002,10000,10001};
	

		
	static final FASTWriterInterpreterDispatch staticWriter = new FASTWriterInterpreterDispatch(writer, dcr, 100, 64, 64, 8, 8, null, 3, new int[0][0],null,64);
	static final FASTReaderInterpreterDispatch staticReader = new FASTReaderInterpreterDispatch(dcr, 3, new int[0][0], 0, 
			                                                                0, 4, 4, null, 64,8, 7, maxGroupCount * 10, 0);
	
	static final int groupTokenNoMap = TokenBuilder.buildToken(TypeMask.Group, 0, 0, TokenBuilder.MASK_ABSENT_DEFAULT);
	static final int groupTokenMap = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_PMap, 0, TokenBuilder.MASK_ABSENT_DEFAULT);
	
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
			
			//When testing decimals the same operator is used for both exponent and mantissa.
			if (tokenType == TypeMask.Decimal || 
				tokenType == TypeMask.DecimalOptional) {
				
				tokenOpp |= tokenOpp<<TokenBuilder.SHIFT_OPER_DECIMAL_EX;
				
			}
			
			lookup[count] = TokenBuilder.buildToken(tokenType, tokenOpp, count, TokenBuilder.MASK_ABSENT_DEFAULT);
					
		}
		return lookup;
		
	}

	public static boolean isInValidCombo(int type, int operator) {
		
		return OperatorMask.Field_Tail==operator && type<=TypeMask.LongSignedOptional;
		
	}

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
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticLongUnsignedNoneOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional,
							OperatorMask.Field_None, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}

	
	public long timeStaticLongUnsignedCopy(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsigned,
						    OperatorMask.Field_Copy, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedCopyOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional,
						    OperatorMask.Field_Copy, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedConstant(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.LongUnsigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedDefault(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsigned, 
						    OperatorMask.Field_Default, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedDefaultOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional, 
						    OperatorMask.Field_Default, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedDelta(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.LongUnsigned, 
						OperatorMask.Field_Delta, 
						0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}

	public long timeStaticLongUnsignedDeltaOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional, 
						    OperatorMask.Field_Delta, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	
	public long timeStaticLongUnsignedIncrement(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsigned, 
						    OperatorMask.Field_Increment, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongUnsignedIncrementOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongUnsignedOptional, 
						    OperatorMask.Field_Increment, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	//Long does not support Tail operator

	public long timeStaticLongSignedNone(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned,
							OperatorMask.Field_None, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	public long timeStaticLongSignedNoneOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional,
							OperatorMask.Field_None, 
							0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}

	
	public long timeStaticLongSignedCopy(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned,
						    OperatorMask.Field_Copy, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedCopyOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional,
						    OperatorMask.Field_Copy, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedConstant(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.LongSigned, //constant operator can not be optional
						    OperatorMask.Field_Constant, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedDefault(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned, 
						    OperatorMask.Field_Default, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedDefaultOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional, 
						    OperatorMask.Field_Default, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedDelta(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.LongSigned, 
						OperatorMask.Field_Delta, 
						0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}

	public long timeStaticLongSignedDeltaOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional, 
						    OperatorMask.Field_Delta, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenNoMap, 0);
	}
	
	
	public long timeStaticLongSignedIncrement(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSigned, 
						    OperatorMask.Field_Increment, 
						     0, TokenBuilder.MASK_ABSENT_DEFAULT), groupTokenMap, 2);
	}
	
	public long timeStaticLongSignedIncrementOptional(int reps) {
		return staticWriteReadLongGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.LongSignedOptional, 
						    OperatorMask.Field_Increment, 
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
			staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize);
			int j = longTestData.length;
			while (--j>=0) {
				result |= longTestData[j];//do nothing
			}
			staticWriter.closeGroup(groupToken);
			staticWriter.flush();

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			staticReader.reset(); //reset message to clear the previous values
			
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
			writer.reset(writer); //clear any values found in writer
			
			///Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, pmapSize);
			int j = longTestData.length;
			while (--j>=0) {
				staticWriter.writeLong(token, longTestData[j]);
			}
			staticWriter.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER));
			staticWriter.flush();

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//staticReader.reset(); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken, pmapSize, reader);
			j = longTestData.length;
			while (--j>=0) {
				result |= staticReader.readLong(token, reader);
			}
			int idx = TokenBuilder.MAX_INSTANCE & groupToken;
			staticReader.closeGroup(groupToken|(OperatorMask.Group_Bit_Close<<TokenBuilder.SHIFT_OPER),idx, reader);
		}
		return result;
	}
	

	
}
