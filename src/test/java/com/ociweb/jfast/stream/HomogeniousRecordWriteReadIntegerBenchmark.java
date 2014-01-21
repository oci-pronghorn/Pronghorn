package com.ociweb.jfast.stream;

import static org.junit.Assert.*;

import java.util.List;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.google.caliper.Param;
import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;

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
	          OperatorMask.None, 
			  OperatorMask.Constant,
			  OperatorMask.Copy,
			  OperatorMask.Delta,
			  OperatorMask.Default,
	          OperatorMask.Increment,
	          OperatorMask.Tail
          };

	static final int[] tokenLookup = buildTokens(fields, types, operators);
	static final DictionaryFactory dcr = new DictionaryFactory(fields,fields,fields,singleCharLength,fields,fields,tokenLookup);
	
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(4096);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter pw = new PrimitiveWriter(internalBufferSize, output, maxGroupCount, false);
	static final PrimitiveReader pr = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final int[] intTestData = new int[] {0,0,1,1,2,2,2000,2002,10000,10001};
	static final long[] longTestData = new long[] {0,0,1,1,2,2,2000,2002,10000,10001};
	

		
	static final FASTStaticWriter staticWriter = new FASTStaticWriter(pw, dcr);
	static final FASTStaticReader staticReader = new FASTStaticReader(pr, dcr);
	
	static final int largeGroupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple,4, 0);
	static final int simpleGroupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple,2, 0);
	static final int zeroGroupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple,0, 0);
	
	
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
		
		if (OperatorMask.Constant==operator & isOptional) {
			//constant operator can never be of type optional
			return true;
		}
		
		if (type>=0 && type<=TypeMask.LongSignedOptional) {
			//integer/long types do not support tail
			if (OperatorMask.Tail==operator) {
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
						OperatorMask.None, 
						0), zeroGroupToken);
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
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	public int timeStaticIntegerUnsignedNoneOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
							OperatorMask.None, 
							0), zeroGroupToken);
	}

	
	public int timeStaticIntegerUnsignedCopyWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedCopyOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedConstantWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerUnsigned, //constant operator can not be optional
						    OperatorMask.Constant, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedDefaultWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedDefaultOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedDeltaWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerUnsigned, 
						OperatorMask.Delta, 
						0), zeroGroupToken);
	}

	public int timeStaticIntegerUnsignedDeltaOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Delta, 
						     0), zeroGroupToken);
	}
	
	
	public int timeStaticIntegerUnsignedIncrementWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedIncrementOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	//Integer does not support Tail operator

	public int timeStaticIntegerSignedNoneWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	public int timeStaticIntegerSignedNoneOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
							OperatorMask.None, 
							0), zeroGroupToken);
	}

	
	public int timeStaticIntegerSignedCopyWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedCopyOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedConstantWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerSigned, //constant operator can not be optional
						    OperatorMask.Constant, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedDefaultWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedDefaultOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedDeltaWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerSigned, 
						OperatorMask.Delta, 
						0), zeroGroupToken);
	}

	public int timeStaticIntegerSignedDeltaOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Delta, 
						     0), zeroGroupToken);
	}
	
	
	public int timeStaticIntegerSignedIncrementWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedIncrementOptionalWR(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	
	public int timeStaticIntegerUnsignedNoneW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned,
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	public int timeStaticIntegerUnsignedNoneOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
							OperatorMask.None, 
							0), zeroGroupToken);
	}

	
	public int timeStaticIntegerUnsignedCopyW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedCopyOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedConstantW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerUnsigned, //constant operator can not be optional
						    OperatorMask.Constant, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedDefaultW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedDefaultOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedDeltaW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerUnsigned, 
						OperatorMask.Delta, 
						0), zeroGroupToken);
	}

	public int timeStaticIntegerUnsignedDeltaOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Delta, 
						     0), zeroGroupToken);
	}
	
	
	public int timeStaticIntegerUnsignedIncrementW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerUnsignedIncrementOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	//Integer does not support Tail operator

	public int timeStaticIntegerSignedNoneW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	public int timeStaticIntegerSignedNoneOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
							OperatorMask.None, 
							0), zeroGroupToken);
	}

	
	public int timeStaticIntegerSignedCopyW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedCopyOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedConstantW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(//special because there is no optional constant
							TypeMask.IntegerSigned, //constant operator can not be optional
						    OperatorMask.Constant, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedDefaultW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedDefaultOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Default, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedDeltaW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
						TypeMask.IntegerSigned, 
						OperatorMask.Delta, 
						0), zeroGroupToken);
	}

	public int timeStaticIntegerSignedDeltaOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Delta, 
						     0), zeroGroupToken);
	}
	
	
	public int timeStaticIntegerSignedIncrementW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSigned, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedIncrementOptionalW(int reps) {
		return staticWriteIntegerGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional, 
						    OperatorMask.Increment, 
						     0), simpleGroupToken);
	}
	
	//
	////
	//////
	////
	//
	
	protected int staticWriteReadOverheadGroup(int reps, int token, int groupToken) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			staticWriter.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, 0);
			int j = intTestData.length;
			while (--j>=0) {
				//->439 ->423 -> 408 ->395
				//294 direct write/read and 446 via static call so 152 just for switches!
				//pw.writeIntegerUnsigned(intTestData[j]);//TODO:hak test to isolate switches? was 34 vs 446
				result |= intTestData[j];//do nothing
			}
			staticWriter.closeGroup(groupToken);
			staticWriter.flush();

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			staticReader.reset(dcr); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken);
			j = intTestData.length;
			while (--j>=0) {
				result |= j;//pr.readIntegerUnsigned();////j;//doing more nothing.
			}
			staticReader.closeGroup(groupToken);
		}
		return result;
	}
	
	
	protected int staticWriteReadIntegerGroup(int reps, int token, int groupToken) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			staticWriter.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, 0);
			int j = intTestData.length;
			while (--j>=0) {
				staticWriter.write(token, intTestData[j]);
			}
			staticWriter.closeGroup(groupToken);
			staticWriter.flush();
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			staticReader.reset(dcr); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken);
			j = intTestData.length;
			while (--j>=0) {
				result |= staticReader.readInt(token, 0);
			}
			staticReader.closeGroup(groupToken);
		}
		return result;
	}
	
	protected int staticWriteIntegerGroup(int reps, int token, int groupToken) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			staticWriter.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, 0);
			int j = intTestData.length;
			while (--j>=0) {
				staticWriter.write(token, intTestData[j]);
			}
			staticWriter.closeGroup(groupToken);
			staticWriter.flush();
			
		}
		return result;
	}
	
}
