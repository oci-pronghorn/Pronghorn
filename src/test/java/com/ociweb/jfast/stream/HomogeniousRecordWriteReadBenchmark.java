package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;

public class HomogeniousRecordWriteReadBenchmark extends Benchmark {

	//Caliper tests///////////////
	//Write and read 1 record, this will time the duration of sending a fixed value end to end.
	//--The records contain 10 fields of the same type and compression operation
	//--This is an estimate of what kind of throughput can be achieved given some test data.
	//--Both the static and dynamic readers/writers will be tested for full comparisons
	//--lowLatency vs bandwidth optimized should both be tested
	//	
	//This is NOT the same as the other tests which measure the duration to produce 1 byte on the stream.
	//--The ns/byte tests are an estimate of how much bandwidth can be saturated given the CPU available.
	
	static final int internalBufferSize = 1024;
	static final int maxGroupCount = 10;
	static final int fields = 10;
	
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(4096);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter pw = new PrimitiveWriter(internalBufferSize, output, maxGroupCount, false);
	static final PrimitiveReader pr = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final int[] intTestData = new int[] {0,0,1,1,2,2,2000,2002,10000,10001};
	
	//list all types
	static final int[] types = new int[] {
			  TypeMask.IntegerUnsigned,
			  TypeMask.IntegerUnsignedOptional,
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
		
	static final FASTStaticWriter staticWriter = new FASTStaticWriter(pw, fields, tokenLookup);
	static final FASTStaticReader staticReader = new FASTStaticReader(pr, fields, tokenLookup);
	
	static final int groupToken = buildGroupToken(10,0);

	
	public static int buildGroupToken(int maxPMapBytes, int repeat) {
		
		return 	0x80000000 |
				maxPMapBytes<<20 |
	            (repeat&0xFFFFF);
		
	}
	
	public static int[] buildTokens(int count, int[] types, int[] operators) {
		int[] lookup = new int[count];
		int typeIdx = types.length-1;
		int opsIdx = operators.length-1;
		while (--count>=0) {
			//high bit set
			//  7 bit type (must match method)
			//  4 bit operation (must match method)
			// 20 bit instance (MUST be lowest for easy mask and frequent use)
			
			int tokenType = types[typeIdx];
			int tokenOpp = operators[opsIdx];
			lookup[count] = buildToken(tokenType, tokenOpp, count);
					
			//find next pattern to be used, rotating over them all.
			do {
			if (--typeIdx<0) {
				if (--opsIdx<0) {
					opsIdx = operators.length-1;
				}
				typeIdx = types.length-1;
			}
			} while (isInValidCombo(types[typeIdx],operators[opsIdx]));
		}
		return lookup;
		
	}

	private static boolean isInValidCombo(int type, int operator) {
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

	protected static int buildToken(int tokenType, int tokenOpp, int count) {
		return 0x80000000 |  
				       (tokenType<<24) |
				       (tokenOpp<<20) |
				       count;
	}
	
	public int timeStaticIntegerUnsignedNone(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsigned,
							OperatorMask.None, 
							0));
	}
	
	public int timeStaticIntegerUnsignedNoneOptional(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsignedOptional,
							OperatorMask.None, 
							0));
	}

	
	public int timeStaticIntegerUnsignedCopy(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsigned,
						    OperatorMask.Copy, 
						     0));
	}
	
	public int timeStaticIntegerUnsignedCopyOptional(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsignedOptional,
						    OperatorMask.Copy, 
						     0));
	}
	
	public int timeStaticIntegerUnsignedConstant(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(//special because there is no optional constant
							TypeMask.IntegerUnsigned, //constant operator can not be optional
						    OperatorMask.Constant, 
						     0));
	}
	
	public int timeStaticIntegerUnsignedDefault(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Default, 
						     0));
	}
	
	public int timeStaticIntegerUnsignedDefaultOptional(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Default, 
						     0));
	}
	
	public int timeStaticIntegerUnsignedDelta(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
						TypeMask.IntegerUnsigned, 
						OperatorMask.Delta, 
						0));
	}

	public int timeStaticIntegerUnsignedDeltaOptional(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Delta, 
						     0));
	}
	
	
	public int timeStaticIntegerUnsignedIncrement(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsigned, 
						    OperatorMask.Increment, 
						     0));
	}
	
	public int timeStaticIntegerUnsignedIncrementOptional(int reps) {
		return staticWriteReadIntegerGroup(reps, 
				buildToken(
							TypeMask.IntegerUnsignedOptional, 
						    OperatorMask.Increment, 
						     0));
	}
	
	//Integer does not support Tail operator

	protected int staticWriteReadIntegerGroup(int reps, int token) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			staticWriter.reset(); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken);
			int j = intTestData.length;
			while (--j>=0) {
				staticWriter.write(token, intTestData[j]);
			}
			staticWriter.closeGroup();
			staticWriter.flush();

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			staticReader.reset(); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken);
			j = intTestData.length;
			while (--j>=0) {
				result |= staticReader.readInt(token, 0);
			}
			staticReader.closeGroup();
		}
		return result;
	}
	
	
}
