package com.ociweb.jfast.stream;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.field.DictionaryFactory;
import com.ociweb.jfast.field.FieldReaderInteger;
import com.ociweb.jfast.field.FieldWriterInteger;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;

public class HomogeniousFieldWriteReadIntegerBenchmark extends Benchmark {

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
	
	static final FieldWriterInteger fw = new FieldWriterInteger(pw,dcr.integerDictionary());
	static final FieldReaderInteger fr = new FieldReaderInteger(pr,dcr.integerDictionary());
		
	
	static final int largeGroupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple,4, 0);
	static final int simpleGroupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple,2, 0);
	static final int zeroGroupToken = TokenBuilder.buildGroupToken(TypeMask.GroupSimple,0, 0);
	
	
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

    @Test
    public void testSize() {
    	int rep = 20;
    	timeStaticIntegerSignedConstantWR(rep);
    	System.out.println("SignedConst "+(pw.totalWritten()/(float)(intTestData.length)));
    	timeStaticIntegerSignedCopyOptionalWR(rep);
    	System.out.println("SignedCopyOptional "+(pw.totalWritten()/(float)(intTestData.length)));
    	timeStaticIntegerSignedDeltaOptionalWR(rep);
    	System.out.println("SignedDeltaOptional "+(pw.totalWritten()/(float)(intTestData.length)));
    	
    }
	
	public int timeStaticIntegerSignedCopyOptionalWR(int reps) {
		return staticWriteReadSignedCopyOptionalRecord(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	public int timeStaticIntegerSignedConstantWR(int reps) {
		return staticWriteReadSignedConstantRecord(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}

	public int timeStaticIntegerSignedDeltaOptionalWR(int reps) {
		return staticWriteReadSignedDeltaOptionalRecord(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Copy, 
						     0), simpleGroupToken);
	}
	
	
	protected int staticWriteReadSignedCopyOptionalRecord(int reps, int token, int groupToken) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			fw.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			
			int maxBytes = TokenBuilder.extractMaxBytes(groupToken);
			if (maxBytes>0) {
				pw.openPMap(maxBytes);
			}
			
			int j = intTestData.length;
			while (--j>=0) {						
				fw.writeIntegerSignedCopyOptional(intTestData[j], token);
			}
			
			if (maxBytes>0) {
				pw.closePMap();
			}
			
			fw.flush();
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			fr.reset(dcr); //reset message to clear the previous values
			
			if (maxBytes>0) {
					pr.readPMap(maxBytes); //TODO: rename method?
			}
			j = intTestData.length;
			while (--j>=0) {
				result |= fr.readIntegerSignedCopyOptional(token, 0);
			}
			if (maxBytes>0) {
				pr.popPMap();
			}
		}
		return result;
	}
	
	protected int staticWriteReadSignedConstantRecord(int reps, int token, int groupToken) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			fw.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			
			int maxBytes = TokenBuilder.extractMaxBytes(groupToken);
			if (maxBytes>0) {
				pw.openPMap(maxBytes);
			}
			
			int j = intTestData.length;
			while (--j>=0) {						
				fw.writeIntegerSignedConstant(intTestData[j], token);
			}
			
			if (maxBytes>0) {
				pw.closePMap();
			}
			
			fw.flush();
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			fr.reset(dcr); //reset message to clear the previous values
			
			if (maxBytes>0) {
					pr.readPMap(maxBytes); //TODO: rename method?
			}
			j = intTestData.length;
			while (--j>=0) {
				result |= fr.readIntegerSignedConstant(token);
			}
			if (maxBytes>0) {
				pr.popPMap();
			}
		}
		return result;
	}
	
	protected int staticWriteReadSignedDeltaOptionalRecord(int reps, int token, int groupToken) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			fw.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			
			int maxBytes = TokenBuilder.extractMaxBytes(groupToken);
			if (maxBytes>0) {
				pw.openPMap(maxBytes);
			}
			
			int j = intTestData.length;
			while (--j>=0) {						
				fw.writeIntegerSignedDeltaOptional(intTestData[j], token);
			}
			
			if (maxBytes>0) {
				pw.closePMap();
			}
			
			fw.flush();
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			fr.reset(dcr); //reset message to clear the previous values
			
			if (maxBytes>0) {
					pr.readPMap(maxBytes); //TODO: rename method?
			}
			j = intTestData.length;
			while (--j>=0) {
				result |= fr.readIntegerSignedDeltaOptional(token,0);
			}
			if (maxBytes>0) {
				pr.popPMap();
			}
		}
		return result;
	}
	
}
