//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.benchmark;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.google.caliper.Benchmark;
import com.ociweb.jfast.generator.StaticGlue;
import com.ociweb.jfast.catalog.loader.DictionaryFactory;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteBuffer;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteBuffer;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;

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
		dcr.setTypeCounts(fields,fields,fields, 16, 128);
	}
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(4096);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter writer = new PrimitiveWriter(internalBufferSize, output, false);
	static final PrimitiveReader reader = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final int[] intTestData = new int[] {0,0,1,1,2,2,2000,2002,10000,10001};
	static final long[] longTestData = new long[] {0,0,1,1,2,2,2000,2002,10000,10001};
	
	static final int[] wIntDictionary = dcr.integerDictionary();
	static final int[] wIntInit = dcr.integerDictionary();
	static final int wIntInstanceMask =  Math.min(TokenBuilder.MAX_INSTANCE, (wIntDictionary.length - 1));
		
	static final int[] rIntDictionary = dcr.integerDictionary();
	static final int[] rIntInit = dcr.integerDictionary();
	static final int MAX_INT_INSTANCE_MASK = Math.min(TokenBuilder.MAX_INSTANCE, (rIntDictionary.length-1));
		
	
	static final int largeGroupToken = TokenBuilder.buildToken(TypeMask.Group,OperatorMask.Group_Bit_PMap,4);
	static final int simpleGroupToken = TokenBuilder.buildToken(TypeMask.Group,OperatorMask.Group_Bit_PMap,2);
	static final int zeroGroupToken = TokenBuilder.buildToken(TypeMask.Group,0,0);
	
	
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

    @Test
    public void testSize() {
    	int rep = 20;
    	timeStaticIntegerSignedConstantWR(rep);
    	System.out.println("SignedConst "+(writer.totalWritten(writer)/(float)(intTestData.length)));
    	timeStaticIntegerSignedCopyOptionalWR(rep);
    	System.out.println("SignedCopyOptional "+(writer.totalWritten(writer)/(float)(intTestData.length)));
    	timeStaticIntegerSignedDeltaOptionalWR(rep);
    	System.out.println("SignedDeltaOptional "+(writer.totalWritten(writer)/(float)(intTestData.length)));
    	
    }
	
	public int timeStaticIntegerSignedCopyOptionalWR(int reps) {
		return staticWriteReadSignedCopyOptionalRecord(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Field_Copy, 
						     0), simpleGroupToken,2);
	}
	
	public int timeStaticIntegerSignedConstantWR(int reps) {
		return staticWriteReadSignedConstantRecord(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Field_Copy, 
						     0), simpleGroupToken,2);
	}

	public int timeStaticIntegerSignedDeltaOptionalWR(int reps) {
		return staticWriteReadSignedDeltaOptionalRecord(reps, 
				TokenBuilder.buildToken(
							TypeMask.IntegerSignedOptional,
						    OperatorMask.Field_Copy, 
						     0), simpleGroupToken, 2);
	}
	
	
	protected int staticWriteReadSignedCopyOptionalRecord(int reps, int token, int groupToken, int pmapSize) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			writer.reset(writer); //clear any values found in writer
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//fw.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			
			if (pmapSize>0) {
				writer.openPMap(pmapSize, writer);
			}
			
			int j = intTestData.length;
			while (--j>=0) {						
				int idx = token & wIntInstanceMask;
                int value = intTestData[j];
				
				if (value >= 0) {
                    value++;
                }
                
                if (value == wIntDictionary[idx]) {
                    PrimitiveWriter.writePMapBit((byte) 0, writer);
                } else {
                    PrimitiveWriter.writePMapBit((byte) 1, writer);
                    PrimitiveWriter.writeIntegerSigned(wIntDictionary[idx] = value, writer);
                }
			}
			
			if (pmapSize>0) {
				writer.closePMap(writer);
			}
			
			writer.flush(writer);
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//fr.reset(dcr); //reset message to clear the previous values
			int constAbsent = TokenBuilder.absentValue32(TokenBuilder.MASK_ABSENT_DEFAULT);
			
			if (pmapSize>0) {
					PrimitiveReader.openPMap(pmapSize, reader);
			}
			j = intTestData.length;
			while (--j>=0) {
				int readFromIdx = -1;
				int target = token&MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;				
				
				int value = rIntDictionary[target] = (PrimitiveReader.readPMapBit(reader) == 0 ? rIntDictionary[source] :  PrimitiveReader.readIntegerSigned(reader));
				result |= (0 == value ? constAbsent: (value>0 ? value-1 : value));
			}
			if (pmapSize>0) {
				PrimitiveReader.closePMap(reader);
			}
		}
		return result;
	}
	
	protected int staticWriteReadSignedConstantRecord(int reps, int token, int groupToken, int pmapSize) {
		int result = 0;
		int constantValue = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			writer.reset(writer); //clear any values found in writer
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//fw.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			
			if (pmapSize>0) {
				writer.openPMap(pmapSize, writer);
			}
			
			int j = intTestData.length;
			while (--j>=0) {						
				assert(wIntDictionary[ token & wIntInstanceMask]==constantValue) : "Only the constant value "+wIntDictionary[ token & wIntInstanceMask]+" from the template may be sent";
				//nothing need be sent because constant does not use pmap and the template
				//on the other receiver side will inject this value from the template
			}
			
			if (pmapSize>0) {
				writer.closePMap(writer);
			}
			
			writer.flush(writer);
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//fr.reset(dcr); //reset message to clear the previous values
			
			if (pmapSize>0) {
					PrimitiveReader.openPMap(pmapSize, reader);
			}
			j = intTestData.length;
			while (--j>=0) {
				result |= rIntDictionary[token & MAX_INT_INSTANCE_MASK];
			}
			if (pmapSize>0) {
				PrimitiveReader.closePMap(reader);
			}
		}
		return result;
	}
	
	protected int staticWriteReadSignedDeltaOptionalRecord(int reps, int token, int groupToken, int pmapSize) {
		int result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			writer.reset(writer); //clear any values found in writer
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//fw.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			
			if (pmapSize>0) {
				writer.openPMap(pmapSize, writer);
			}
			
			int j = intTestData.length;
			while (--j>=0) {						
				int idx = token & wIntInstanceMask;
                int value = intTestData[j];
				
				int last = wIntDictionary[idx];
                if (value > 0 == last > 0) { // optimization using int when possible instead of long
                    int dif = value - last;
                    wIntDictionary[idx] = value;
                    PrimitiveWriter.writeIntegerSigned(dif >= 0 ? 1 + dif : dif, writer);
                } else {
                    long dif = value - (long) last;
                    wIntDictionary[idx] = value;
                    PrimitiveWriter.writeLongSigned(dif >= 0 ? 1 + dif : dif, writer);
                }
			}
			
			if (pmapSize>0) {
				writer.closePMap(writer);
			}
			
			writer.flush(writer);
			
			//13 to 18 bytes per record with 10 fields, It would be nice if caliper can display this but how?
			//System.err.println("bytes written:"+pw.totalWritten()+" for "+TokenBuilder.tokenToString(token));

			input.reset(); //for testing reset bytes back to the beginning.
			PrimitiveReader.reset(reader);//for testing clear any data found in reader 
			
			//Not a normal part of read/write record and will slow down test (would be needed per template)
			//fr.reset(dcr); //reset message to clear the previous values
			int constAbsent = TokenBuilder.absentValue32(TokenBuilder.MASK_ABSENT_DEFAULT);
			
			if (pmapSize>0) {
					PrimitiveReader.openPMap(pmapSize, reader);
			}
			j = intTestData.length;
			while (--j>=0) {
				int readFromIdx = -1;
				int target = token&MAX_INT_INSTANCE_MASK;
				int source = readFromIdx>0? readFromIdx&MAX_INT_INSTANCE_MASK : target;
                // Delta opp never uses PMAP
                long value = PrimitiveReader.readLongSigned(reader);
                int result1;
                if (0 == value) {
                    rIntDictionary[target] = 0;// set to absent
                    result1 = constAbsent;
                } else {
                    result1 = rIntDictionary[target] = (int) (rIntDictionary[source] + (value > 0 ? value - 1 : value));
                }
				result |= result1;
			}
			if (pmapSize>0) {
				PrimitiveReader.closePMap(reader);
			}
		}
		return result;
	}
	
}
