package com.ociweb.jfast.stream;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;



public class StreamingIntegerKryoTest extends BaseStreamingTest {
	
	final int groupToken 	  = buildGroupToken(maxMPapBytes,0);//TODO: repeat still unsupported
	final int[] testData     = buildTestDataUnsigned(fields);
	boolean sendNulls = false;
		
	Output output;
	Input input;
	
	
	@Test
	public void integerUnsignedTest() {
		int[] types = new int[] {
                  TypeMask.IntegerUnsigned,
		    	  TypeMask.IntegerUnsignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None,  //no need for pmap
                OperatorMask.Delta, //no need for pmap
                OperatorMask.Copy,
                OperatorMask.Increment,
                OperatorMask.Constant, //test runner knows not to use with optional
                OperatorMask.Default
                };
				
		tester(types, operators, "KryoUnsignedInteger");
	}
	
	@Test
	public void integerSignedTest() {
		int[] types = new int[] {
                  TypeMask.IntegerSigned,
				  TypeMask.IntegerSignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None,  //no need for pmap
                OperatorMask.Delta, //no need for pmap
                OperatorMask.Copy,
                OperatorMask.Increment,
                OperatorMask.Constant, //test runner knows not to use with optional
                OperatorMask.Default
                };
		tester(types, operators, "KryoSignedInteger");
	}

	@Override
	protected long timeWriteLoop(int fields, int fieldsPerGroup, int maxMPapBytes, int operationIters,
			int[] tokenLookup, DictionaryFactory dcr) {
		
		
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
				
		int i = operationIters;
		int g = fieldsPerGroup;
//		fw.openGroup(groupToken);

		//TODO: change this to  Kryo test on higher level writing object?
		//TODO: move this to low level primitive test code.
		
		while (--i>=0) {
			int f = fields;
		
			while (--f>=0) {
				
				int token = tokenLookup[f]; 
				
				if (sendNulls && ((f&0xF)==0) && (0!=(token&0x1000000))) {
					output.writeVarInt(0, true);
					//fw.write(token);
				} else {
					output.writeVarInt(testData[f],true);
					//fw.write(token, testData[f]); 
				}
							
		//		g = groupManagementWrite(fieldsPerGroup, fw, i, g, groupToken, f);				
			}			
		}
//		if (fw.isGroupOpen()) {
//			fw.closeGroup(groupToken);
//		}
		output.flush();
				
		return System.nanoTime() - start;
	}
	
	@Override
	protected long timeReadLoop(int fields, int fieldsPerGroup, int maxMPapBytes, 
			                      int operationIters, int[] tokenLookup, DictionaryFactory dcr) {
				
		long start = System.nanoTime();
		if (operationIters<3) {
			throw new UnsupportedOperationException("must allow operations to have 3 data points but only had "+operationIters);
		}
			
		int i = operationIters;
		int g = fieldsPerGroup;
		
//		fr.openGroup(groupToken);
		
		while (--i>=0) {
			int f = fields;
			
			while (--f>=0) {
				
				int token = tokenLookup[f]; 	
				
				if (sendNulls && (f&0xF)==0 && (0!=(token&0x1000000))) {
		     		//int value = fr.readInt(tokenLookup[f], Integer.MIN_VALUE);
		     		int value = input.readVarInt(true);
					if (0!=value) {
						assertEquals(0, value);
					}
				} else { 
					//int value = fr.readInt(tokenLookup[f], Integer.MAX_VALUE);
					int value = input.readVarInt(true);
					
					if (testData[f]!=value) {
						System.err.println(TokenBuilder.tokenToString(tokenLookup[f]));
						assertEquals(testData[f], value);
					}
				}
//				g = groupManagementRead(fieldsPerGroup, fr, i, g, groupToken, f);				
			}			
		}
//		if (fr.isGroupOpen()) {
//			fr.closeGroup(groupToken);
//		}
			
		long duration = System.nanoTime() - start;
		return duration;
	}

	public long totalWritten() {
		return output.total();
	}
	
	protected void resetOutputWriter() {
		output.clear();
	}

	protected void buildOutputWriter(int maxGroupCount, byte[] writeBuffer) {
		output = new Output(writeBuffer);
	}


	protected long totalRead() {
		return input.total();
	}
	
	protected void resetInputReader() {
			input.setPosition(0);
	}

	protected void buildInputReader(int maxGroupCount, byte[] writtenData) {
		input = new Input(writtenData);
		input.setPosition(0);
	}

	
}
