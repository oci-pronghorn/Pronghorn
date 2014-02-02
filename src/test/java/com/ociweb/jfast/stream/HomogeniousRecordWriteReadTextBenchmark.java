package com.ociweb.jfast.stream;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

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
	          OperatorMask.None, 
			  OperatorMask.Constant,
			  OperatorMask.Copy,
			  OperatorMask.Delta,
			  OperatorMask.Default,
	          OperatorMask.Increment,
	          OperatorMask.Tail
          };

	static final int[] tokenLookup = buildTokens(fields, types, operators);
	static final DictionaryFactory dcr = new DictionaryFactory(fields,fields,fields,singleCharLength,fields,fields, tokenLookup);
	
	static final ByteBuffer directBuffer = ByteBuffer.allocateDirect(internalBufferSize);
	
	static final FASTOutputByteBuffer output = new FASTOutputByteBuffer(directBuffer);
	static final FASTInputByteBuffer input = new FASTInputByteBuffer(directBuffer);
		
	static final PrimitiveWriter pw = new PrimitiveWriter(internalBufferSize, output, maxGroupCount, false);
	static final PrimitiveReader pr = new PrimitiveReader(internalBufferSize, input, maxGroupCount*10);

	static final CharSequence[] textTestData = new CharSequence[]{"","","a","a","ab","ab","abcd","abcd","abcdefgh","abcdefgh"};
	
	

		
	static final FASTWriterDispatch staticWriter = new FASTWriterDispatch(pw, dcr);
	static final FASTReaderDispatch staticReader = new FASTReaderDispatch(pr, dcr);
	
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

	//TODO: split these tests into different classes per type for easier comparison.

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
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	public long timeStaticTextASCIINoneOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	public long timeStaticTextUTF8None(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	public long timeStaticTextUTF8NoneOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.None, 
							0), zeroGroupToken);
	}
	
	////
	
	public long timeStaticTextASCIIConstant(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Constant, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextASCIIConstantOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Constant, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8Constant(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Constant, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8ConstantOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Constant, 
							0), simpleGroupToken);
	}
	
	////
	
	public long timeStaticTextASCIICopy(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Copy, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextASCIICopyOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Copy, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8Copy(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Copy, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8CopyOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Copy, 
							0), simpleGroupToken);
	}
	
	/////
		
	public long timeStaticTextASCIIDefault(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Default, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextASCIIDefaultOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Default, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8Default(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Default, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8DefaultOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Default, 
							0), simpleGroupToken);
	}
	
	////
	
	@Test
	public void testThis() {
		assertTrue(0==timeStaticTextUTF8DeltaOptional(20));
	}
	
	
	public long timeStaticTextASCIIDelta(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Delta, 
							0), zeroGroupToken);
	}
	
	public long timeStaticTextASCIIDeltaOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Delta, 
							0), zeroGroupToken);
	}
	
	public long timeStaticTextUTF8Delta(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Delta, 
							0), zeroGroupToken);
	}
	
	public long timeStaticTextUTF8DeltaOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Delta, 
							0), zeroGroupToken);
	}
	
	////
	
	
	public long timeStaticTextASCIITail(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCII,
							OperatorMask.Tail, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextASCIITailOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextASCIIOptional,
							OperatorMask.Tail, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8Tail(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8,
							OperatorMask.Tail, 
							0), simpleGroupToken);
	}
	
	public long timeStaticTextUTF8TailOptional(int reps) {
		return staticWriteReadTextGroup(reps, 
				TokenBuilder.buildToken(
							TypeMask.TextUTF8Optional,
							OperatorMask.Tail, 
							0), simpleGroupToken);
	}
	
	
	//
	////
	//////
	////
	//
	
	protected int staticWriteReadOverheadGroup(int reps) {
		int result = 0;
		int groupToken = zeroGroupToken;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			staticWriter.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, 0);
			int j = textTestData.length;
			while (--j>=0) {
				result |= textTestData[j].length();//do nothing
			}
			staticWriter.closeGroup(groupToken);
			staticWriter.flush();

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			staticReader.reset(dcr); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken);
			j = textTestData.length;
			while (--j>=0) {
				result |= j;//doing more nothing.
			}
			staticReader.closeGroup(groupToken);
		}
		return result;
	}
	
	
	
	protected long staticWriteReadTextGroup(int reps, int token, int groupToken) {
		long result = 0;
		for (int i = 0; i < reps; i++) {
			output.reset(); //reset output to start of byte buffer
			pw.reset(); //clear any values found in writer
			staticWriter.reset(dcr); //reset message to clear out old values;
			
			//////////////////////////////////////////////////////////////////
			//This is an example of how to use the staticWriter
			//Note that this is fast but does not allow for dynamic templates
			//////////////////////////////////////////////////////////////////
			staticWriter.openGroup(groupToken, 0);
			int j = textTestData.length;
			while (--j>=0) {
				staticWriter.write(token, textTestData[j]);
			}
			staticWriter.closeGroup(groupToken);
			staticWriter.flush();

			input.reset(); //for testing reset bytes back to the beginning.
			pr.reset();//for testing clear any data found in reader 
			
			staticReader.reset(dcr); //reset message to clear the previous values
			
			staticReader.openGroup(groupToken);
			j = textTestData.length;
			while (--j>=0) {
				result |= staticReader.readText(token);
			}
			staticReader.closeGroup(groupToken);
		}
		return result;
	}
	

	
}
