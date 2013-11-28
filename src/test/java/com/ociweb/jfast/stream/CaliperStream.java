package com.ociweb.jfast.stream;

import com.google.caliper.Benchmark;
import com.google.caliper.runner.CaliperMain;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputByteArray;
import com.ociweb.jfast.primitive.adapter.FASTOutputByteArray;

public class CaliperStream extends Benchmark {

	

	
	public static void main(String[] args) { 
		args = new String[]{"-t","3"};
		CaliperMain.main(CaliperStream.class, args); 
				
	}
	
	IntegerStreamingTest test = new IntegerStreamingTest();

	
	final int fields         = 600;
	final int[] testData    = test.buildTestDataUnsigned(fields);
	final int fieldsPerGroup = 10;
	final int maxMPapBytes   = (int)Math.ceil(fieldsPerGroup/7d);
	final int groupToken = test.buildGroupToken(maxMPapBytes,0);//TODO: repeat unsupported

	int operationIters;
	int[] tokenLookup;
	FASTOutputByteArray output;
	PrimitiveWriter pw;
	FASTStaticWriter fw;
	
	FASTInputByteArray input;
	PrimitiveReader pr;
	FASTStaticReader fr;
	
	
	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
		
		System.out.println("data bytes:"+pr.totalRead());
		
	}


	@Override
	protected void setUp() throws Exception {
		operationIters = 1;//fields;
		int[] types = new int[] {
				  TypeMask.IntegerUnSigned,
				  TypeMask.IntegerUnSignedOptional,
				  };
		
		int[] operators = new int[] {
                OperatorMask.None, 
				 // OperatorMask.Constant,
				//  OperatorMask.Copy,
				 // OperatorMask.Delta,
				 // OperatorMask.Default,
               // OperatorMask.Increment,
                };

		tokenLookup = test.buildTokens(fields, types, operators);
		
		int streamByteSize = operationIters*((maxMPapBytes*(fields/fieldsPerGroup))+(fields*6));
		
		byte[] writeBuffer = new byte[streamByteSize];
		int maxGroupCount = operationIters*fields/fieldsPerGroup;
		
		output = new FASTOutputByteArray(writeBuffer);
		pw = new PrimitiveWriter(streamByteSize, output, maxGroupCount, false);
		fw = new FASTStaticWriter(pw, fields, tokenLookup);
		
		input = new FASTInputByteArray(writeBuffer);
		pr = new PrimitiveReader(streamByteSize*10, input, maxGroupCount*10);
		fr = new FASTStaticReader(pr, fields, tokenLookup);
	}


//	public void testVoid() {
//		
//		
//		test.writeReadTest(operationIters, tokenLookup, output, pw, fw, input, pr, fr);
//		
//		
//	}
	

	public void timeInteger(int reps) {
        for (int i = 0; i < reps; i++) {

        	test.writeReadTest(fields, fieldsPerGroup, operationIters, tokenLookup, output, pw, fw, input, pr, fr);
        	
        }
       // System.err.println("reps:"+reps+" bytes "+pr.totalRead());
    }
	
}
