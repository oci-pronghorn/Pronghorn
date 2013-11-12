package com.ociweb.jfast;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.ociweb.jfast.field.util.Field;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveReaderWriterTest;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.primitive.adapter.FASTOutputStream;
import com.ociweb.jfast.read.FieldType;
import com.ociweb.jfast.tree.GroupBuilder;
import com.ociweb.jfast.tree.Necessity;

public class GroupReadWriteTest {
/**
 * Does no compression making use of Dictionary all fields are present.
 * Tests encoding and decoding of all the simple field types.
 */
	private static final int AVG_FIELD_SIZE 		= 10;//big enough for everything tested here.
	private static final int WARMUP_REPEAT  		= 15;
	private static final FASTAccept NO_TESTER = new FASTAcceptNone();
	private static final int cycles = 10000;
	
	//TODO: builder helper test methods for each of these types that iterate over the tested types.
	@Test
	public void testString() {
		testStringUTF8Nullable();
		testStringUTF8();
		testStringASCIINullable();
		testStringASCII();
	}
	
	@Test
	public void testInteger() {
		testInt();
		testIntNullable();
		testIntNullableBoxed();
		testIntU();
		testIntUNullable();
		testLong();
		testLongNullable();
		testLongU();
		testLongUNullable();
	}
	
	@Test
	public void testScaled() {
		testDecimal();
		testDecimalNullable();
		testDecimalNullableBoxed();
	}
	
	@Test
	public void testByteArray() {
		testBytes();
		testBytesNullable();
		testBytesNullableBoxed();
	}
	
	public void testDecimal() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedLongData.length;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		
		final int[] exponentData = new int[j];
		final long[] mantissaData = new long[j];
		
		while (--j>=0) {
			exponentData[j] = j%5;
			mantissaData[j] = -PrimitiveReaderWriterTest.unsignedLongData[--k];
					
			if (k==0) {
				k=length;
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(mantissaData,exponentData);
			}			
		};
		
		FieldType type = FieldType.Decimal;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}
	
	public void testDecimalNullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedIntData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		
		final int[] exponentData = new int[j];
		final long[] mantissaData = new long[j];
		
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				
				exponentData[j] = 0;
				mantissaData[j] = 0;
				
				k--;
			} else {
				nulls[j] = false;
				
				exponentData[j] = j%5;
				mantissaData[j] = -PrimitiveReaderWriterTest.unsignedLongData[--k];
				
				if (k==0) {
					k=length;
				}
			}
		}	
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, mantissaData, exponentData);
			}			
		};
		
		FieldType type = FieldType.Decimal;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);
	}
	
	public void testDecimalNullableBoxed() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedIntData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		
		final Integer[] exponentData = new Integer[j];
		final Long[] mantissaData = new Long[j];
		
		while (--j>=0) {
			if (k==length) {
				exponentData[j] = null;
				mantissaData[j] = null;
				
				k--;
			} else {
				exponentData[j] = j%5;
				mantissaData[j] = -PrimitiveReaderWriterTest.unsignedLongData[--k];
				
				if (k==0) {
					k=length;
				}
			}
		}	
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArrayBoxed(mantissaData, exponentData);
			}			
		};
		
		FieldType type = FieldType.Decimal;
		typeSpecificTester("B_", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.optional, Operator.None, FASTProvideArrayBoxed.IDX_DECIMAL);
		typeSpecificTester("B_", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.optional, Operator.None, FASTProvideArrayBoxed.IDX_DECIMAL);
	}

	public void testInt() {
	
		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedIntData.length;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final int[] data = new int[j];
		while (--j>=0) {
			data[j] = -PrimitiveReaderWriterTest.unsignedIntData[--k];
			if (k==0) {
				k=length;
			}
		}	
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(data);
			}			
		};
		
		FieldType type = FieldType.Int32;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}

	
	public void testIntNullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedIntData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		final int[] data = new int[j];
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				data[j] = 0;
				k--;
			} else {
				nulls[j] = false;
				data[j] = -PrimitiveReaderWriterTest.unsignedIntData[--k];
				if (k==0) {
					k=length;
				}
			}
		}	
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, data);
			}			
		};
						
		FieldType type = FieldType.Int32;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);
	}
	
	public void testIntNullableBoxed() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedIntData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final Integer[] data = new Integer[j];
		while (--j>=0) {
			if (k==length) {
				data[j] = null;
				k--;
			} else {
				data[j] = -PrimitiveReaderWriterTest.unsignedIntData[--k];
				if (k==0) {
					k=length;
				}
			}
		}	
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArrayBoxed(data);
			}			
		};

		FieldType type = FieldType.Int32;
		typeSpecificTester("B_", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.optional, Operator.None, FASTProvideArrayBoxed.IDX_INTEGER);
		typeSpecificTester("B_", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.optional, Operator.None, FASTProvideArrayBoxed.IDX_INTEGER);
		
	}

	public void testIntU() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedIntData.length;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final int[] data = new int[j];
		while (--j>=0) {
			data[j] = PrimitiveReaderWriterTest.unsignedIntData[--k];
			if (k==0) {
				k=length;
			}
		}		
		//////////////////////////	
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(data);
			}			
		};
		
		FieldType type = FieldType.uInt32;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}
	
	public void testIntUNullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedIntData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		final int[] data = new int[j];
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				data[j] = 0;
				k--;
			} else {
				nulls[j] = false;
				data[j] = PrimitiveReaderWriterTest.unsignedIntData[--k];
				if (k==0) {
					k=length;
				}
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, data);
			}			
		};
		
		FieldType type = FieldType.uInt32;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);
	}

	public void testLong() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedLongData.length;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final long[] data = new long[j];
		while (--j>=0) {
			data[j] = -PrimitiveReaderWriterTest.unsignedLongData[--k];
			if (k==0) {
				k=length;
			}
		}	
		//////////////////////////		
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(data);
			}			
		};
		
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, FieldType.Int64, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, FieldType.Int64, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}

	public void testLongNullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedLongData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		final long[] data = new long[j];
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				data[j] = 0;
				k--;
			} else {
				nulls[j] = false;
				data[j] = -PrimitiveReaderWriterTest.unsignedLongData[--k];
				if (k==0) {
					k=length;
				}
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, data);
			}			
		};
				
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, FieldType.Int64, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, FieldType.Int64, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);
	}
	

	public void testLongU() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedLongData.length;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final long[] data = new long[j];
		while (--j>=0) {
			data[j] = PrimitiveReaderWriterTest.unsignedLongData[--k];
			if (k==0) {
				k=length;
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(data);
			}			
		};
		
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, FieldType.uInt64, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, FieldType.uInt64, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}
	

	public void testLongUNullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.unsignedLongData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		final long[] data = new long[j];
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				data[j] = 0;
				k--;
			} else {
				nulls[j] = false;
				data[j] = PrimitiveReaderWriterTest.unsignedLongData[--k];
				if (k==0) {
					k=length;
				}
			}
		}	
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, data);
			}			
		};
		
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, FieldType.uInt64, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, FieldType.uInt64, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);
	}
	


	public void testStringASCII() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.STRING_SPEED_TEST_LIMIT;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final String[] data = new String[j];
		while (--j>=0) {
			data[j] = PrimitiveReaderWriterTest.stringData[--k];
			if (k==0) {
				k=length;
			}
		}	
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(data);
			}			
		};
		
		FieldType type = FieldType.CharsASCII;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}

	public void testStringASCIINullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.STRING_SPEED_TEST_LIMIT+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		final String[] data = new String[j];
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				data[j] = null;
				k--;
			} else {
				nulls[j] = false;
				data[j] = PrimitiveReaderWriterTest.stringData[--k];
				if (k==0) {
					k=length;
				}
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, data);
			}			
		};
		
		FieldType type = FieldType.CharsASCII;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);	
	}

	public void testStringUTF8() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.STRING_SPEED_TEST_LIMIT;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final String[] data = new String[j];
		while (--j>=0) {
			data[j] = PrimitiveReaderWriterTest.stringData[--k];
			if (k==0) {
				k=length;
			}
		}
		FASTProvideArray provider = new FASTProvideArray(data);		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(data);
			}			
		};
		
		FieldType type = FieldType.CharsUTF8;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}

	public void testStringUTF8Nullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.STRING_SPEED_TEST_LIMIT+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		final String[] data = new String[j];
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				data[j] = null;
				k--;
			} else {
				nulls[j] = false;
				data[j] = PrimitiveReaderWriterTest.stringData[--k];
				if (k==0) {
					k=length;
				}
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, data);
			}			
		};
		
		FieldType type = FieldType.CharsUTF8;
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, type, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, type, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);	
	}

	public void testBytes() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.byteData.length;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final byte[][] data = new byte[j][];
		while (--j>=0) {
			data[j] = PrimitiveReaderWriterTest.byteData[--k];
			if (k==0) {
				k=length;
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(data);
			}			
		};
		
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, FieldType.Bytes, 1, Necessity.mandatory, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, FieldType.Bytes, WARMUP_REPEAT, Necessity.mandatory, Operator.None, -1);
	}

	public void testBytesNullable() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.byteData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final boolean[] nulls = new boolean[j];
		final byte[][] data = new byte[j][];
		while (--j>=0) {
			if (k==length) {
				nulls[j] = true;
				data[j] = null;
				k--;
			} else {
				nulls[j] = false;
				data[j] = PrimitiveReaderWriterTest.byteData[--k];
				if (k==0) {
					k=length;
				}
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArray(nulls, data);
			}			
		};
		
		typeSpecificTester("__", new FASTAcceptTester(c), c, testSize, FieldType.Bytes, 1, Necessity.optional, Operator.None, -1);
		typeSpecificTester("__", NO_TESTER, c, testSize, FieldType.Bytes, WARMUP_REPEAT, Necessity.optional, Operator.None, -1);
	}
	
	public void testBytesNullableBoxed() {

		//////////////////////////
		//build test data provider
		//////////////////////////
		int length = PrimitiveReaderWriterTest.byteData.length+1;
		final int testSize = cycles*length;
		int j = testSize;
		int k = length;
		final byte[][] data = new byte[j][];
		while (--j>=0) {
			if (k==length) {
				data[j] = null;
				k--;
			} else {
				data[j] = PrimitiveReaderWriterTest.byteData[--k];
				if (k==0) {
					k=length;
				}
			}
		}		
		//////////////////////////
		Callable<FASTProvide> c = new Callable<FASTProvide>() {
			@Override
			public FASTProvide call() throws Exception {
				return new FASTProvideArrayBoxed(data);
			}			
		};
		typeSpecificTester("B_", new FASTAcceptTester(c), c, testSize, FieldType.Bytes, 1, Necessity.optional, Operator.None, FASTProvideArrayBoxed.IDX_BYTES);
		typeSpecificTester("B_", NO_TESTER, c, testSize, FieldType.Bytes, WARMUP_REPEAT, Necessity.optional, Operator.None, FASTProvideArrayBoxed.IDX_BYTES);
	}
	
	ValueDictionaryEntry vde = new ValueDictionaryEntry(null);
	
	private final void typeSpecificTester(String label, FASTAccept tester, Callable<FASTProvide> testData,
								int total, FieldType fieldType, 
								int iterations, Necessity presence, Operator operator, int id) {
				
		ValueDictionary dictionary = new ValueDictionary(total);
		
		int j = 0;
		GroupBuilder builder = new GroupBuilder(-1,dictionary);
		while (j<total) {
			builder.addField(Operator.None, fieldType, presence, id);
			j++;
		}
				
		//TODO: must start using new write group ASAP but must write it first.
		Field noPMapGroup = builder.buildSimpleGroup();

		
		long smallestWriteDuration = Long.MAX_VALUE;
		long smallestReadDuration = Long.MAX_VALUE;
		
		//System.err.println("total iterations "+total);
		
		int iter = iterations;
		while (--iter>=0) {
								
			int bufferSize = total*AVG_FIELD_SIZE;
			
			ByteArrayOutputStream baos = new ByteArrayOutputStream(bufferSize);

			FASTOutputStream outputStream = new FASTOutputStream(baos);
			PrimitiveWriter output = new PrimitiveWriter(outputStream);
			
			FASTProvide provider;
			try {
				provider = testData.call();
			} catch (Exception e) {
				e.printStackTrace();
				return;
			}
			
			
			
			long writeStartTime = System.nanoTime();
			noPMapGroup.writer(output, provider); //TODO: put back after we find problem
			long writeDuration = System.nanoTime()-writeStartTime;
			
			if (writeDuration < smallestWriteDuration) {
				smallestWriteDuration = writeDuration;
			}
			
			output.flush();
			//bytesWritten = output.totalWritten();
			
			
			FASTInputStream inputStream = new FASTInputStream(new ByteArrayInputStream(baos.toByteArray()));
			PrimitiveReader input = new PrimitiveReader(inputStream);

			long readStartTime = System.nanoTime();
			noPMapGroup.reader(input, tester);
			long readDuration = System.nanoTime()-readStartTime;
			
			if (readDuration < smallestReadDuration) {
				smallestReadDuration = readDuration;
			}
			
		}
		
		//only report best times if we have run at least 4 tests
		if (iterations>4) {
			System.err.println(label+fieldType.name()+'_'+presence+": "+ (smallestWriteDuration/total)+"ns per write, total time "+(smallestWriteDuration/1E+6)+"ms");
			System.err.println(label+fieldType.name()+'_'+presence+": "+(smallestReadDuration/total)+"ns per read, total time "+(smallestReadDuration/1E+6)+"ms");
		}
	}

	
}
