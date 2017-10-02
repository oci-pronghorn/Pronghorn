package com.ociweb.pronghorn.pipe;

public class TestDataSchema extends MessageSchema<TestDataSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400002,0xb8000000,0xc0200002,0xc0400003,0x80000000,0xb8000001,0xc0200003,0xc0400003,0x90000000,0xb8000002,0xc0200003,0xc0400002,0x80000001,0xc0200002,0xc0400002,0x90000001,0xc0200002,0xc0400003,0xb8000003,0x80000002,0xc0200003,0xc0400003,0xb8000004,0x90000002,0xc0200003},
		    (short)0,
		    new String[]{"ChunkedStream","ByteArray",null,"IntAndChunkedStream","IntValue","ByteArray",null,
		    "LongAndChunkedStream","LongValue","ByteArray",null,"Int","IntValue",null,"Long",
		    "LongValue",null,"ChunkedStreamAndInt","ByteArray","IntValue",null,"ChunkedStreamAndLong",
		    "ByteArray","LongValue",null},
		    new long[]{10, 12, 0, 20, 21, 22, 0, 30, 31, 32, 0, 40, 41, 0, 50, 51, 0, 60, 62, 61, 0, 70, 72, 71, 0},
		    new String[]{"global",null,null,"global",null,null,null,"global",null,null,null,"global",null,
		    null,"global",null,null,"global",null,null,null,"global",null,null,null},
		    "testDataSchema.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected TestDataSchema() { 
		    super(FROM);
		}

		public static final TestDataSchema instance = new TestDataSchema();

		public static final int MSG_CHUNKEDSTREAM_10 = 0x00000000; //Group/OpenTempl/2
		public static final int MSG_CHUNKEDSTREAM_10_FIELD_BYTEARRAY_12 = 0x01c00001; //ByteVector/None/0
		public static final int MSG_INTANDCHUNKEDSTREAM_20 = 0x00000003; //Group/OpenTempl/3
		public static final int MSG_INTANDCHUNKEDSTREAM_20_FIELD_INTVALUE_21 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_INTANDCHUNKEDSTREAM_20_FIELD_BYTEARRAY_22 = 0x01c00002; //ByteVector/None/1
		public static final int MSG_LONGANDCHUNKEDSTREAM_30 = 0x00000007; //Group/OpenTempl/3
		public static final int MSG_LONGANDCHUNKEDSTREAM_30_FIELD_LONGVALUE_31 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_LONGANDCHUNKEDSTREAM_30_FIELD_BYTEARRAY_32 = 0x01c00003; //ByteVector/None/2
		public static final int MSG_INT_40 = 0x0000000b; //Group/OpenTempl/2
		public static final int MSG_INT_40_FIELD_INTVALUE_41 = 0x00000001; //IntegerUnsigned/None/1
		public static final int MSG_LONG_50 = 0x0000000e; //Group/OpenTempl/2
		public static final int MSG_LONG_50_FIELD_LONGVALUE_51 = 0x00800001; //LongUnsigned/None/1
		public static final int MSG_CHUNKEDSTREAMANDINT_60 = 0x00000011; //Group/OpenTempl/3
		public static final int MSG_CHUNKEDSTREAMANDINT_60_FIELD_BYTEARRAY_62 = 0x01c00001; //ByteVector/None/3
		public static final int MSG_CHUNKEDSTREAMANDINT_60_FIELD_INTVALUE_61 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_CHUNKEDSTREAMANDLONG_70 = 0x00000015; //Group/OpenTempl/3
		public static final int MSG_CHUNKEDSTREAMANDLONG_70_FIELD_BYTEARRAY_72 = 0x01c00001; //ByteVector/None/4
		public static final int MSG_CHUNKEDSTREAMANDLONG_70_FIELD_LONGVALUE_71 = 0x00800003; //LongUnsigned/None/2


		public static void consume(Pipe<TestDataSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_CHUNKEDSTREAM_10:
		                consumeChunkedStream(input);
		            break;
		            case MSG_INTANDCHUNKEDSTREAM_20:
		                consumeIntAndChunkedStream(input);
		            break;
		            case MSG_LONGANDCHUNKEDSTREAM_30:
		                consumeLongAndChunkedStream(input);
		            break;
		            case MSG_INT_40:
		                consumeInt(input);
		            break;
		            case MSG_LONG_50:
		                consumeLong(input);
		            break;
		            case MSG_CHUNKEDSTREAMANDINT_60:
		                consumeChunkedStreamAndInt(input);
		            break;
		            case MSG_CHUNKEDSTREAMANDLONG_70:
		                consumeChunkedStreamAndLong(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeChunkedStream(Pipe<TestDataSchema> input) {
		    DataInputBlobReader<TestDataSchema> fieldByteArray = PipeReader.inputStream(input, MSG_CHUNKEDSTREAM_10_FIELD_BYTEARRAY_12);
		}
		public static void consumeIntAndChunkedStream(Pipe<TestDataSchema> input) {
		    int fieldIntValue = PipeReader.readInt(input,MSG_INTANDCHUNKEDSTREAM_20_FIELD_INTVALUE_21);
		    DataInputBlobReader<TestDataSchema> fieldByteArray = PipeReader.inputStream(input, MSG_INTANDCHUNKEDSTREAM_20_FIELD_BYTEARRAY_22);
		}
		public static void consumeLongAndChunkedStream(Pipe<TestDataSchema> input) {
		    long fieldLongValue = PipeReader.readLong(input,MSG_LONGANDCHUNKEDSTREAM_30_FIELD_LONGVALUE_31);
		    DataInputBlobReader<TestDataSchema> fieldByteArray = PipeReader.inputStream(input, MSG_LONGANDCHUNKEDSTREAM_30_FIELD_BYTEARRAY_32);
		}
		public static void consumeInt(Pipe<TestDataSchema> input) {
		    int fieldIntValue = PipeReader.readInt(input,MSG_INT_40_FIELD_INTVALUE_41);
		}
		public static void consumeLong(Pipe<TestDataSchema> input) {
		    long fieldLongValue = PipeReader.readLong(input,MSG_LONG_50_FIELD_LONGVALUE_51);
		}
		public static void consumeChunkedStreamAndInt(Pipe<TestDataSchema> input) {
		    DataInputBlobReader<TestDataSchema> fieldByteArray = PipeReader.inputStream(input, MSG_CHUNKEDSTREAMANDINT_60_FIELD_BYTEARRAY_62);
		    int fieldIntValue = PipeReader.readInt(input,MSG_CHUNKEDSTREAMANDINT_60_FIELD_INTVALUE_61);
		}
		public static void consumeChunkedStreamAndLong(Pipe<TestDataSchema> input) {
		    DataInputBlobReader<TestDataSchema> fieldByteArray = PipeReader.inputStream(input, MSG_CHUNKEDSTREAMANDLONG_70_FIELD_BYTEARRAY_72);
		    long fieldLongValue = PipeReader.readLong(input,MSG_CHUNKEDSTREAMANDLONG_70_FIELD_LONGVALUE_71);
		}

		public static void publishChunkedStream(Pipe<TestDataSchema> output, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_CHUNKEDSTREAM_10);
		        PipeWriter.writeBytes(output,MSG_CHUNKEDSTREAM_10_FIELD_BYTEARRAY_12, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishIntAndChunkedStream(Pipe<TestDataSchema> output, int fieldIntValue, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_INTANDCHUNKEDSTREAM_20);
		        PipeWriter.writeInt(output,MSG_INTANDCHUNKEDSTREAM_20_FIELD_INTVALUE_21, fieldIntValue);
		        PipeWriter.writeBytes(output,MSG_INTANDCHUNKEDSTREAM_20_FIELD_BYTEARRAY_22, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishLongAndChunkedStream(Pipe<TestDataSchema> output, long fieldLongValue, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_LONGANDCHUNKEDSTREAM_30);
		        PipeWriter.writeLong(output,MSG_LONGANDCHUNKEDSTREAM_30_FIELD_LONGVALUE_31, fieldLongValue);
		        PipeWriter.writeBytes(output,MSG_LONGANDCHUNKEDSTREAM_30_FIELD_BYTEARRAY_32, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishInt(Pipe<TestDataSchema> output, int fieldIntValue) {
		        PipeWriter.presumeWriteFragment(output, MSG_INT_40);
		        PipeWriter.writeInt(output,MSG_INT_40_FIELD_INTVALUE_41, fieldIntValue);
		        PipeWriter.publishWrites(output);
		}
		public static void publishLong(Pipe<TestDataSchema> output, long fieldLongValue) {
		        PipeWriter.presumeWriteFragment(output, MSG_LONG_50);
		        PipeWriter.writeLong(output,MSG_LONG_50_FIELD_LONGVALUE_51, fieldLongValue);
		        PipeWriter.publishWrites(output);
		}
		public static void publishChunkedStreamAndInt(Pipe<TestDataSchema> output, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength, int fieldIntValue) {
		        PipeWriter.presumeWriteFragment(output, MSG_CHUNKEDSTREAMANDINT_60);
		        PipeWriter.writeBytes(output,MSG_CHUNKEDSTREAMANDINT_60_FIELD_BYTEARRAY_62, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		        PipeWriter.writeInt(output,MSG_CHUNKEDSTREAMANDINT_60_FIELD_INTVALUE_61, fieldIntValue);
		        PipeWriter.publishWrites(output);
		}
		public static void publishChunkedStreamAndLong(Pipe<TestDataSchema> output, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength, long fieldLongValue) {
		        PipeWriter.presumeWriteFragment(output, MSG_CHUNKEDSTREAMANDLONG_70);
		        PipeWriter.writeBytes(output,MSG_CHUNKEDSTREAMANDLONG_70_FIELD_BYTEARRAY_72, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		        PipeWriter.writeLong(output,MSG_CHUNKEDSTREAMANDLONG_70_FIELD_LONGVALUE_71, fieldLongValue);
		        PipeWriter.publishWrites(output);
		}
		
}
