package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class BlockStorageXmitSchema extends MessageSchema<BlockStorageXmitSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400003,0x90000000,0x80000000,0xc0200003},
		    (short)0,
		    new String[]{"Write","Position","Payload",null,"Read","Position","ReadLength",null},
		    new long[]{1, 12, 11, 0, 2, 12, 10, 0},
		    new String[]{"global",null,null,null,"global",null,null,null},
		    "BlockStorageXmit.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected BlockStorageXmitSchema() { 
		    super(FROM);
		}

		public static final BlockStorageXmitSchema instance = new BlockStorageXmitSchema();

		public static final int MSG_WRITE_1 = 0x00000000; //Group/OpenTempl/3
		public static final int MSG_WRITE_1_FIELD_POSITION_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_WRITE_1_FIELD_PAYLOAD_11 = 0x01c00003; //ByteVector/None/0
		public static final int MSG_READ_2 = 0x00000004; //Group/OpenTempl/3
		public static final int MSG_READ_2_FIELD_POSITION_12 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_READ_2_FIELD_READLENGTH_10 = 0x00000003; //IntegerUnsigned/None/0


		public static void consume(Pipe<BlockStorageXmitSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_WRITE_1:
		                consumeWrite(input);
		            break;
		            case MSG_READ_2:
		                consumeRead(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeWrite(Pipe<BlockStorageXmitSchema> input) {
		    long fieldPosition = PipeReader.readLong(input,MSG_WRITE_1_FIELD_POSITION_12);
		    DataInputBlobReader<BlockStorageXmitSchema> fieldPayload = PipeReader.inputStream(input, MSG_WRITE_1_FIELD_PAYLOAD_11);
		}
		public static void consumeRead(Pipe<BlockStorageXmitSchema> input) {
		    long fieldPosition = PipeReader.readLong(input,MSG_READ_2_FIELD_POSITION_12);
		    int fieldReadLength = PipeReader.readInt(input,MSG_READ_2_FIELD_READLENGTH_10);
		}

		public static void publishWrite(Pipe<BlockStorageXmitSchema> output, long fieldPosition, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_WRITE_1);
		        PipeWriter.writeLong(output,MSG_WRITE_1_FIELD_POSITION_12, fieldPosition);
		        PipeWriter.writeBytes(output,MSG_WRITE_1_FIELD_PAYLOAD_11, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishRead(Pipe<BlockStorageXmitSchema> output, long fieldPosition, int fieldReadLength) {
			    assert(fieldReadLength>0) : "must have some length";
		        PipeWriter.presumeWriteFragment(output, MSG_READ_2);
		        PipeWriter.writeLong(output,MSG_READ_2_FIELD_POSITION_12, fieldPosition);
		        PipeWriter.writeInt(output,MSG_READ_2_FIELD_READLENGTH_10, fieldReadLength);
		        PipeWriter.publishWrites(output);
		}

}
