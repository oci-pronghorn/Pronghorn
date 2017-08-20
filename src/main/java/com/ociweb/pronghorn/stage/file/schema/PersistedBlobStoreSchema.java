package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PersistedBlobStoreSchema extends MessageSchema<PersistedBlobStoreSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0xb8000000,0xc0200003,0xc0400002,0x90000000,0xc0200002,0xc0400001,0xc0200001,0xc0400001,0xc0200001},
		    (short)0,
		    new String[]{"Block","BlockId","ByteArray",null,"Release","BlockId",null,"RequestReplay",null,
		    "Clear",null},
		    new long[]{1, 3, 2, 0, 7, 3, 0, 6, 0, 12, 0},
		    new String[]{"global",null,null,null,"global",null,null,"global",null,"global",null},
		    "PersistedBlobStore.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected PersistedBlobStoreSchema() { 
		    super(FROM);
		}

		public static final PersistedBlobStoreSchema instance = new PersistedBlobStoreSchema();
		
		public static final int MSG_BLOCK_1 = 0x00000000; //Group/OpenTempl/3
		public static final int MSG_BLOCK_1_FIELD_BLOCKID_3 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_BLOCK_1_FIELD_BYTEARRAY_2 = 0x01c00003; //ByteVector/None/0
		public static final int MSG_RELEASE_7 = 0x00000004; //Group/OpenTempl/2
		public static final int MSG_RELEASE_7_FIELD_BLOCKID_3 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_REQUESTREPLAY_6 = 0x00000007; //Group/OpenTempl/1
		public static final int MSG_CLEAR_12 = 0x00000009; //Group/OpenTempl/1


		public static void consume(Pipe<PersistedBlobStoreSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_BLOCK_1:
		                consumeBlock(input);
		            break;
		            case MSG_RELEASE_7:
		                consumeRelease(input);
		            break;
		            case MSG_REQUESTREPLAY_6:
		                consumeRequestReplay(input);
		            break;
		            case MSG_CLEAR_12:
		                consumeClear(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeBlock(Pipe<PersistedBlobStoreSchema> input) {
		    long fieldBlockId = PipeReader.readLong(input,MSG_BLOCK_1_FIELD_BLOCKID_3);
		    DataInputBlobReader<PersistedBlobStoreSchema> fieldByteArray = PipeReader.inputStream(input, MSG_BLOCK_1_FIELD_BYTEARRAY_2);
		}
		public static void consumeRelease(Pipe<PersistedBlobStoreSchema> input) {
		    long fieldBlockId = PipeReader.readLong(input,MSG_RELEASE_7_FIELD_BLOCKID_3);
		}
		public static void consumeRequestReplay(Pipe<PersistedBlobStoreSchema> input) {
		}
		public static void consumeClear(Pipe<PersistedBlobStoreSchema> input) {
		}

		public static void publishBlock(Pipe<PersistedBlobStoreSchema> output, long fieldBlockId, byte[] fieldByteArrayBacking, int fieldByteArrayPosition, int fieldByteArrayLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_BLOCK_1);
		        PipeWriter.writeLong(output,MSG_BLOCK_1_FIELD_BLOCKID_3, fieldBlockId);
		        PipeWriter.writeBytes(output,MSG_BLOCK_1_FIELD_BYTEARRAY_2, fieldByteArrayBacking, fieldByteArrayPosition, fieldByteArrayLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishRelease(Pipe<PersistedBlobStoreSchema> output, long fieldBlockId) {
		        PipeWriter.presumeWriteFragment(output, MSG_RELEASE_7);
		        PipeWriter.writeLong(output,MSG_RELEASE_7_FIELD_BLOCKID_3, fieldBlockId);
		        PipeWriter.publishWrites(output);
		}
		public static void publishRequestReplay(Pipe<PersistedBlobStoreSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_REQUESTREPLAY_6);
		        PipeWriter.publishWrites(output);
		}
		public static void publishClear(Pipe<PersistedBlobStoreSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_CLEAR_12);
		        PipeWriter.publishWrites(output);
		}
		
}
