package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class FolderWatchSchema extends MessageSchema<FolderWatchSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400004,0xa8000000,0xa8000001,0xb8000002,0xc0200004,0xc0400004,0xa8000000,0xa8000001,0xb8000002,0xc0200004,0xc0400003,0xa8000000,0xa8000001,0xc0200003},
		    (short)0,
		    new String[]{"NewFile","Path","File","Payload",null,"UpdatedFile","Path","File","Payload",null,
		    "DeletedFile","Path","File",null},
		    new long[]{1, 10, 11, 12, 0, 2, 10, 11, 12, 0, 3, 10, 11, 0},
		    new String[]{"global",null,null,null,null,"global",null,null,null,null,"global",null,null,null},
		    "FolderWatch.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public FolderWatchSchema() { 
		    super(FROM);
		}

		protected FolderWatchSchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final FolderWatchSchema instance = new FolderWatchSchema();

		public static final int MSG_NEWFILE_1 = 0x00000000; //Group/OpenTempl/4
		public static final int MSG_NEWFILE_1_FIELD_PATH_10 = 0x01400001; //UTF8/None/0
		public static final int MSG_NEWFILE_1_FIELD_FILE_11 = 0x01400003; //UTF8/None/1
		public static final int MSG_NEWFILE_1_FIELD_PAYLOAD_12 = 0x01c00005; //ByteVector/None/2
		public static final int MSG_UPDATEDFILE_2 = 0x00000005; //Group/OpenTempl/4
		public static final int MSG_UPDATEDFILE_2_FIELD_PATH_10 = 0x01400001; //UTF8/None/0
		public static final int MSG_UPDATEDFILE_2_FIELD_FILE_11 = 0x01400003; //UTF8/None/1
		public static final int MSG_UPDATEDFILE_2_FIELD_PAYLOAD_12 = 0x01c00005; //ByteVector/None/2
		public static final int MSG_DELETEDFILE_3 = 0x0000000a; //Group/OpenTempl/3
		public static final int MSG_DELETEDFILE_3_FIELD_PATH_10 = 0x01400001; //UTF8/None/0
		public static final int MSG_DELETEDFILE_3_FIELD_FILE_11 = 0x01400003; //UTF8/None/1

		public static void consume(Pipe<FolderWatchSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_NEWFILE_1:
		                consumeNewFile(input);
		            break;
		            case MSG_UPDATEDFILE_2:
		                consumeUpdatedFile(input);
		            break;
		            case MSG_DELETEDFILE_3:
		                consumeDeletedFile(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeNewFile(Pipe<FolderWatchSchema> input) {
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_NEWFILE_1_FIELD_PATH_10,new StringBuilder(PipeReader.readBytesLength(input,MSG_NEWFILE_1_FIELD_PATH_10)));
		    StringBuilder fieldFile = PipeReader.readUTF8(input,MSG_NEWFILE_1_FIELD_FILE_11,new StringBuilder(PipeReader.readBytesLength(input,MSG_NEWFILE_1_FIELD_FILE_11)));
		    DataInputBlobReader<FolderWatchSchema> fieldPayload = PipeReader.inputStream(input, MSG_NEWFILE_1_FIELD_PAYLOAD_12);
		}
		public static void consumeUpdatedFile(Pipe<FolderWatchSchema> input) {
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_UPDATEDFILE_2_FIELD_PATH_10,new StringBuilder(PipeReader.readBytesLength(input,MSG_UPDATEDFILE_2_FIELD_PATH_10)));
		    StringBuilder fieldFile = PipeReader.readUTF8(input,MSG_UPDATEDFILE_2_FIELD_FILE_11,new StringBuilder(PipeReader.readBytesLength(input,MSG_UPDATEDFILE_2_FIELD_FILE_11)));
		    DataInputBlobReader<FolderWatchSchema> fieldPayload = PipeReader.inputStream(input, MSG_UPDATEDFILE_2_FIELD_PAYLOAD_12);
		}
		public static void consumeDeletedFile(Pipe<FolderWatchSchema> input) {
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_DELETEDFILE_3_FIELD_PATH_10,new StringBuilder(PipeReader.readBytesLength(input,MSG_DELETEDFILE_3_FIELD_PATH_10)));
		    StringBuilder fieldFile = PipeReader.readUTF8(input,MSG_DELETEDFILE_3_FIELD_FILE_11,new StringBuilder(PipeReader.readBytesLength(input,MSG_DELETEDFILE_3_FIELD_FILE_11)));
		}

		public static void publishNewFile(Pipe<FolderWatchSchema> output, CharSequence fieldPath, CharSequence fieldFile, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_NEWFILE_1);
		        PipeWriter.writeUTF8(output,MSG_NEWFILE_1_FIELD_PATH_10, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_NEWFILE_1_FIELD_FILE_11, fieldFile);
		        PipeWriter.writeBytes(output,MSG_NEWFILE_1_FIELD_PAYLOAD_12, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishUpdatedFile(Pipe<FolderWatchSchema> output, CharSequence fieldPath, CharSequence fieldFile, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_UPDATEDFILE_2);
		        PipeWriter.writeUTF8(output,MSG_UPDATEDFILE_2_FIELD_PATH_10, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_UPDATEDFILE_2_FIELD_FILE_11, fieldFile);
		        PipeWriter.writeBytes(output,MSG_UPDATEDFILE_2_FIELD_PAYLOAD_12, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishDeletedFile(Pipe<FolderWatchSchema> output, CharSequence fieldPath, CharSequence fieldFile) {
		        PipeWriter.presumeWriteFragment(output, MSG_DELETEDFILE_3);
		        PipeWriter.writeUTF8(output,MSG_DELETEDFILE_3_FIELD_PATH_10, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_DELETEDFILE_3_FIELD_FILE_11, fieldFile);
		        PipeWriter.publishWrites(output);
		}
}
