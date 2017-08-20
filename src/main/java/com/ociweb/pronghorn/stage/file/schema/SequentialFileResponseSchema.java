package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SequentialFileResponseSchema extends MessageSchema<SequentialFileResponseSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400001,0xc0200001,0xc0400003,0x90000000,0x90000000,0xc0200003,0xc0400002,0x90000001,0xc0200002},
		    (short)0,
		    new String[]{"ClearAck",null,"MetaResponse","Size","Date",null,"WriteAck","Id",null},
		    new long[]{1, 0, 2, 11, 11, 0, 3, 12, 0},
		    new String[]{"global",null,"global",null,null,null,"global",null,null},
		    "SequentialFileResponse.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected SequentialFileResponseSchema() { 
		    super(FROM);
		}

		public static final SequentialFileResponseSchema instance = new SequentialFileResponseSchema();

		public static final int MSG_CLEARACK_1 = 0x00000000; //Group/OpenTempl/1
		public static final int MSG_METARESPONSE_2 = 0x00000002; //Group/OpenTempl/3
		public static final int MSG_METARESPONSE_2_FIELD_SIZE_11 = 0x00800001; //LongUnsigned/None/0
		public static final int MSG_METARESPONSE_2_FIELD_DATE_11 = 0x00800003; //LongUnsigned/None/0
		public static final int MSG_WRITEACK_3 = 0x00000006; //Group/OpenTempl/2
		public static final int MSG_WRITEACK_3_FIELD_ID_12 = 0x00800001; //LongUnsigned/None/1


		public static void consume(Pipe<SequentialFileResponseSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_CLEARACK_1:
		                consumeClearAck(input);
		            break;
		            case MSG_METARESPONSE_2:
		                consumeMetaResponse(input);
		            break;
		            case MSG_WRITEACK_3:
		                consumeWriteAck(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeClearAck(Pipe<SequentialFileResponseSchema> input) {
		}
		public static void consumeMetaResponse(Pipe<SequentialFileResponseSchema> input) {
		    long fieldSize = PipeReader.readLong(input,MSG_METARESPONSE_2_FIELD_SIZE_11);
		    long fieldDate = PipeReader.readLong(input,MSG_METARESPONSE_2_FIELD_DATE_11);
		}
		public static void consumeWriteAck(Pipe<SequentialFileResponseSchema> input) {
		    long fieldId = PipeReader.readLong(input,MSG_WRITEACK_3_FIELD_ID_12);
		}

		public static void publishClearAck(Pipe<SequentialFileResponseSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_CLEARACK_1);
		        PipeWriter.publishWrites(output);
		}
		public static void publishMetaResponse(Pipe<SequentialFileResponseSchema> output, long fieldSize, long fieldDate) {
		        PipeWriter.presumeWriteFragment(output, MSG_METARESPONSE_2);
		        PipeWriter.writeLong(output,MSG_METARESPONSE_2_FIELD_SIZE_11, fieldSize);
		        PipeWriter.writeLong(output,MSG_METARESPONSE_2_FIELD_DATE_11, fieldDate);
		        PipeWriter.publishWrites(output);
		}
		public static void publishWriteAck(Pipe<SequentialFileResponseSchema> output, long fieldId) {
		        PipeWriter.presumeWriteFragment(output, MSG_WRITEACK_3);
		        PipeWriter.writeLong(output,MSG_WRITEACK_3_FIELD_ID_12, fieldId);
		        PipeWriter.publishWrites(output);
		}

}
