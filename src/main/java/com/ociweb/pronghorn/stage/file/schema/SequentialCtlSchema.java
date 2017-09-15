package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SequentialCtlSchema extends MessageSchema<SequentialCtlSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400002,0x90000000,0xc0200002},
		    (short)0,
		    new String[]{"Replay",null,"Clear",null,"MetaRequest",null,"IdToSave","Id",null},
		    new long[]{1, 0, 2, 0, 3, 0, 4, 10, 0},
		    new String[]{"global",null,"global",null,"global",null,"global",null,null},
		    "SequentialCtl.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected SequentialCtlSchema() { 
		    super(FROM);
		}

		public static final SequentialCtlSchema instance = new SequentialCtlSchema();

		public static final int MSG_REPLAY_1 = 0x00000000; //Group/OpenTempl/1
		public static final int MSG_CLEAR_2 = 0x00000002; //Group/OpenTempl/1
		public static final int MSG_METAREQUEST_3 = 0x00000004; //Group/OpenTempl/1
		public static final int MSG_IDTOSAVE_4 = 0x00000006; //Group/OpenTempl/2
		public static final int MSG_IDTOSAVE_4_FIELD_ID_10 = 0x00800001; //LongUnsigned/None/0


		public static void consume(Pipe<SequentialCtlSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_REPLAY_1:
		                consumeReplay(input);
		            break;
		            case MSG_CLEAR_2:
		                consumeClear(input);
		            break;
		            case MSG_METAREQUEST_3:
		                consumeMetaRequest(input);
		            break;
		            case MSG_IDTOSAVE_4:
		                consumeIdToSave(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeReplay(Pipe<SequentialCtlSchema> input) {
		}
		public static void consumeClear(Pipe<SequentialCtlSchema> input) {
		}
		public static void consumeMetaRequest(Pipe<SequentialCtlSchema> input) {
		}
		public static void consumeIdToSave(Pipe<SequentialCtlSchema> input) {
		    long fieldId = PipeReader.readLong(input,MSG_IDTOSAVE_4_FIELD_ID_10);
		}

		public static void publishReplay(Pipe<SequentialCtlSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_REPLAY_1);
		        PipeWriter.publishWrites(output);
		}
		public static void publishClear(Pipe<SequentialCtlSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_CLEAR_2);
		        PipeWriter.publishWrites(output);
		}
		public static void publishMetaRequest(Pipe<SequentialCtlSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_METAREQUEST_3);
		        PipeWriter.publishWrites(output);
		}
		public static void publishIdToSave(Pipe<SequentialCtlSchema> output, long fieldId) {
		        PipeWriter.presumeWriteFragment(output, MSG_IDTOSAVE_4);
		        PipeWriter.writeLong(output,MSG_IDTOSAVE_4_FIELD_ID_10, fieldId);
		        PipeWriter.publishWrites(output);
		}
}
