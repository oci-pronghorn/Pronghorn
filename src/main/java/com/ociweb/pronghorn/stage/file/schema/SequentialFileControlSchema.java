package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SequentialFileControlSchema extends MessageSchema<SequentialFileControlSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400001,0xc0200001,0xc0400001,0xc0200001,0xc0400001,0xc0200001},
		    (short)0,
		    new String[]{"Replay",null,"Clear",null,"MetaRequest",null},
		    new long[]{1, 0, 2, 0, 3, 0},
		    new String[]{"global",null,"global",null,"global",null},
		    "SequentialFileControl.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected SequentialFileControlSchema() { 
		    super(FROM);
		}

		public static final SequentialFileControlSchema instance = new SequentialFileControlSchema();

		public static final int MSG_REPLAY_1 = 0x00000000; //Group/OpenTempl/1
		public static final int MSG_CLEAR_2 = 0x00000002; //Group/OpenTempl/1
		public static final int MSG_METAREQUEST_3 = 0x00000004; //Group/OpenTempl/1


		public static void consume(Pipe<SequentialFileControlSchema> input) {
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
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeReplay(Pipe<SequentialFileControlSchema> input) {
		}
		public static void consumeClear(Pipe<SequentialFileControlSchema> input) {
		}
		public static void consumeMetaRequest(Pipe<SequentialFileControlSchema> input) {
		}

		public static void publishReplay(Pipe<SequentialFileControlSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_REPLAY_1);
		        PipeWriter.publishWrites(output);
		}
		public static void publishClear(Pipe<SequentialFileControlSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_CLEAR_2);
		        PipeWriter.publishWrites(output);
		}
		public static void publishMetaRequest(Pipe<SequentialFileControlSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_METAREQUEST_3);
		        PipeWriter.publishWrites(output);
		}

		
}
