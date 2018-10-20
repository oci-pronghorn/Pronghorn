package com.ociweb.pronghorn.stage.blocking;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

@Deprecated
public class BlockingWorkInProgressSchema extends MessageSchema<BlockingWorkInProgressSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400001,0xc0200001},
		    (short)0,
		    new String[]{"inFlight",null},
		    new long[]{1, 0},
		    new String[]{"global",null},
		    "BlockingWorkInProgress.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public BlockingWorkInProgressSchema() { 
		    super(FROM);
		}

		protected BlockingWorkInProgressSchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final BlockingWorkInProgressSchema instance = new BlockingWorkInProgressSchema();

		public static final int MSG_INFLIGHT_1 = 0x00000000; //Group/OpenTempl/1

		public static void consume(Pipe<BlockingWorkInProgressSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_INFLIGHT_1:
		                consumeinFlight(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeinFlight(Pipe<BlockingWorkInProgressSchema> input) {
		}

		public static void publishinFlight(Pipe<BlockingWorkInProgressSchema> output) {
		        PipeWriter.presumeWriteFragment(output, MSG_INFLIGHT_1);
		        PipeWriter.publishWrites(output);
		}

}
