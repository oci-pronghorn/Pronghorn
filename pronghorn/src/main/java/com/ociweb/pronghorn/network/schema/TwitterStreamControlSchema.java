package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class TwitterStreamControlSchema extends MessageSchema<TwitterStreamControlSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400001,0xc0200001,0xc0400002,0x90000000,0xc0200002},
		    (short)0,
		    new String[]{"Reconnect",null,"FinishedBlock","MaxPostId",null},
		    new long[]{100, 0, 101, 31, 0},
		    new String[]{"global",null,"global",null,null},
		    "TwitterUserStreamControl.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
	    

    private TwitterStreamControlSchema() {
        super(FROM);
    }
    
    
    public static final TwitterStreamControlSchema instance = new TwitterStreamControlSchema();
    
    public static final int MSG_RECONNECT_100 = 0x00000000; //Group/OpenTempl/1
    public static final int MSG_FINISHEDBLOCK_101 = 0x00000002; //Group/OpenTempl/2
    public static final int MSG_FINISHEDBLOCK_101_FIELD_MAXPOSTID_31 = 0x00800001; //LongUnsigned/None/0


    public static void consume(Pipe<TwitterStreamControlSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RECONNECT_100:
                    consumeReconnect(input);
                break;
                case MSG_FINISHEDBLOCK_101:
                    consumeFinishedBlock(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeReconnect(Pipe<TwitterStreamControlSchema> input) {
    }
    public static void consumeFinishedBlock(Pipe<TwitterStreamControlSchema> input) {
        long fieldMaxPostId = PipeReader.readLong(input,MSG_FINISHEDBLOCK_101_FIELD_MAXPOSTID_31);
    }

    public static void publishReconnect(Pipe<TwitterStreamControlSchema> output) {
            PipeWriter.presumeWriteFragment(output, MSG_RECONNECT_100);
            PipeWriter.publishWrites(output);
    }
    public static void publishFinishedBlock(Pipe<TwitterStreamControlSchema> output, long fieldMaxPostId) {
            PipeWriter.presumeWriteFragment(output, MSG_FINISHEDBLOCK_101);
            PipeWriter.writeLong(output,MSG_FINISHEDBLOCK_101_FIELD_MAXPOSTID_31, fieldMaxPostId);
            PipeWriter.publishWrites(output);
    }

}
