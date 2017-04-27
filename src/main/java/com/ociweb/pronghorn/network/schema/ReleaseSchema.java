package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ReleaseSchema extends MessageSchema<ReleaseSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x90000000,0x90000001,0xc0200003,0xc0400004,0x90000000,0x90000001,0x88000000,0xc0200004},
		    (short)0,
		    new String[]{"Release","ConnectionID","Position",null,"ReleaseWithSeq","ConnectionID","Position","SequenceNo",null},
		    new long[]{100, 1, 2, 0, 101, 1, 2, 3, 0},
		    new String[]{"global",null,null,null,"global",null,null,null,null},
		    "Release.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


    protected ReleaseSchema() {
        super(FROM);
    }
    
    public static final ReleaseSchema instance = new ReleaseSchema();
    
    public static final int MSG_RELEASE_100 = 0x00000000;
    public static final int MSG_RELEASE_100_FIELD_CONNECTIONID_1 = 0x00800001;
    public static final int MSG_RELEASE_100_FIELD_POSITION_2 = 0x00800003;
    public static final int MSG_RELEASEWITHSEQ_101 = 0x00000004;
    public static final int MSG_RELEASEWITHSEQ_101_FIELD_CONNECTIONID_1 = 0x00800001;
    public static final int MSG_RELEASEWITHSEQ_101_FIELD_POSITION_2 = 0x00800003;
    public static final int MSG_RELEASEWITHSEQ_101_FIELD_SEQUENCENO_3 = 0x00400005;


    public static void consume(Pipe<ReleaseSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RELEASE_100:
                    consumeRelease(input);
                break;
                case MSG_RELEASEWITHSEQ_101:
                    consumeReleaseWithSeq(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeRelease(Pipe<ReleaseSchema> input) {
        long fieldConnectionID = PipeReader.readLong(input,MSG_RELEASE_100_FIELD_CONNECTIONID_1);
        long fieldPosition = PipeReader.readLong(input,MSG_RELEASE_100_FIELD_POSITION_2);
    }
    public static void consumeReleaseWithSeq(Pipe<ReleaseSchema> input) {
        long fieldConnectionID = PipeReader.readLong(input,MSG_RELEASEWITHSEQ_101_FIELD_CONNECTIONID_1);
        long fieldPosition = PipeReader.readLong(input,MSG_RELEASEWITHSEQ_101_FIELD_POSITION_2);
        int fieldSequenceNo = PipeReader.readInt(input,MSG_RELEASEWITHSEQ_101_FIELD_SEQUENCENO_3);
    }

    public static boolean publishRelease(Pipe<ReleaseSchema> output, long fieldConnectionID, long fieldPosition) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_RELEASE_100)) {
            PipeWriter.writeLong(output,MSG_RELEASE_100_FIELD_CONNECTIONID_1, fieldConnectionID);
            PipeWriter.writeLong(output,MSG_RELEASE_100_FIELD_POSITION_2, fieldPosition);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishReleaseWithSeq(Pipe<ReleaseSchema> output, long fieldConnectionID, long fieldPosition, int fieldSequenceNo) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_RELEASEWITHSEQ_101)) {
            PipeWriter.writeLong(output,MSG_RELEASEWITHSEQ_101_FIELD_CONNECTIONID_1, fieldConnectionID);
            PipeWriter.writeLong(output,MSG_RELEASEWITHSEQ_101_FIELD_POSITION_2, fieldPosition);
            PipeWriter.writeInt(output,MSG_RELEASEWITHSEQ_101_FIELD_SEQUENCENO_3, fieldSequenceNo);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }

        
}
