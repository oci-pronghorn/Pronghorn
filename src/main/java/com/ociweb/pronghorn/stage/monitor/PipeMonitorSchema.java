package com.ociweb.pronghorn.stage.monitor;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PipeMonitorSchema extends MessageSchema<PipeMonitorSchema>{
	
	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc1400007,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0x90800003,0xc1200007},
		    (short)0,
		    new String[]{"RingStatSample","MS","Head","Tail","TemplateId","BufferSize","Consumed",null},
		    new long[]{100, 1, 2, 3, 4, 5, 6, 0},
		    new String[]{"global",null,null,null,null,null,null,null},
		    "PipeMonitor.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


    public static PipeMonitorSchema instance = new PipeMonitorSchema();

    
    private PipeMonitorSchema() {
        super(FROM);
    }
    
    public static final int MSG_RINGSTATSAMPLE_100 = 0x00000000; //Group/OpenTemplPMap/7
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_MS_1 = 0x00800001; //LongUnsigned/Delta/0
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2 = 0x00800003; //LongUnsigned/Delta/1
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3 = 0x00800005; //LongUnsigned/Delta/2
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4 = 0x00000007; //IntegerUnsigned/None/0
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5 = 0x00000008; //IntegerUnsigned/Copy/1
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_CONSUMED_6 = 0x00800009; //LongUnsigned/Delta/3


    public static void consume(Pipe<PipeMonitorSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RINGSTATSAMPLE_100:
                    consumeRingStatSample(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeRingStatSample(Pipe<PipeMonitorSchema> input) {
        long fieldMS = PipeReader.readLong(input,MSG_RINGSTATSAMPLE_100_FIELD_MS_1);
        long fieldHead = PipeReader.readLong(input,MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2);
        long fieldTail = PipeReader.readLong(input,MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3);
        int fieldTemplateId = PipeReader.readInt(input,MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4);
        int fieldBufferSize = PipeReader.readInt(input,MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5);
        long fieldConsumed = PipeReader.readLong(input,MSG_RINGSTATSAMPLE_100_FIELD_CONSUMED_6);
    }

    public static void publishRingStatSample(Pipe<PipeMonitorSchema> output, long fieldMS, long fieldHead, long fieldTail, int fieldTemplateId, int fieldBufferSize, long fieldConsumed) {
            PipeWriter.presumeWriteFragment(output, MSG_RINGSTATSAMPLE_100);
            PipeWriter.writeLong(output,MSG_RINGSTATSAMPLE_100_FIELD_MS_1, fieldMS);
            PipeWriter.writeLong(output,MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2, fieldHead);
            PipeWriter.writeLong(output,MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3, fieldTail);
            PipeWriter.writeInt(output,MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4, fieldTemplateId);
            PipeWriter.writeInt(output,MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5, fieldBufferSize);
            PipeWriter.writeLong(output,MSG_RINGSTATSAMPLE_100_FIELD_CONSUMED_6, fieldConsumed);
            PipeWriter.publishWrites(output);
    }
}
