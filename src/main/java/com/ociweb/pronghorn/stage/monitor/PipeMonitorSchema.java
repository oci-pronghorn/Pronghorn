package com.ociweb.pronghorn.stage.monitor;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class PipeMonitorSchema extends MessageSchema<PipeMonitorSchema>{
	
    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc1400006,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0xc1200006,0xc1400008,0x90800000,0x90800001,0x90800002,0x80000000,0x80200001,0x80800002,0x80200003,0xc1200008},
            (short)0,
            new String[]{"RingStatSample","MS","Head","Tail","TemplateId","BufferSize",null,"RingStatEnhancedSample","MS","Head","Tail","TemplateId","BufferSize","StackDepth","Latency",null},
            new long[]{100, 1, 2, 3, 4, 5, 0, 200, 1, 2, 3, 4, 5, 21, 22, 0},
            new String[]{"global",null,null,null,null,null,null,"global",null,null,null,null,null,null,null,null},
            "ringMonitor.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    


    public static PipeMonitorSchema instance = new PipeMonitorSchema();

    
    private PipeMonitorSchema() {
        super(FROM);
    }
    
    public static final int MSG_RINGSTATSAMPLE_100 = 0x00000000;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_MS_1 = 0x00800001;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2 = 0x00800003;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3 = 0x00800005;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4 = 0x00000007;
    public static final int MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5 = 0x00000008;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200 = 0x00000007;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_MS_1 = 0x00800001;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_HEAD_2 = 0x00800003;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TAIL_3 = 0x00800005;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TEMPLATEID_4 = 0x00000007;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_BUFFERSIZE_5 = 0x00000008;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_STACKDEPTH_21 = 0x00000009;
    public static final int MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_LATENCY_22 = 0x0000000a;


    public static void consume(Pipe<PipeMonitorSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RINGSTATSAMPLE_100:
                    consumeRingStatSample(input);
                break;
                case MSG_RINGSTATENHANCEDSAMPLE_200:
                    consumeRingStatEnhancedSample(input);
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
    }
    public static void consumeRingStatEnhancedSample(Pipe<PipeMonitorSchema> input) {
        long fieldMS = PipeReader.readLong(input,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_MS_1);
        long fieldHead = PipeReader.readLong(input,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_HEAD_2);
        long fieldTail = PipeReader.readLong(input,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TAIL_3);
        int fieldTemplateId = PipeReader.readInt(input,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TEMPLATEID_4);
        int fieldBufferSize = PipeReader.readInt(input,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_BUFFERSIZE_5);
        int fieldStackDepth = PipeReader.readInt(input,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_STACKDEPTH_21);
        int fieldLatency = PipeReader.readInt(input,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_LATENCY_22);
    }

    public static boolean publishRingStatSample(Pipe<PipeMonitorSchema> output, long fieldMS, long fieldHead, long fieldTail, int fieldTemplateId, int fieldBufferSize) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_RINGSTATSAMPLE_100)) {
            PipeWriter.writeLong(output,MSG_RINGSTATSAMPLE_100_FIELD_MS_1, fieldMS);
            PipeWriter.writeLong(output,MSG_RINGSTATSAMPLE_100_FIELD_HEAD_2, fieldHead);
            PipeWriter.writeLong(output,MSG_RINGSTATSAMPLE_100_FIELD_TAIL_3, fieldTail);
            PipeWriter.writeInt(output,MSG_RINGSTATSAMPLE_100_FIELD_TEMPLATEID_4, fieldTemplateId);
            PipeWriter.writeInt(output,MSG_RINGSTATSAMPLE_100_FIELD_BUFFERSIZE_5, fieldBufferSize);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishRingStatEnhancedSample(Pipe<PipeMonitorSchema> output, long fieldMS, long fieldHead, long fieldTail, int fieldTemplateId, int fieldBufferSize, int fieldStackDepth, int fieldLatency) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_RINGSTATENHANCEDSAMPLE_200)) {
            PipeWriter.writeLong(output,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_MS_1, fieldMS);
            PipeWriter.writeLong(output,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_HEAD_2, fieldHead);
            PipeWriter.writeLong(output,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TAIL_3, fieldTail);
            PipeWriter.writeInt(output,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_TEMPLATEID_4, fieldTemplateId);
            PipeWriter.writeInt(output,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_BUFFERSIZE_5, fieldBufferSize);
            PipeWriter.writeInt(output,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_STACKDEPTH_21, fieldStackDepth);
            PipeWriter.writeInt(output,MSG_RINGSTATENHANCEDSAMPLE_200_FIELD_LATENCY_22, fieldLatency);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }




}
