package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class MQTTIdRangeSchema extends MessageSchema<MQTTIdRangeSchema> {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400002,0x80000000,0xc0200002},
            (short)0,
            new String[]{"IdRange","Range",null},
            new long[]{1, 100, 0},
            new String[]{"global",null,null},
            "MQTTIdRanges.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});


    protected MQTTIdRangeSchema() {
        super(FROM);
    }
        
    
    public static final MQTTIdRangeSchema instance = new MQTTIdRangeSchema();
    
    public static final int MSG_IDRANGE_1 = 0x00000000;
    public static final int MSG_IDRANGE_1_FIELD_RANGE_100 = 0x00000001;


    public static void consume(Pipe<MQTTIdRangeSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_IDRANGE_1:
                    consumeIdRange(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeIdRange(Pipe<MQTTIdRangeSchema> input) {
        int fieldRange = PipeReader.readInt(input,MSG_IDRANGE_1_FIELD_RANGE_100);
    }

    public static boolean publishIdRange(Pipe<MQTTIdRangeSchema> output, int fieldRange) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_IDRANGE_1)) {
            PipeWriter.writeInt(output,MSG_IDRANGE_1_FIELD_RANGE_100, fieldRange);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


}
