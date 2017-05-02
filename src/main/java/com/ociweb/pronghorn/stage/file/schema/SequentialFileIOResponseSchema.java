package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SequentialFileIOResponseSchema extends MessageSchema<SequentialFileIOResponseSchema> {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400002,0x88000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400003,0x88000000,0x80000001,0xc0200003,0xc0400002,0x88000000,0xc0200002},
            (short)0,
            new String[]{"FileWritten","reqId",null,"FileNotFound","reqId",null,"FileReadBegin","reqId",null,"FileReadData","reqId","fragmentCount",null,"FileReadEnd","reqId",null},
            new long[]{303, 1, 0, 103, 1, 0, 201, 1, 0, 202, 1, 2, 0, 203, 1, 0},
            new String[]{"global",null,null,"global",null,null,"global",null,null,"global",null,null,null,"global",null,null},
            "fileIOResponse.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    

    
    private SequentialFileIOResponseSchema() {
        super(FROM);
    }

    public static final SequentialFileIOResponseSchema instance = new SequentialFileIOResponseSchema();
    
    public static final int MSG_FILEWRITTEN_303 = 0x00000000;
    public static final int MSG_FILEWRITTEN_303_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILENOTFOUND_103 = 0x00000003;
    public static final int MSG_FILENOTFOUND_103_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILEREADBEGIN_201 = 0x00000006;
    public static final int MSG_FILEREADBEGIN_201_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILEREADDATA_202 = 0x00000009;
    public static final int MSG_FILEREADDATA_202_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILEREADDATA_202_FIELD_FRAGMENTCOUNT_2 = 0x00000002;
    public static final int MSG_FILEREADEND_203 = 0x0000000d;
    public static final int MSG_FILEREADEND_203_FIELD_REQID_1 = 0x00400001;


    public static void consume(Pipe<SequentialFileIOResponseSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_FILEWRITTEN_303:
                    consumeFileWritten(input);
                break;
                case MSG_FILENOTFOUND_103:
                    consumeFileNotFound(input);
                break;
                case MSG_FILEREADBEGIN_201:
                    consumeFileReadBegin(input);
                break;
                case MSG_FILEREADDATA_202:
                    consumeFileReadData(input);
                break;
                case MSG_FILEREADEND_203:
                    consumeFileReadEnd(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeFileWritten(Pipe<SequentialFileIOResponseSchema> input) {
        int fieldreqId = PipeReader.readInt(input,MSG_FILEWRITTEN_303_FIELD_REQID_1);
    }
    public static void consumeFileNotFound(Pipe<SequentialFileIOResponseSchema> input) {
        int fieldreqId = PipeReader.readInt(input,MSG_FILENOTFOUND_103_FIELD_REQID_1);
    }
    public static void consumeFileReadBegin(Pipe<SequentialFileIOResponseSchema> input) {
        int fieldreqId = PipeReader.readInt(input,MSG_FILEREADBEGIN_201_FIELD_REQID_1);
    }
    public static void consumeFileReadData(Pipe<SequentialFileIOResponseSchema> input) {
        int fieldreqId = PipeReader.readInt(input,MSG_FILEREADDATA_202_FIELD_REQID_1);
        int fieldfragmentCount = PipeReader.readInt(input,MSG_FILEREADDATA_202_FIELD_FRAGMENTCOUNT_2);
    }
    public static void consumeFileReadEnd(Pipe<SequentialFileIOResponseSchema> input) {
        int fieldreqId = PipeReader.readInt(input,MSG_FILEREADEND_203_FIELD_REQID_1);
    }

    public static boolean publishFileWritten(Pipe<SequentialFileIOResponseSchema> output, int fieldreqId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FILEWRITTEN_303)) {
            PipeWriter.writeInt(output,MSG_FILEWRITTEN_303_FIELD_REQID_1, fieldreqId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishFileNotFound(Pipe<SequentialFileIOResponseSchema> output, int fieldreqId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FILENOTFOUND_103)) {
            PipeWriter.writeInt(output,MSG_FILENOTFOUND_103_FIELD_REQID_1, fieldreqId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishFileReadBegin(Pipe<SequentialFileIOResponseSchema> output, int fieldreqId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FILEREADBEGIN_201)) {
            PipeWriter.writeInt(output,MSG_FILEREADBEGIN_201_FIELD_REQID_1, fieldreqId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishFileReadData(Pipe<SequentialFileIOResponseSchema> output, int fieldreqId, int fieldfragmentCount) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FILEREADDATA_202)) {
            PipeWriter.writeInt(output,MSG_FILEREADDATA_202_FIELD_REQID_1, fieldreqId);
            PipeWriter.writeInt(output,MSG_FILEREADDATA_202_FIELD_FRAGMENTCOUNT_2, fieldfragmentCount);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishFileReadEnd(Pipe<SequentialFileIOResponseSchema> output, int fieldreqId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FILEREADEND_203)) {
            PipeWriter.writeInt(output,MSG_FILEREADEND_203_FIELD_REQID_1, fieldreqId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


    
    
}
