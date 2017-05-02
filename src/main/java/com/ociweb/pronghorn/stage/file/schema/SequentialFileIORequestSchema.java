package com.ociweb.pronghorn.stage.file.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class SequentialFileIORequestSchema extends MessageSchema<SequentialFileIORequestSchema> {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400003,0xa0000000,0xa0000001,0xc0200003,0xc0400003,0xa0000000,0x88000000,0xc0200003,0xc0400003,0xa0000000,0x88000000,0xc0200003,0xc0400003,0x88000000,0x80000001,0xc0200003,0xc0400002,0x88000000,0xc0200002},
            (short)0,
            new String[]{"RenameFile","absolutePathOld","absolutePathNew",null,"ReadFile","absolutePath","reqId",null,"WriteBegin","absolutePath","reqId",null,"WriteData","reqId","fragmentCount",null,"WriteDone","reqId",null},
            new long[]{100, 10, 11, 0, 200, 10, 1, 0, 300, 10, 1, 0, 301, 1, 2, 0, 302, 1, 0},
            new String[]{"global",null,null,null,"global",null,null,null,"global",null,null,null,"global",null,null,null,"global",null,null},
            "fileIORequest.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});

    
    private SequentialFileIORequestSchema() {
        super(FROM);
    }

    
    public static final SequentialFileIORequestSchema instance = new SequentialFileIORequestSchema();
    
    public static final int MSG_RENAMEFILE_100 = 0x00000000;
    public static final int MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHOLD_10 = 0x01000001;
    public static final int MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHNEW_11 = 0x01000003;
    public static final int MSG_READFILE_200 = 0x00000004;
    public static final int MSG_READFILE_200_FIELD_ABSOLUTEPATH_10 = 0x01000001;
    public static final int MSG_READFILE_200_FIELD_REQID_1 = 0x00400003;
    public static final int MSG_WRITEBEGIN_300 = 0x00000008;
    public static final int MSG_WRITEBEGIN_300_FIELD_ABSOLUTEPATH_10 = 0x01000001;
    public static final int MSG_WRITEBEGIN_300_FIELD_REQID_1 = 0x00400003;
    public static final int MSG_WRITEDATA_301 = 0x0000000c;
    public static final int MSG_WRITEDATA_301_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_WRITEDATA_301_FIELD_FRAGMENTCOUNT_2 = 0x00000002;
    public static final int MSG_WRITEDONE_302 = 0x00000010;
    public static final int MSG_WRITEDONE_302_FIELD_REQID_1 = 0x00400001;


    public static void consume(Pipe<SequentialFileIORequestSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_RENAMEFILE_100:
                    consumeRenameFile(input);
                break;
                case MSG_READFILE_200:
                    consumeReadFile(input);
                break;
                case MSG_WRITEBEGIN_300:
                    consumeWriteBegin(input);
                break;
                case MSG_WRITEDATA_301:
                    consumeWriteData(input);
                break;
                case MSG_WRITEDONE_302:
                    consumeWriteDone(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeRenameFile(Pipe<SequentialFileIORequestSchema> input) {
        StringBuilder fieldabsolutePathOld = PipeReader.readUTF8(input,MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHOLD_10,new StringBuilder(PipeReader.readBytesLength(input,MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHOLD_10)));
        StringBuilder fieldabsolutePathNew = PipeReader.readUTF8(input,MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHNEW_11,new StringBuilder(PipeReader.readBytesLength(input,MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHNEW_11)));
    }
    public static void consumeReadFile(Pipe<SequentialFileIORequestSchema> input) {
        StringBuilder fieldabsolutePath = PipeReader.readUTF8(input,MSG_READFILE_200_FIELD_ABSOLUTEPATH_10,new StringBuilder(PipeReader.readBytesLength(input,MSG_READFILE_200_FIELD_ABSOLUTEPATH_10)));
        int fieldreqId = PipeReader.readInt(input,MSG_READFILE_200_FIELD_REQID_1);
    }
    public static void consumeWriteBegin(Pipe<SequentialFileIORequestSchema> input) {
        StringBuilder fieldabsolutePath = PipeReader.readUTF8(input,MSG_WRITEBEGIN_300_FIELD_ABSOLUTEPATH_10,new StringBuilder(PipeReader.readBytesLength(input,MSG_WRITEBEGIN_300_FIELD_ABSOLUTEPATH_10)));
        int fieldreqId = PipeReader.readInt(input,MSG_WRITEBEGIN_300_FIELD_REQID_1);
    }
    public static void consumeWriteData(Pipe<SequentialFileIORequestSchema> input) {
        int fieldreqId = PipeReader.readInt(input,MSG_WRITEDATA_301_FIELD_REQID_1);
        int fieldfragmentCount = PipeReader.readInt(input,MSG_WRITEDATA_301_FIELD_FRAGMENTCOUNT_2);
    }
    public static void consumeWriteDone(Pipe<SequentialFileIORequestSchema> input) {
        int fieldreqId = PipeReader.readInt(input,MSG_WRITEDONE_302_FIELD_REQID_1);
    }

    public static boolean publishRenameFile(Pipe<SequentialFileIORequestSchema> output, CharSequence fieldabsolutePathOld, CharSequence fieldabsolutePathNew) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_RENAMEFILE_100)) {
            PipeWriter.writeUTF8(output,MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHOLD_10, fieldabsolutePathOld);
            PipeWriter.writeUTF8(output,MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHNEW_11, fieldabsolutePathNew);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishReadFile(Pipe<SequentialFileIORequestSchema> output, CharSequence fieldabsolutePath, int fieldreqId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_READFILE_200)) {
            PipeWriter.writeUTF8(output,MSG_READFILE_200_FIELD_ABSOLUTEPATH_10, fieldabsolutePath);
            PipeWriter.writeInt(output,MSG_READFILE_200_FIELD_REQID_1, fieldreqId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishWriteBegin(Pipe<SequentialFileIORequestSchema> output, CharSequence fieldabsolutePath, int fieldreqId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_WRITEBEGIN_300)) {
            PipeWriter.writeUTF8(output,MSG_WRITEBEGIN_300_FIELD_ABSOLUTEPATH_10, fieldabsolutePath);
            PipeWriter.writeInt(output,MSG_WRITEBEGIN_300_FIELD_REQID_1, fieldreqId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishWriteData(Pipe<SequentialFileIORequestSchema> output, int fieldreqId, int fieldfragmentCount) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_WRITEDATA_301)) {
            PipeWriter.writeInt(output,MSG_WRITEDATA_301_FIELD_REQID_1, fieldreqId);
            PipeWriter.writeInt(output,MSG_WRITEDATA_301_FIELD_FRAGMENTCOUNT_2, fieldfragmentCount);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishWriteDone(Pipe<SequentialFileIORequestSchema> output, int fieldreqId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_WRITEDONE_302)) {
            PipeWriter.writeInt(output,MSG_WRITEDONE_302_FIELD_REQID_1, fieldreqId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


    
}
