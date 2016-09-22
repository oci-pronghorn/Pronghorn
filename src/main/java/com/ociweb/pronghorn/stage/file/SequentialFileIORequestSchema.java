package com.ociweb.pronghorn.stage.file;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class SequentialFileIORequestSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400003,0xa0000000,0xa0000001,0xc0200003,0xc0400003,0xa0000000,0x88000000,0xc0200003,0xc0400003,0xa0000000,0x88000000,0xc0200003,0xc0400003,0x88000000,0x80000001,0xc0200003,0xc0400002,0x88000000,0xc0200002},
            (short)0,
            new String[]{"RenameFile","absolutePathOld","absolutePathNew",null,"ReadFile","absolutePath","reqId",null,"WriteBegin","absolutePath","reqId",null,"WriteData","reqId","fragmentCount",null,"WriteDone","reqId",null},
            new long[]{100, 10, 11, 0, 200, 10, 1, 0, 300, 10, 1, 0, 301, 1, 2, 0, 302, 1, 0},
            new String[]{"global",null,null,null,"global",null,null,null,"global",null,null,null,"global",null,null,null,"global",null,null},
            "fileIORequest.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    public static final SequentialFileIORequestSchema instance = new SequentialFileIORequestSchema(FROM);
    
    public static final int MSG_RENAMEFILE_100 = 0x00000000;
    public static final int MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHOLD_10 = 0x01000001;
    public static final int MSG_RENAMEFILE_100_FIELD_ABSOLUTEPATHNEW_11 = 0x01000003;
    public static final int MSG_READFILE_200 = 0x00000004;
    public static final int MSG_READFILE_200_FIELD_ABSOLUTEPATH_10 = 0x01000001;
    public static final int MSG_READFILE_200_FIELD_REQID_1 = 0x00400003;
    public static final int MSG_WRITEBEGIN_300 = 0x00000008;
    public static final int MSG_WRITEBEGIN_300_FIELD_ABSOLUTEPATH_10 = 0x01000001;
    public static final int MSG_WRITEBEGIN_300_FIELD_REQID_1 = 0x00400003;
    public static final int MSG_WRITEDATA_301 = 0x0000000C;
    public static final int MSG_WRITEDATA_301_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_WRITEDATA_301_FIELD_FRAGMENTCOUNT_2 = 0x00000002;
    public static final int MSG_WRITEDONE_302 = 0x00000010;
    public static final int MSG_WRITEDONE_302_FIELD_REQID_1 = 0x00400001;

    
    private SequentialFileIORequestSchema(FieldReferenceOffsetManager from) {
        super(from);
    }

    
    
}
