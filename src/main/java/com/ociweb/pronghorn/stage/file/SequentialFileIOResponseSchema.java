package com.ociweb.pronghorn.stage.file;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class SequentialFileIOResponseSchema extends MessageSchema {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc0400002,0x88000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400003,0x88000000,0x80000001,0xc0200003,0xc0400002,0x88000000,0xc0200002},
            (short)0,
            new String[]{"FileWritten","reqId",null,"FileNotFound","reqId",null,"FileReadBegin","reqId",null,"FileReadData","reqId","fragmentCount",null,"FileReadEnd","reqId",null},
            new long[]{303, 1, 0, 103, 1, 0, 201, 1, 0, 202, 1, 2, 0, 203, 1, 0},
            new String[]{"global",null,null,"global",null,null,"global",null,null,"global",null,null,null,"global",null,null},
            "fileIOResponse.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    public static final SequentialFileIOResponseSchema instance = new SequentialFileIOResponseSchema(FROM);
    
    public static final int MSG_FILEWRITTEN_303 = 0x00000000;
    public static final int MSG_FILEWRITTEN_303_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILENOTFOUND_103 = 0x00000003;
    public static final int MSG_FILENOTFOUND_103_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILEREADBEGIN_201 = 0x00000006;
    public static final int MSG_FILEREADBEGIN_201_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILEREADDATA_202 = 0x00000009;
    public static final int MSG_FILEREADDATA_202_FIELD_REQID_1 = 0x00400001;
    public static final int MSG_FILEREADDATA_202_FIELD_FRAGMENTCOUNT_2 = 0x00000002;
    public static final int MSG_FILEREADEND_203 = 0x0000000D;
    public static final int MSG_FILEREADEND_203_FIELD_REQID_1 = 0x00400001;

    
    private SequentialFileIOResponseSchema(FieldReferenceOffsetManager from) {
        super(from);
    }

    
    
}
