package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ServerConnectionSchema extends MessageSchema<ServerConnectionSchema> {

    public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
            new int[]{0xc1400003,0x90200000,0x90800001,0xc1200003},
            (short)0,
            new String[]{"ServerConnection","ConnectionGroup","ChannelId",null},
            new long[]{100, 1, 2, 0},
            new String[]{"global",null,null,null},
            "serverConnect.xml",
            new long[]{2, 2, 0},
            new int[]{2, 2, 0});
    
    private ServerConnectionSchema() {
    	super(FROM);
    }
    
    public static final ServerConnectionSchema instance = new ServerConnectionSchema();
    
    public static final int MSG_SERVERCONNECTION_100 = 0x00000000;
    public static final int MSG_SERVERCONNECTION_100_FIELD_CONNECTIONGROUP_1 = 0x00800001;
    public static final int MSG_SERVERCONNECTION_100_FIELD_CHANNELID_2 = 0x00800003;


    public static void consume(Pipe<ServerConnectionSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_SERVERCONNECTION_100:
                    consumeServerConnection(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeServerConnection(Pipe<ServerConnectionSchema> input) {
        long fieldConnectionGroup = PipeReader.readLong(input,MSG_SERVERCONNECTION_100_FIELD_CONNECTIONGROUP_1);
        long fieldChannelId = PipeReader.readLong(input,MSG_SERVERCONNECTION_100_FIELD_CHANNELID_2);
    }

    public static boolean publishServerConnection(Pipe<ServerConnectionSchema> output, long fieldConnectionGroup, long fieldChannelId) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_SERVERCONNECTION_100)) {
            PipeWriter.writeLong(output,MSG_SERVERCONNECTION_100_FIELD_CONNECTIONGROUP_1, fieldConnectionGroup);
            PipeWriter.writeLong(output,MSG_SERVERCONNECTION_100_FIELD_CHANNELID_2, fieldChannelId);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


    
    
}
