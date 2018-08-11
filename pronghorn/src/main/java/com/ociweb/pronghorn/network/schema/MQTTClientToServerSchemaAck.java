package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class MQTTClientToServerSchemaAck extends MessageSchema<MQTTClientToServerSchemaAck> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400002,0x80000000,0xc0200002,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400003,0x90000000,0x80000000,0xc0200003,0xc0400001,0xc0200001},
		    (short)0,
		    new String[]{"StopRePublish","PacketId",null,"PubAck","Time","PacketId",null,"PubComp","Time",
		    "PacketId",null,"PubRel","Time","PacketId",null,"BrokerAcknowledgedConnection",null},
		    new long[]{99, 20, 0, 4, 37, 20, 0, 7, 37, 20, 0, 6, 37, 20, 0, 98, 0},
		    new String[]{"global",null,null,"global",null,null,null,"global",null,null,null,"global",null,
		    null,null,"global",null},
		    "MQTTClientToServerAck.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    
    protected MQTTClientToServerSchemaAck() {
        super(FROM);
    }

    
    public static final MQTTClientToServerSchemaAck instance = new MQTTClientToServerSchemaAck();
    
    public static final int MSG_STOPREPUBLISH_99 = 0x00000000; //Group/OpenTempl/2
    public static final int MSG_STOPREPUBLISH_99_FIELD_PACKETID_20 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_PUBACK_4 = 0x00000003; //Group/OpenTempl/3
    public static final int MSG_PUBACK_4_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_PUBACK_4_FIELD_PACKETID_20 = 0x00000003; //IntegerUnsigned/None/0
    public static final int MSG_PUBCOMP_7 = 0x00000007; //Group/OpenTempl/3
    public static final int MSG_PUBCOMP_7_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_PUBCOMP_7_FIELD_PACKETID_20 = 0x00000003; //IntegerUnsigned/None/0
    public static final int MSG_PUBREL_6 = 0x0000000b; //Group/OpenTempl/3
    public static final int MSG_PUBREL_6_FIELD_TIME_37 = 0x00800001; //LongUnsigned/None/0
    public static final int MSG_PUBREL_6_FIELD_PACKETID_20 = 0x00000003; //IntegerUnsigned/None/0
    public static final int MSG_BROKERACKNOWLEDGEDCONNECTION_98 = 0x0000000f; //Group/OpenTempl/1


    public static void consume(Pipe<MQTTClientToServerSchemaAck> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_STOPREPUBLISH_99:
                    consumeStopRePublish(input);
                break;
                case MSG_PUBACK_4:
                    consumePubAck(input);
                break;
                case MSG_PUBCOMP_7:
                    consumePubComp(input);
                break;
                case MSG_PUBREL_6:
                    consumePubRel(input);
                break;
                case MSG_BROKERACKNOWLEDGEDCONNECTION_98:
                    consumeBrokerAcknowledgedConnection(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeStopRePublish(Pipe<MQTTClientToServerSchemaAck> input) {
        int fieldPacketId = PipeReader.readInt(input,MSG_STOPREPUBLISH_99_FIELD_PACKETID_20);
    }
    public static void consumePubAck(Pipe<MQTTClientToServerSchemaAck> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBACK_4_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBACK_4_FIELD_PACKETID_20);
    }
    public static void consumePubComp(Pipe<MQTTClientToServerSchemaAck> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBCOMP_7_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBCOMP_7_FIELD_PACKETID_20);
    }
    public static void consumePubRel(Pipe<MQTTClientToServerSchemaAck> input) {
        long fieldTime = PipeReader.readLong(input,MSG_PUBREL_6_FIELD_TIME_37);
        int fieldPacketId = PipeReader.readInt(input,MSG_PUBREL_6_FIELD_PACKETID_20);
    }
    public static void consumeBrokerAcknowledgedConnection(Pipe<MQTTClientToServerSchemaAck> input) {
    }

    public static void publishStopRePublish(Pipe<MQTTClientToServerSchemaAck> output, int fieldPacketId) {
            PipeWriter.presumeWriteFragment(output, MSG_STOPREPUBLISH_99);
            PipeWriter.writeInt(output,MSG_STOPREPUBLISH_99_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
    }
    public static void publishPubAck(Pipe<MQTTClientToServerSchemaAck> output, long fieldTime, int fieldPacketId) {
            PipeWriter.presumeWriteFragment(output, MSG_PUBACK_4);
            PipeWriter.writeLong(output,MSG_PUBACK_4_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBACK_4_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
    }
    public static void publishPubComp(Pipe<MQTTClientToServerSchemaAck> output, long fieldTime, int fieldPacketId) {
            PipeWriter.presumeWriteFragment(output, MSG_PUBCOMP_7);
            PipeWriter.writeLong(output,MSG_PUBCOMP_7_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBCOMP_7_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
    }
    public static void publishPubRel(Pipe<MQTTClientToServerSchemaAck> output, long fieldTime, int fieldPacketId) {
            PipeWriter.presumeWriteFragment(output, MSG_PUBREL_6);
            PipeWriter.writeLong(output,MSG_PUBREL_6_FIELD_TIME_37, fieldTime);
            PipeWriter.writeInt(output,MSG_PUBREL_6_FIELD_PACKETID_20, fieldPacketId);
            PipeWriter.publishWrites(output);
    }
    public static void publishBrokerAcknowledgedConnection(Pipe<MQTTClientToServerSchemaAck> output) {
            PipeWriter.presumeWriteFragment(output, MSG_BROKERACKNOWLEDGEDCONNECTION_98);
            PipeWriter.publishWrites(output);
    }

        
}
