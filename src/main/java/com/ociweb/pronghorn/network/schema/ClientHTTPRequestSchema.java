package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ClientHTTPRequestSchema extends MessageSchema<ClientHTTPRequestSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400007,0x80000000,0x80000001,0x80000002,0xa8000000,0xa8000001,0xa8000002,0xc0200007,0xc0400008,0x80000000,0x80000001,0x80000002,0xa8000000,0x90000000,0xa8000001,0xa8000002,0xc0200008,0xc0400008,0x80000000,0x80000001,0x80000002,0xa8000000,0xa8000001,0xa8000002,0xb8000003,0xc0200008,0xc0400008,0x80000000,0x80000001,0x80000002,0xa8000000,0xa8000001,0x90000001,0xb8000003,0xc0200008,0xc0400004,0x80000000,0x80000001,0xb8000003,0xc0200004,0xc0400004,0x80000001,0x80000002,0xa8000000,0xc0200004,0xc0400009,0x80000000,0x80000001,0x80000002,0xa8000000,0x90000000,0xa8000001,0xa8000002,0xb8000003,0xc0200009,0xc0400005,0x80000001,0x80000002,0x90000000,0xa8000000,0xc0200005},
		    (short)0,
		    new String[]{"HTTPGet","Destination","Session","Port","Host","Path","Headers",null,"FastHTTPGet",
		    "Destination","Session","Port","Host","ConnectionId","Path","Headers",null,"HTTPPost",
		    "Destination","Session","Port","Host","Path","Headers","Payload",null,"HTTPPostChunked",
		    "Destination","Session","Port","Host","Path","TotalLength","PayloadChunk",null,"HTTPPostChunk",
		    "Destination","Session","PayloadChunk",null,"Close","Session","Port","Host",null,
		    "FastHTTPPost","Destination","Session","Port","Host","ConnectionId","Path","Headers",
		    "Payload",null,"FastClose","Session","Port","ConnectionId","Host",null},
		    new long[]{100, 11, 10, 1, 2, 3, 7, 0, 200, 11, 10, 1, 2, 20, 3, 7, 0, 101, 11, 10, 1, 2, 3, 7, 5, 0, 102, 11, 10, 1, 2, 3, 6, 5, 0, 103, 11, 10, 5, 0, 104, 10, 1, 2, 0, 201, 11, 10, 1, 2, 20, 3, 7, 5, 0, 204, 10, 1, 20, 2, 0},
		    new String[]{"global",null,null,null,null,null,null,null,"global",null,null,null,null,null,null,
		    null,null,"global",null,null,null,null,null,null,null,null,"global",null,null,null,
		    null,null,null,null,null,"global",null,null,null,null,"global",null,null,null,null,
		    "global",null,null,null,null,null,null,null,null,null,"global",null,null,null,null,
		    null},
		    "ClientHTTPRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    

    
    protected ClientHTTPRequestSchema() {
        super(FROM);
    }
    
    public static final ClientHTTPRequestSchema instance = new ClientHTTPRequestSchema();
    

    public static final int MSG_HTTPGET_100 = 0x00000000; //Group/OpenTempl/7
    public static final int MSG_HTTPGET_100_FIELD_DESTINATION_11 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_HTTPGET_100_FIELD_SESSION_10 = 0x00000002; //IntegerUnsigned/None/1
    public static final int MSG_HTTPGET_100_FIELD_PORT_1 = 0x00000003; //IntegerUnsigned/None/2
    public static final int MSG_HTTPGET_100_FIELD_HOST_2 = 0x01400004; //UTF8/None/0
    public static final int MSG_HTTPGET_100_FIELD_PATH_3 = 0x01400006; //UTF8/None/1
    public static final int MSG_HTTPGET_100_FIELD_HEADERS_7 = 0x01400008; //UTF8/None/2
    
    public static final int MSG_FASTHTTPGET_200 = 0x00000008; //Group/OpenTempl/8
    public static final int MSG_FASTHTTPGET_200_FIELD_DESTINATION_11 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_FASTHTTPGET_200_FIELD_SESSION_10 = 0x00000002; //IntegerUnsigned/None/1
    public static final int MSG_FASTHTTPGET_200_FIELD_PORT_1 = 0x00000003; //IntegerUnsigned/None/2
    public static final int MSG_FASTHTTPGET_200_FIELD_HOST_2 = 0x01400004; //UTF8/None/0
    public static final int MSG_FASTHTTPGET_200_FIELD_CONNECTIONID_20 = 0x00800006; //LongUnsigned/None/0
    public static final int MSG_FASTHTTPGET_200_FIELD_PATH_3 = 0x01400008; //UTF8/None/1
    public static final int MSG_FASTHTTPGET_200_FIELD_HEADERS_7 = 0x0140000a; //UTF8/None/2
    
    public static final int MSG_HTTPPOST_101 = 0x00000011; //Group/OpenTempl/8
    public static final int MSG_HTTPPOST_101_FIELD_DESTINATION_11 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_HTTPPOST_101_FIELD_SESSION_10 = 0x00000002; //IntegerUnsigned/None/1
    public static final int MSG_HTTPPOST_101_FIELD_PORT_1 = 0x00000003; //IntegerUnsigned/None/2
    public static final int MSG_HTTPPOST_101_FIELD_HOST_2 = 0x01400004; //UTF8/None/0
    public static final int MSG_HTTPPOST_101_FIELD_PATH_3 = 0x01400006; //UTF8/None/1
    public static final int MSG_HTTPPOST_101_FIELD_HEADERS_7 = 0x01400008; //UTF8/None/2
    public static final int MSG_HTTPPOST_101_FIELD_PAYLOAD_5 = 0x01c0000a; //ByteVector/None/3
    
    
    public static final int MSG_HTTPPOSTCHUNKED_102 = 0x0000001a; //Group/OpenTempl/8
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_DESTINATION_11 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_SESSION_10 = 0x00000002; //IntegerUnsigned/None/1
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PORT_1 = 0x00000003; //IntegerUnsigned/None/2
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2 = 0x01400004; //UTF8/None/0
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3 = 0x01400006; //UTF8/None/1
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_TOTALLENGTH_6 = 0x00800008; //LongUnsigned/None/1
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5 = 0x01c0000a; //ByteVector/None/3
    
    public static final int MSG_HTTPPOSTCHUNK_103 = 0x00000023; //Group/OpenTempl/4
    public static final int MSG_HTTPPOSTCHUNK_103_FIELD_DESTINATION_11 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_HTTPPOSTCHUNK_103_FIELD_SESSION_10 = 0x00000002; //IntegerUnsigned/None/1
    public static final int MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5 = 0x01c00003; //ByteVector/None/3
    
    public static final int MSG_CLOSE_104 = 0x00000028; //Group/OpenTempl/4
    public static final int MSG_CLOSE_104_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/1
    public static final int MSG_CLOSE_104_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/2
    public static final int MSG_CLOSE_104_FIELD_HOST_2 = 0x01400003; //UTF8/None/0
    
    public static final int MSG_FASTHTTPPOST_201 = 0x0000002d; //Group/OpenTempl/9
    public static final int MSG_FASTHTTPPOST_201_FIELD_DESTINATION_11 = 0x00000001; //IntegerUnsigned/None/0
    public static final int MSG_FASTHTTPPOST_201_FIELD_SESSION_10 = 0x00000002; //IntegerUnsigned/None/1
    public static final int MSG_FASTHTTPPOST_201_FIELD_PORT_1 = 0x00000003; //IntegerUnsigned/None/2
    public static final int MSG_FASTHTTPPOST_201_FIELD_HOST_2 = 0x01400004; //UTF8/None/0
    public static final int MSG_FASTHTTPPOST_201_FIELD_CONNECTIONID_20 = 0x00800006; //LongUnsigned/None/0
    public static final int MSG_FASTHTTPPOST_201_FIELD_PATH_3 = 0x01400008; //UTF8/None/1
    public static final int MSG_FASTHTTPPOST_201_FIELD_HEADERS_7 = 0x0140000a; //UTF8/None/2
    public static final int MSG_FASTHTTPPOST_201_FIELD_PAYLOAD_5 = 0x01c0000c; //ByteVector/None/3
    
    public static final int MSG_FASTCLOSE_204 = 0x00000037; //Group/OpenTempl/5
    public static final int MSG_FASTCLOSE_204_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/1
    public static final int MSG_FASTCLOSE_204_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/2
    public static final int MSG_FASTCLOSE_204_FIELD_CONNECTIONID_20 = 0x00800003; //LongUnsigned/None/0
    public static final int MSG_FASTCLOSE_204_FIELD_HOST_2 = 0x01400005; //UTF8/None/0


    public static void consume(Pipe<ClientHTTPRequestSchema> input) {
        while (PipeReader.tryReadFragment(input)) {
            int msgIdx = PipeReader.getMsgIdx(input);
            switch(msgIdx) {
                case MSG_HTTPGET_100:
                    consumeHTTPGet(input);
                break;
                case MSG_FASTHTTPGET_200:
                    consumeFastHTTPGet(input);
                break;
                case MSG_HTTPPOST_101:
                    consumeHTTPPost(input);
                break;
                case MSG_HTTPPOSTCHUNKED_102:
                    consumeHTTPPostChunked(input);
                break;
                case MSG_HTTPPOSTCHUNK_103:
                    consumeHTTPPostChunk(input);
                break;
                case MSG_CLOSE_104:
                    consumeClose(input);
                break;
                case MSG_FASTHTTPPOST_201:
                    consumeFastHTTPPost(input);
                break;
                case MSG_FASTCLOSE_204:
                    consumeFastClose(input);
                break;
                case -1:
                   //requestShutdown();
                break;
            }
            PipeReader.releaseReadLock(input);
        }
    }

    public static void consumeHTTPGet(Pipe<ClientHTTPRequestSchema> input) {
        int fieldDestination = PipeReader.readInt(input,MSG_HTTPGET_100_FIELD_DESTINATION_11);
        int fieldSession = PipeReader.readInt(input,MSG_HTTPGET_100_FIELD_SESSION_10);
        int fieldPort = PipeReader.readInt(input,MSG_HTTPGET_100_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_HTTPGET_100_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPGET_100_FIELD_HOST_2)));
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_HTTPGET_100_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPGET_100_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_HTTPGET_100_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPGET_100_FIELD_HEADERS_7)));
    }
    public static void consumeFastHTTPGet(Pipe<ClientHTTPRequestSchema> input) {
        int fieldDestination = PipeReader.readInt(input,MSG_FASTHTTPGET_200_FIELD_DESTINATION_11);
        int fieldSession = PipeReader.readInt(input,MSG_FASTHTTPGET_200_FIELD_SESSION_10);
        int fieldPort = PipeReader.readInt(input,MSG_FASTHTTPGET_200_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_FASTHTTPGET_200_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPGET_200_FIELD_HOST_2)));
        long fieldConnectionId = PipeReader.readLong(input,MSG_FASTHTTPGET_200_FIELD_CONNECTIONID_20);
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_FASTHTTPGET_200_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPGET_200_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_FASTHTTPGET_200_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPGET_200_FIELD_HEADERS_7)));
    }
    public static void consumeHTTPPost(Pipe<ClientHTTPRequestSchema> input) {
        int fieldDestination = PipeReader.readInt(input,MSG_HTTPPOST_101_FIELD_DESTINATION_11);
        int fieldSession = PipeReader.readInt(input,MSG_HTTPPOST_101_FIELD_SESSION_10);
        int fieldPort = PipeReader.readInt(input,MSG_HTTPPOST_101_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_HTTPPOST_101_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOST_101_FIELD_HOST_2)));
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_HTTPPOST_101_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOST_101_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_HTTPPOST_101_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOST_101_FIELD_HEADERS_7)));
        DataInputBlobReader<ClientHTTPRequestSchema> fieldPayload = PipeReader.inputStream(input, MSG_HTTPPOST_101_FIELD_PAYLOAD_5);
    }
    public static void consumeHTTPPostChunked(Pipe<ClientHTTPRequestSchema> input) {
        int fieldDestination = PipeReader.readInt(input,MSG_HTTPPOSTCHUNKED_102_FIELD_DESTINATION_11);
        int fieldSession = PipeReader.readInt(input,MSG_HTTPPOSTCHUNKED_102_FIELD_SESSION_10);
        int fieldPort = PipeReader.readInt(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2)));
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3)));
        long fieldTotalLength = PipeReader.readLong(input,MSG_HTTPPOSTCHUNKED_102_FIELD_TOTALLENGTH_6);
        DataInputBlobReader<ClientHTTPRequestSchema> fieldPayloadChunk = PipeReader.inputStream(input, MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5);
    }
    public static void consumeHTTPPostChunk(Pipe<ClientHTTPRequestSchema> input) {
        int fieldDestination = PipeReader.readInt(input,MSG_HTTPPOSTCHUNK_103_FIELD_DESTINATION_11);
        int fieldSession = PipeReader.readInt(input,MSG_HTTPPOSTCHUNK_103_FIELD_SESSION_10);
        DataInputBlobReader<ClientHTTPRequestSchema> fieldPayloadChunk = PipeReader.inputStream(input, MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5);
    }
    public static void consumeClose(Pipe<ClientHTTPRequestSchema> input) {
        int fieldSession = PipeReader.readInt(input,MSG_CLOSE_104_FIELD_SESSION_10);
        int fieldPort = PipeReader.readInt(input,MSG_CLOSE_104_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_CLOSE_104_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_CLOSE_104_FIELD_HOST_2)));
    }
    public static void consumeFastHTTPPost(Pipe<ClientHTTPRequestSchema> input) {
        int fieldDestination = PipeReader.readInt(input,MSG_FASTHTTPPOST_201_FIELD_DESTINATION_11);
        int fieldSession = PipeReader.readInt(input,MSG_FASTHTTPPOST_201_FIELD_SESSION_10);
        int fieldPort = PipeReader.readInt(input,MSG_FASTHTTPPOST_201_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_FASTHTTPPOST_201_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPPOST_201_FIELD_HOST_2)));
        long fieldConnectionId = PipeReader.readLong(input,MSG_FASTHTTPPOST_201_FIELD_CONNECTIONID_20);
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_FASTHTTPPOST_201_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPPOST_201_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_FASTHTTPPOST_201_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPPOST_201_FIELD_HEADERS_7)));
        DataInputBlobReader<ClientHTTPRequestSchema> fieldPayload = PipeReader.inputStream(input, MSG_FASTHTTPPOST_201_FIELD_PAYLOAD_5);
    }
    public static void consumeFastClose(Pipe<ClientHTTPRequestSchema> input) {
        int fieldSession = PipeReader.readInt(input,MSG_FASTCLOSE_204_FIELD_SESSION_10);
        int fieldPort = PipeReader.readInt(input,MSG_FASTCLOSE_204_FIELD_PORT_1);
        long fieldConnectionId = PipeReader.readLong(input,MSG_FASTCLOSE_204_FIELD_CONNECTIONID_20);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_FASTCLOSE_204_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTCLOSE_204_FIELD_HOST_2)));
    }

    public static void publishHTTPGet(Pipe<ClientHTTPRequestSchema> output, int fieldDestination, int fieldSession, int fieldPort, CharSequence fieldHost, CharSequence fieldPath, CharSequence fieldHeaders) {
            PipeWriter.presumeWriteFragment(output, MSG_HTTPGET_100);
            assert(fieldDestination>=0);
            PipeWriter.writeInt(output,MSG_HTTPGET_100_FIELD_DESTINATION_11, fieldDestination);
            PipeWriter.writeInt(output,MSG_HTTPGET_100_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeInt(output,MSG_HTTPGET_100_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_HTTPGET_100_FIELD_HOST_2, fieldHost);
            PipeWriter.writeUTF8(output,MSG_HTTPGET_100_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_HTTPGET_100_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.publishWrites(output);
    }
    public static void publishFastHTTPGet(Pipe<ClientHTTPRequestSchema> output, int fieldDestination, int fieldSession, int fieldPort, CharSequence fieldHost, long fieldConnectionId, CharSequence fieldPath, CharSequence fieldHeaders) {
            PipeWriter.presumeWriteFragment(output, MSG_FASTHTTPGET_200);
            PipeWriter.writeInt(output,MSG_FASTHTTPGET_200_FIELD_DESTINATION_11, fieldDestination);
            PipeWriter.writeInt(output,MSG_FASTHTTPGET_200_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeInt(output,MSG_FASTHTTPGET_200_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPGET_200_FIELD_HOST_2, fieldHost);
            PipeWriter.writeLong(output,MSG_FASTHTTPGET_200_FIELD_CONNECTIONID_20, fieldConnectionId);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPGET_200_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPGET_200_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.publishWrites(output);
    }
    public static void publishHTTPPost(Pipe<ClientHTTPRequestSchema> output, int fieldDestination, int fieldSession, int fieldPort, CharSequence fieldHost, CharSequence fieldPath, CharSequence fieldHeaders, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_HTTPPOST_101);
            PipeWriter.writeInt(output,MSG_HTTPPOST_101_FIELD_DESTINATION_11, fieldDestination);
            PipeWriter.writeInt(output,MSG_HTTPPOST_101_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeInt(output,MSG_HTTPPOST_101_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_HTTPPOST_101_FIELD_HOST_2, fieldHost);
            PipeWriter.writeUTF8(output,MSG_HTTPPOST_101_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_HTTPPOST_101_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.writeBytes(output,MSG_HTTPPOST_101_FIELD_PAYLOAD_5, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishHTTPPostChunked(Pipe<ClientHTTPRequestSchema> output, int fieldDestination, int fieldSession, int fieldPort, CharSequence fieldHost, CharSequence fieldPath, long fieldTotalLength, byte[] fieldPayloadChunkBacking, int fieldPayloadChunkPosition, int fieldPayloadChunkLength) {
            PipeWriter.presumeWriteFragment(output, MSG_HTTPPOSTCHUNKED_102);
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNKED_102_FIELD_DESTINATION_11, fieldDestination);
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNKED_102_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNKED_102_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2, fieldHost);
            PipeWriter.writeUTF8(output,MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3, fieldPath);
            PipeWriter.writeLong(output,MSG_HTTPPOSTCHUNKED_102_FIELD_TOTALLENGTH_6, fieldTotalLength);
            PipeWriter.writeBytes(output,MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5, fieldPayloadChunkBacking, fieldPayloadChunkPosition, fieldPayloadChunkLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishHTTPPostChunk(Pipe<ClientHTTPRequestSchema> output, int fieldDestination, int fieldSession, byte[] fieldPayloadChunkBacking, int fieldPayloadChunkPosition, int fieldPayloadChunkLength) {
            PipeWriter.presumeWriteFragment(output, MSG_HTTPPOSTCHUNK_103);
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNK_103_FIELD_DESTINATION_11, fieldDestination);
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNK_103_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeBytes(output,MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5, fieldPayloadChunkBacking, fieldPayloadChunkPosition, fieldPayloadChunkLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishClose(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, CharSequence fieldHost) {
            PipeWriter.presumeWriteFragment(output, MSG_CLOSE_104);
            PipeWriter.writeInt(output,MSG_CLOSE_104_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeInt(output,MSG_CLOSE_104_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_CLOSE_104_FIELD_HOST_2, fieldHost);
            PipeWriter.publishWrites(output);
    }
    public static void publishFastHTTPPost(Pipe<ClientHTTPRequestSchema> output, int fieldDestination, int fieldSession, int fieldPort, CharSequence fieldHost, long fieldConnectionId, CharSequence fieldPath, CharSequence fieldHeaders, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
            PipeWriter.presumeWriteFragment(output, MSG_FASTHTTPPOST_201);
            PipeWriter.writeInt(output,MSG_FASTHTTPPOST_201_FIELD_DESTINATION_11, fieldDestination);
            PipeWriter.writeInt(output,MSG_FASTHTTPPOST_201_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeInt(output,MSG_FASTHTTPPOST_201_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPPOST_201_FIELD_HOST_2, fieldHost);
            PipeWriter.writeLong(output,MSG_FASTHTTPPOST_201_FIELD_CONNECTIONID_20, fieldConnectionId);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPPOST_201_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPPOST_201_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.writeBytes(output,MSG_FASTHTTPPOST_201_FIELD_PAYLOAD_5, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
    }
    public static void publishFastClose(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, long fieldConnectionId, CharSequence fieldHost) {
            PipeWriter.presumeWriteFragment(output, MSG_FASTCLOSE_204);
            PipeWriter.writeInt(output,MSG_FASTCLOSE_204_FIELD_SESSION_10, fieldSession);
            PipeWriter.writeInt(output,MSG_FASTCLOSE_204_FIELD_PORT_1, fieldPort);
            PipeWriter.writeLong(output,MSG_FASTCLOSE_204_FIELD_CONNECTIONID_20, fieldConnectionId);
            PipeWriter.writeUTF8(output,MSG_FASTCLOSE_204_FIELD_HOST_2, fieldHost);
            PipeWriter.publishWrites(output);
    }
}
