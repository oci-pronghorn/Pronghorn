package com.ociweb.pronghorn.network.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class ClientHTTPRequestSchema extends MessageSchema<ClientHTTPRequestSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400006,0x80000000,0x80000001,0xa8000000,0xa8000001,0xa8000002,0xc0200006,0xc0400007,0x80000000,0x80000001,0xa8000000,0x90000000,0xa8000001,0xa8000002,0xc0200007,0xc0400007,0x80000000,0x80000001,0xa8000000,0xa8000001,0xa8000002,0xb8000003,0xc0200007,0xc0400007,0x80000000,0x80000001,0xa8000000,0xa8000001,0x90000001,0xb8000003,0xc0200007,0xc0400003,0x80000000,0xb8000003,0xc0200003,0xc0400004,0x80000000,0x80000001,0xa8000000,0xc0200004,0xc0400008,0x80000000,0x80000001,0xa8000000,0x90000000,0xa8000001,0xa8000002,0xb8000003,0xc0200008,0xc0400005,0x80000000,0x80000001,0x90000000,0xa8000000,0xc0200005},
		    (short)0,
		    new String[]{"HTTPGet","Listener","Port","Host","Path","Headers",null,"FastHTTPGet","Listener",
		    "Port","Host","ConnectionId","Path","Headers",null,"HTTPPost","Listener","Port","Host",
		    "Path","Headers","Payload",null,"HTTPPostChunked","Listener","Port","Host","Path",
		    "TotalLength","PayloadChunk",null,"HTTPPostChunk","Listener","PayloadChunk",null,
		    "Close","Listener","Port","Host",null,"FastHTTPPost","Listener","Port","Host","ConnectionId",
		    "Path","Headers","Payload",null,"FastClose","Listener","Port","ConnectionId","Host",
		    null},
		    new long[]{100, 10, 1, 2, 3, 7, 0, 200, 10, 1, 2, 20, 3, 7, 0, 101, 10, 1, 2, 3, 7, 5, 0, 102, 10, 1, 2, 3, 6, 5, 0, 103, 10, 5, 0, 104, 10, 1, 2, 0, 201, 10, 1, 2, 20, 3, 7, 5, 0, 204, 10, 1, 20, 2, 0},
		    new String[]{"global",null,null,null,null,null,null,"global",null,null,null,null,null,null,null,
		    "global",null,null,null,null,null,null,null,"global",null,null,null,null,null,null,
		    null,"global",null,null,null,"global",null,null,null,null,"global",null,null,null,
		    null,null,null,null,null,"global",null,null,null,null,null},
		    "ClientHTTPRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});
    

    
    protected ClientHTTPRequestSchema() {
        super(FROM);
    }
    
    public static final ClientHTTPRequestSchema instance = new ClientHTTPRequestSchema();
    
    public static final int MSG_HTTPGET_100 = 0x00000000;
    public static final int MSG_HTTPGET_100_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_HTTPGET_100_FIELD_PORT_1 = 0x00000002;
    public static final int MSG_HTTPGET_100_FIELD_HOST_2 = 0x01400003;
    public static final int MSG_HTTPGET_100_FIELD_PATH_3 = 0x01400005;
    public static final int MSG_HTTPGET_100_FIELD_HEADERS_7 = 0x01400007;
    public static final int MSG_FASTHTTPGET_200 = 0x00000007;
    public static final int MSG_FASTHTTPGET_200_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_FASTHTTPGET_200_FIELD_PORT_1 = 0x00000002;
    public static final int MSG_FASTHTTPGET_200_FIELD_HOST_2 = 0x01400003;
    public static final int MSG_FASTHTTPGET_200_FIELD_CONNECTIONID_20 = 0x00800005;
    public static final int MSG_FASTHTTPGET_200_FIELD_PATH_3 = 0x01400007;
    public static final int MSG_FASTHTTPGET_200_FIELD_HEADERS_7 = 0x01400009;
    public static final int MSG_HTTPPOST_101 = 0x0000000f;
    public static final int MSG_HTTPPOST_101_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_HTTPPOST_101_FIELD_PORT_1 = 0x00000002;
    public static final int MSG_HTTPPOST_101_FIELD_HOST_2 = 0x01400003;
    public static final int MSG_HTTPPOST_101_FIELD_PATH_3 = 0x01400005;
    public static final int MSG_HTTPPOST_101_FIELD_HEADERS_7 = 0x01400007;
    public static final int MSG_HTTPPOST_101_FIELD_PAYLOAD_5 = 0x01c00009;
    public static final int MSG_HTTPPOSTCHUNKED_102 = 0x00000017;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PORT_1 = 0x00000002;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2 = 0x01400003;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3 = 0x01400005;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_TOTALLENGTH_6 = 0x00800007;
    public static final int MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5 = 0x01c00009;
    public static final int MSG_HTTPPOSTCHUNK_103 = 0x0000001f;
    public static final int MSG_HTTPPOSTCHUNK_103_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5 = 0x01c00002;
    public static final int MSG_CLOSE_104 = 0x00000023;
    public static final int MSG_CLOSE_104_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_CLOSE_104_FIELD_PORT_1 = 0x00000002;
    public static final int MSG_CLOSE_104_FIELD_HOST_2 = 0x01400003;
    public static final int MSG_FASTHTTPPOST_201 = 0x00000028;
    public static final int MSG_FASTHTTPPOST_201_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_FASTHTTPPOST_201_FIELD_PORT_1 = 0x00000002;
    public static final int MSG_FASTHTTPPOST_201_FIELD_HOST_2 = 0x01400003;
    public static final int MSG_FASTHTTPPOST_201_FIELD_CONNECTIONID_20 = 0x00800005;
    public static final int MSG_FASTHTTPPOST_201_FIELD_PATH_3 = 0x01400007;
    public static final int MSG_FASTHTTPPOST_201_FIELD_HEADERS_7 = 0x01400009;
    public static final int MSG_FASTHTTPPOST_201_FIELD_PAYLOAD_5 = 0x01c0000b;
    public static final int MSG_FASTCLOSE_204 = 0x00000031;
    public static final int MSG_FASTCLOSE_204_FIELD_LISTENER_10 = 0x00000001;
    public static final int MSG_FASTCLOSE_204_FIELD_PORT_1 = 0x00000002;
    public static final int MSG_FASTCLOSE_204_FIELD_CONNECTIONID_20 = 0x00800003;
    public static final int MSG_FASTCLOSE_204_FIELD_HOST_2 = 0x01400005;


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
        int fieldListener = PipeReader.readInt(input,MSG_HTTPGET_100_FIELD_LISTENER_10);
        int fieldPort = PipeReader.readInt(input,MSG_HTTPGET_100_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_HTTPGET_100_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPGET_100_FIELD_HOST_2)));
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_HTTPGET_100_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPGET_100_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_HTTPGET_100_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPGET_100_FIELD_HEADERS_7)));
    }
    public static void consumeFastHTTPGet(Pipe<ClientHTTPRequestSchema> input) {
        int fieldListener = PipeReader.readInt(input,MSG_FASTHTTPGET_200_FIELD_LISTENER_10);
        int fieldPort = PipeReader.readInt(input,MSG_FASTHTTPGET_200_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_FASTHTTPGET_200_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPGET_200_FIELD_HOST_2)));
        long fieldConnectionId = PipeReader.readLong(input,MSG_FASTHTTPGET_200_FIELD_CONNECTIONID_20);
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_FASTHTTPGET_200_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPGET_200_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_FASTHTTPGET_200_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPGET_200_FIELD_HEADERS_7)));
    }
    public static void consumeHTTPPost(Pipe<ClientHTTPRequestSchema> input) {
        int fieldListener = PipeReader.readInt(input,MSG_HTTPPOST_101_FIELD_LISTENER_10);
        int fieldPort = PipeReader.readInt(input,MSG_HTTPPOST_101_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_HTTPPOST_101_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOST_101_FIELD_HOST_2)));
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_HTTPPOST_101_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOST_101_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_HTTPPOST_101_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOST_101_FIELD_HEADERS_7)));
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_HTTPPOST_101_FIELD_PAYLOAD_5,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_HTTPPOST_101_FIELD_PAYLOAD_5)));
    }
    public static void consumeHTTPPostChunked(Pipe<ClientHTTPRequestSchema> input) {
        int fieldListener = PipeReader.readInt(input,MSG_HTTPPOSTCHUNKED_102_FIELD_LISTENER_10);
        int fieldPort = PipeReader.readInt(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2)));
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3)));
        long fieldTotalLength = PipeReader.readLong(input,MSG_HTTPPOSTCHUNKED_102_FIELD_TOTALLENGTH_6);
        ByteBuffer fieldPayloadChunk = PipeReader.readBytes(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5)));
    }
    public static void consumeHTTPPostChunk(Pipe<ClientHTTPRequestSchema> input) {
        int fieldListener = PipeReader.readInt(input,MSG_HTTPPOSTCHUNK_103_FIELD_LISTENER_10);
        ByteBuffer fieldPayloadChunk = PipeReader.readBytes(input,MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5)));
    }
    public static void consumeClose(Pipe<ClientHTTPRequestSchema> input) {
        int fieldListener = PipeReader.readInt(input,MSG_CLOSE_104_FIELD_LISTENER_10);
        int fieldPort = PipeReader.readInt(input,MSG_CLOSE_104_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_CLOSE_104_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_CLOSE_104_FIELD_HOST_2)));
    }
    public static void consumeFastHTTPPost(Pipe<ClientHTTPRequestSchema> input) {
        int fieldListener = PipeReader.readInt(input,MSG_FASTHTTPPOST_201_FIELD_LISTENER_10);
        int fieldPort = PipeReader.readInt(input,MSG_FASTHTTPPOST_201_FIELD_PORT_1);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_FASTHTTPPOST_201_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPPOST_201_FIELD_HOST_2)));
        long fieldConnectionId = PipeReader.readLong(input,MSG_FASTHTTPPOST_201_FIELD_CONNECTIONID_20);
        StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_FASTHTTPPOST_201_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPPOST_201_FIELD_PATH_3)));
        StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_FASTHTTPPOST_201_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTHTTPPOST_201_FIELD_HEADERS_7)));
        ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_FASTHTTPPOST_201_FIELD_PAYLOAD_5,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_FASTHTTPPOST_201_FIELD_PAYLOAD_5)));
    }
    public static void consumeFastClose(Pipe<ClientHTTPRequestSchema> input) {
        int fieldListener = PipeReader.readInt(input,MSG_FASTCLOSE_204_FIELD_LISTENER_10);
        int fieldPort = PipeReader.readInt(input,MSG_FASTCLOSE_204_FIELD_PORT_1);
        long fieldConnectionId = PipeReader.readLong(input,MSG_FASTCLOSE_204_FIELD_CONNECTIONID_20);
        StringBuilder fieldHost = PipeReader.readUTF8(input,MSG_FASTCLOSE_204_FIELD_HOST_2,new StringBuilder(PipeReader.readBytesLength(input,MSG_FASTCLOSE_204_FIELD_HOST_2)));
    }

    public static boolean publishHTTPGet(Pipe<ClientHTTPRequestSchema> output, int fieldListener, int fieldPort, CharSequence fieldHost, CharSequence fieldPath, CharSequence fieldHeaders) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_HTTPGET_100)) {
            PipeWriter.writeInt(output,MSG_HTTPGET_100_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeInt(output,MSG_HTTPGET_100_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_HTTPGET_100_FIELD_HOST_2, fieldHost);
            PipeWriter.writeUTF8(output,MSG_HTTPGET_100_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_HTTPGET_100_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishFastHTTPGet(Pipe<ClientHTTPRequestSchema> output, int fieldListener, int fieldPort, CharSequence fieldHost, long fieldConnectionId, CharSequence fieldPath, CharSequence fieldHeaders) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FASTHTTPGET_200)) {
            PipeWriter.writeInt(output,MSG_FASTHTTPGET_200_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeInt(output,MSG_FASTHTTPGET_200_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPGET_200_FIELD_HOST_2, fieldHost);
            PipeWriter.writeLong(output,MSG_FASTHTTPGET_200_FIELD_CONNECTIONID_20, fieldConnectionId);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPGET_200_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPGET_200_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishHTTPPost(Pipe<ClientHTTPRequestSchema> output, int fieldListener, int fieldPort, CharSequence fieldHost, CharSequence fieldPath, CharSequence fieldHeaders, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_HTTPPOST_101)) {
            PipeWriter.writeInt(output,MSG_HTTPPOST_101_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeInt(output,MSG_HTTPPOST_101_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_HTTPPOST_101_FIELD_HOST_2, fieldHost);
            PipeWriter.writeUTF8(output,MSG_HTTPPOST_101_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_HTTPPOST_101_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.writeBytes(output,MSG_HTTPPOST_101_FIELD_PAYLOAD_5, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishHTTPPostChunked(Pipe<ClientHTTPRequestSchema> output, int fieldListener, int fieldPort, CharSequence fieldHost, CharSequence fieldPath, long fieldTotalLength, byte[] fieldPayloadChunkBacking, int fieldPayloadChunkPosition, int fieldPayloadChunkLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_HTTPPOSTCHUNKED_102)) {
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNKED_102_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNKED_102_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_HTTPPOSTCHUNKED_102_FIELD_HOST_2, fieldHost);
            PipeWriter.writeUTF8(output,MSG_HTTPPOSTCHUNKED_102_FIELD_PATH_3, fieldPath);
            PipeWriter.writeLong(output,MSG_HTTPPOSTCHUNKED_102_FIELD_TOTALLENGTH_6, fieldTotalLength);
            PipeWriter.writeBytes(output,MSG_HTTPPOSTCHUNKED_102_FIELD_PAYLOADCHUNK_5, fieldPayloadChunkBacking, fieldPayloadChunkPosition, fieldPayloadChunkLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishHTTPPostChunk(Pipe<ClientHTTPRequestSchema> output, int fieldListener, byte[] fieldPayloadChunkBacking, int fieldPayloadChunkPosition, int fieldPayloadChunkLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_HTTPPOSTCHUNK_103)) {
            PipeWriter.writeInt(output,MSG_HTTPPOSTCHUNK_103_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeBytes(output,MSG_HTTPPOSTCHUNK_103_FIELD_PAYLOADCHUNK_5, fieldPayloadChunkBacking, fieldPayloadChunkPosition, fieldPayloadChunkLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishClose(Pipe<ClientHTTPRequestSchema> output, int fieldListener, int fieldPort, CharSequence fieldHost) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_CLOSE_104)) {
            PipeWriter.writeInt(output,MSG_CLOSE_104_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeInt(output,MSG_CLOSE_104_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_CLOSE_104_FIELD_HOST_2, fieldHost);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishFastHTTPPost(Pipe<ClientHTTPRequestSchema> output, int fieldListener, int fieldPort, CharSequence fieldHost, long fieldConnectionId, CharSequence fieldPath, CharSequence fieldHeaders, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FASTHTTPPOST_201)) {
            PipeWriter.writeInt(output,MSG_FASTHTTPPOST_201_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeInt(output,MSG_FASTHTTPPOST_201_FIELD_PORT_1, fieldPort);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPPOST_201_FIELD_HOST_2, fieldHost);
            PipeWriter.writeLong(output,MSG_FASTHTTPPOST_201_FIELD_CONNECTIONID_20, fieldConnectionId);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPPOST_201_FIELD_PATH_3, fieldPath);
            PipeWriter.writeUTF8(output,MSG_FASTHTTPPOST_201_FIELD_HEADERS_7, fieldHeaders);
            PipeWriter.writeBytes(output,MSG_FASTHTTPPOST_201_FIELD_PAYLOAD_5, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }
    public static boolean publishFastClose(Pipe<ClientHTTPRequestSchema> output, int fieldListener, int fieldPort, long fieldConnectionId, CharSequence fieldHost) {
        boolean result = false;
        if (PipeWriter.tryWriteFragment(output, MSG_FASTCLOSE_204)) {
            PipeWriter.writeInt(output,MSG_FASTCLOSE_204_FIELD_LISTENER_10, fieldListener);
            PipeWriter.writeInt(output,MSG_FASTCLOSE_204_FIELD_PORT_1, fieldPort);
            PipeWriter.writeLong(output,MSG_FASTCLOSE_204_FIELD_CONNECTIONID_20, fieldConnectionId);
            PipeWriter.writeUTF8(output,MSG_FASTCLOSE_204_FIELD_HOST_2, fieldHost);
            PipeWriter.publishWrites(output);
            result = true;
        }
        return result;
    }


        
}
