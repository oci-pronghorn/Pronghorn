package com.ociweb.pronghorn.network.schema;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Defines a client hTTP request. Includes payload, headers, destination, session, port, host, path,
 * and more required for a functioning HTTP client.
 */
public class ClientHTTPRequestSchema extends MessageSchema<ClientHTTPRequestSchema> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400008,0x80000000,0x80000001,0x80000002,0x90000000,0x80000003,0xa8000000,0xa8000001,0xc0200008,0xc0400009,0x80000000,0x80000001,0x80000002,0x90000000,0x80000003,0xa8000000,0xa8000001,0xb8000002,0xc0200009,0xc0400005,0x80000000,0x80000001,0x80000002,0x90000000,0xc0200005,0xc0400008,0x80000000,0x80000001,0x80000002,0x90000000,0x80000003,0xa8000000,0xa8000001,0xc0200008,0xc0400008,0x80000000,0x80000001,0x80000002,0x90000000,0x80000003,0xa8000000,0xa8000001,0xc0200008,0xc0400009,0x80000000,0x80000001,0x80000002,0x90000000,0x80000003,0xa8000000,0xa8000001,0xb8000002,0xc0200009,0xc0400009,0x80000000,0x80000001,0x80000002,0x90000000,0x80000003,0xa8000000,0xa8000001,0xb8000002,0xc0200009},
		    (short)0,
		    new String[]{"GET","Session","Port","HostId","ConnectionId","Destination","Path","Headers",null,
		    "POST","Session","Port","HostId","ConnectionId","Destination","Path","Headers","Payload",
		    null,"CLOSEConnection","Session","Port","HostId","ConnectionId",null,"HEAD","Session",
		    "Port","HostId","ConnectionId","Destination","Path","Headers",null,"DELETE","Session",
		    "Port","HostId","ConnectionId","Destination","Path","Headers",null,"PUT","Session",
		    "Port","HostId","ConnectionId","Destination","Path","Headers","Payload",null,"PATCH",
		    "Session","Port","HostId","ConnectionId","Destination","Path","Headers","Payload",
		    null},
		    new long[]{200, 10, 1, 2, 20, 11, 3, 7, 0, 201, 10, 1, 2, 20, 11, 3, 7, 5, 0, 104, 10, 1, 2, 20, 0, 202, 10, 1, 2, 20, 11, 3, 7, 0, 203, 10, 1, 2, 20, 11, 3, 7, 0, 204, 10, 1, 2, 20, 11, 3, 7, 5, 0, 205, 10, 1, 2, 20, 11, 3, 7, 5, 0},
		    new String[]{"global",null,null,null,null,null,null,null,null,"global",null,null,null,null,null,
		    null,null,null,null,"global",null,null,null,null,null,"global",null,null,null,null,
		    null,null,null,null,"global",null,null,null,null,null,null,null,null,"global",null,
		    null,null,null,null,null,null,null,null,"global",null,null,null,null,null,null,null,
		    null,null},
		    "ClientHTTPRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		public ClientHTTPRequestSchema() { 
		    super(FROM);
		}

		protected ClientHTTPRequestSchema(FieldReferenceOffsetManager from) { 
		    super(from);
		}

		public static final ClientHTTPRequestSchema instance = new ClientHTTPRequestSchema();

		public static final int MSG_GET_200 = 0x00000000; //Group/OpenTempl/8
		public static final int MSG_GET_200_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_GET_200_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/1
		public static final int MSG_GET_200_FIELD_HOSTID_2 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_GET_200_FIELD_CONNECTIONID_20 = 0x00800004; //LongUnsigned/None/0
		public static final int MSG_GET_200_FIELD_DESTINATION_11 = 0x00000006; //IntegerUnsigned/None/3
		public static final int MSG_GET_200_FIELD_PATH_3 = 0x01400007; //UTF8/None/0
		public static final int MSG_GET_200_FIELD_HEADERS_7 = 0x01400009; //UTF8/None/1
		public static final int MSG_POST_201 = 0x00000009; //Group/OpenTempl/9
		public static final int MSG_POST_201_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_POST_201_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/1
		public static final int MSG_POST_201_FIELD_HOSTID_2 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_POST_201_FIELD_CONNECTIONID_20 = 0x00800004; //LongUnsigned/None/0
		public static final int MSG_POST_201_FIELD_DESTINATION_11 = 0x00000006; //IntegerUnsigned/None/3
		public static final int MSG_POST_201_FIELD_PATH_3 = 0x01400007; //UTF8/None/0
		public static final int MSG_POST_201_FIELD_HEADERS_7 = 0x01400009; //UTF8/None/1
		public static final int MSG_POST_201_FIELD_PAYLOAD_5 = 0x01c0000b; //ByteVector/None/2
		public static final int MSG_CLOSECONNECTION_104 = 0x00000013; //Group/OpenTempl/5
		public static final int MSG_CLOSECONNECTION_104_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_CLOSECONNECTION_104_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/1
		public static final int MSG_CLOSECONNECTION_104_FIELD_HOSTID_2 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_CLOSECONNECTION_104_FIELD_CONNECTIONID_20 = 0x00800004; //LongUnsigned/None/0
		public static final int MSG_HEAD_202 = 0x00000019; //Group/OpenTempl/8
		public static final int MSG_HEAD_202_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_HEAD_202_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/1
		public static final int MSG_HEAD_202_FIELD_HOSTID_2 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_HEAD_202_FIELD_CONNECTIONID_20 = 0x00800004; //LongUnsigned/None/0
		public static final int MSG_HEAD_202_FIELD_DESTINATION_11 = 0x00000006; //IntegerUnsigned/None/3
		public static final int MSG_HEAD_202_FIELD_PATH_3 = 0x01400007; //UTF8/None/0
		public static final int MSG_HEAD_202_FIELD_HEADERS_7 = 0x01400009; //UTF8/None/1
		public static final int MSG_DELETE_203 = 0x00000022; //Group/OpenTempl/8
		public static final int MSG_DELETE_203_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_DELETE_203_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/1
		public static final int MSG_DELETE_203_FIELD_HOSTID_2 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_DELETE_203_FIELD_CONNECTIONID_20 = 0x00800004; //LongUnsigned/None/0
		public static final int MSG_DELETE_203_FIELD_DESTINATION_11 = 0x00000006; //IntegerUnsigned/None/3
		public static final int MSG_DELETE_203_FIELD_PATH_3 = 0x01400007; //UTF8/None/0
		public static final int MSG_DELETE_203_FIELD_HEADERS_7 = 0x01400009; //UTF8/None/1
		public static final int MSG_PUT_204 = 0x0000002b; //Group/OpenTempl/9
		public static final int MSG_PUT_204_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_PUT_204_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/1
		public static final int MSG_PUT_204_FIELD_HOSTID_2 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_PUT_204_FIELD_CONNECTIONID_20 = 0x00800004; //LongUnsigned/None/0
		public static final int MSG_PUT_204_FIELD_DESTINATION_11 = 0x00000006; //IntegerUnsigned/None/3
		public static final int MSG_PUT_204_FIELD_PATH_3 = 0x01400007; //UTF8/None/0
		public static final int MSG_PUT_204_FIELD_HEADERS_7 = 0x01400009; //UTF8/None/1
		public static final int MSG_PUT_204_FIELD_PAYLOAD_5 = 0x01c0000b; //ByteVector/None/2
		public static final int MSG_PATCH_205 = 0x00000035; //Group/OpenTempl/9
		public static final int MSG_PATCH_205_FIELD_SESSION_10 = 0x00000001; //IntegerUnsigned/None/0
		public static final int MSG_PATCH_205_FIELD_PORT_1 = 0x00000002; //IntegerUnsigned/None/1
		public static final int MSG_PATCH_205_FIELD_HOSTID_2 = 0x00000003; //IntegerUnsigned/None/2
		public static final int MSG_PATCH_205_FIELD_CONNECTIONID_20 = 0x00800004; //LongUnsigned/None/0
		public static final int MSG_PATCH_205_FIELD_DESTINATION_11 = 0x00000006; //IntegerUnsigned/None/3
		public static final int MSG_PATCH_205_FIELD_PATH_3 = 0x01400007; //UTF8/None/0
		public static final int MSG_PATCH_205_FIELD_HEADERS_7 = 0x01400009; //UTF8/None/1
		public static final int MSG_PATCH_205_FIELD_PAYLOAD_5 = 0x01c0000b; //ByteVector/None/2

		public static void consume(Pipe<ClientHTTPRequestSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_GET_200:
		                consumeGET(input);
		            break;
		            case MSG_POST_201:
		                consumePOST(input);
		            break;
		            case MSG_CLOSECONNECTION_104:
		                consumeCLOSEConnection(input);
		            break;
		            case MSG_HEAD_202:
		                consumeHEAD(input);
		            break;
		            case MSG_DELETE_203:
		                consumeDELETE(input);
		            break;
		            case MSG_PUT_204:
		                consumePUT(input);
		            break;
		            case MSG_PATCH_205:
		                consumePATCH(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeGET(Pipe<ClientHTTPRequestSchema> input) {
		    int fieldSession = PipeReader.readInt(input,MSG_GET_200_FIELD_SESSION_10);
		    int fieldPort = PipeReader.readInt(input,MSG_GET_200_FIELD_PORT_1);
		    int fieldHostId = PipeReader.readInt(input,MSG_GET_200_FIELD_HOSTID_2);
		    long fieldConnectionId = PipeReader.readLong(input,MSG_GET_200_FIELD_CONNECTIONID_20);
		    int fieldDestination = PipeReader.readInt(input,MSG_GET_200_FIELD_DESTINATION_11);
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_GET_200_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_GET_200_FIELD_PATH_3)));
		    StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_GET_200_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_GET_200_FIELD_HEADERS_7)));
		}
		public static void consumePOST(Pipe<ClientHTTPRequestSchema> input) {
		    int fieldSession = PipeReader.readInt(input,MSG_POST_201_FIELD_SESSION_10);
		    int fieldPort = PipeReader.readInt(input,MSG_POST_201_FIELD_PORT_1);
		    int fieldHostId = PipeReader.readInt(input,MSG_POST_201_FIELD_HOSTID_2);
		    long fieldConnectionId = PipeReader.readLong(input,MSG_POST_201_FIELD_CONNECTIONID_20);
		    int fieldDestination = PipeReader.readInt(input,MSG_POST_201_FIELD_DESTINATION_11);
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_POST_201_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_POST_201_FIELD_PATH_3)));
		    StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_POST_201_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_POST_201_FIELD_HEADERS_7)));
		    DataInputBlobReader<ClientHTTPRequestSchema> fieldPayload = PipeReader.inputStream(input, MSG_POST_201_FIELD_PAYLOAD_5);
		}
		public static void consumeCLOSEConnection(Pipe<ClientHTTPRequestSchema> input) {
		    int fieldSession = PipeReader.readInt(input,MSG_CLOSECONNECTION_104_FIELD_SESSION_10);
		    int fieldPort = PipeReader.readInt(input,MSG_CLOSECONNECTION_104_FIELD_PORT_1);
		    int fieldHostId = PipeReader.readInt(input,MSG_CLOSECONNECTION_104_FIELD_HOSTID_2);
		    long fieldConnectionId = PipeReader.readLong(input,MSG_CLOSECONNECTION_104_FIELD_CONNECTIONID_20);
		}
		public static void consumeHEAD(Pipe<ClientHTTPRequestSchema> input) {
		    int fieldSession = PipeReader.readInt(input,MSG_HEAD_202_FIELD_SESSION_10);
		    int fieldPort = PipeReader.readInt(input,MSG_HEAD_202_FIELD_PORT_1);
		    int fieldHostId = PipeReader.readInt(input,MSG_HEAD_202_FIELD_HOSTID_2);
		    long fieldConnectionId = PipeReader.readLong(input,MSG_HEAD_202_FIELD_CONNECTIONID_20);
		    int fieldDestination = PipeReader.readInt(input,MSG_HEAD_202_FIELD_DESTINATION_11);
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_HEAD_202_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_HEAD_202_FIELD_PATH_3)));
		    StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_HEAD_202_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_HEAD_202_FIELD_HEADERS_7)));
		}
		public static void consumeDELETE(Pipe<ClientHTTPRequestSchema> input) {
		    int fieldSession = PipeReader.readInt(input,MSG_DELETE_203_FIELD_SESSION_10);
		    int fieldPort = PipeReader.readInt(input,MSG_DELETE_203_FIELD_PORT_1);
		    int fieldHostId = PipeReader.readInt(input,MSG_DELETE_203_FIELD_HOSTID_2);
		    long fieldConnectionId = PipeReader.readLong(input,MSG_DELETE_203_FIELD_CONNECTIONID_20);
		    int fieldDestination = PipeReader.readInt(input,MSG_DELETE_203_FIELD_DESTINATION_11);
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_DELETE_203_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_DELETE_203_FIELD_PATH_3)));
		    StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_DELETE_203_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_DELETE_203_FIELD_HEADERS_7)));
		}
		public static void consumePUT(Pipe<ClientHTTPRequestSchema> input) {
		    int fieldSession = PipeReader.readInt(input,MSG_PUT_204_FIELD_SESSION_10);
		    int fieldPort = PipeReader.readInt(input,MSG_PUT_204_FIELD_PORT_1);
		    int fieldHostId = PipeReader.readInt(input,MSG_PUT_204_FIELD_HOSTID_2);
		    long fieldConnectionId = PipeReader.readLong(input,MSG_PUT_204_FIELD_CONNECTIONID_20);
		    int fieldDestination = PipeReader.readInt(input,MSG_PUT_204_FIELD_DESTINATION_11);
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_PUT_204_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_PUT_204_FIELD_PATH_3)));
		    StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_PUT_204_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_PUT_204_FIELD_HEADERS_7)));
		    DataInputBlobReader<ClientHTTPRequestSchema> fieldPayload = PipeReader.inputStream(input, MSG_PUT_204_FIELD_PAYLOAD_5);
		}
		public static void consumePATCH(Pipe<ClientHTTPRequestSchema> input) {
		    int fieldSession = PipeReader.readInt(input,MSG_PATCH_205_FIELD_SESSION_10);
		    int fieldPort = PipeReader.readInt(input,MSG_PATCH_205_FIELD_PORT_1);
		    int fieldHostId = PipeReader.readInt(input,MSG_PATCH_205_FIELD_HOSTID_2);
		    long fieldConnectionId = PipeReader.readLong(input,MSG_PATCH_205_FIELD_CONNECTIONID_20);
		    int fieldDestination = PipeReader.readInt(input,MSG_PATCH_205_FIELD_DESTINATION_11);
		    StringBuilder fieldPath = PipeReader.readUTF8(input,MSG_PATCH_205_FIELD_PATH_3,new StringBuilder(PipeReader.readBytesLength(input,MSG_PATCH_205_FIELD_PATH_3)));
		    StringBuilder fieldHeaders = PipeReader.readUTF8(input,MSG_PATCH_205_FIELD_HEADERS_7,new StringBuilder(PipeReader.readBytesLength(input,MSG_PATCH_205_FIELD_HEADERS_7)));
		    DataInputBlobReader<ClientHTTPRequestSchema> fieldPayload = PipeReader.inputStream(input, MSG_PATCH_205_FIELD_PAYLOAD_5);
		}

		public static void publishGET(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, int fieldHostId, long fieldConnectionId, int fieldDestination, CharSequence fieldPath, CharSequence fieldHeaders) {
		        PipeWriter.presumeWriteFragment(output, MSG_GET_200);
		        PipeWriter.writeInt(output,MSG_GET_200_FIELD_SESSION_10, fieldSession);
		        PipeWriter.writeInt(output,MSG_GET_200_FIELD_PORT_1, fieldPort);
		        PipeWriter.writeInt(output,MSG_GET_200_FIELD_HOSTID_2, fieldHostId);
		        PipeWriter.writeLong(output,MSG_GET_200_FIELD_CONNECTIONID_20, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_GET_200_FIELD_DESTINATION_11, fieldDestination);
		        PipeWriter.writeUTF8(output,MSG_GET_200_FIELD_PATH_3, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_GET_200_FIELD_HEADERS_7, fieldHeaders);
		        PipeWriter.publishWrites(output);
		}
		public static void publishPOST(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, int fieldHostId, long fieldConnectionId, int fieldDestination, CharSequence fieldPath, CharSequence fieldHeaders, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_POST_201);
		        PipeWriter.writeInt(output,MSG_POST_201_FIELD_SESSION_10, fieldSession);
		        PipeWriter.writeInt(output,MSG_POST_201_FIELD_PORT_1, fieldPort);
		        PipeWriter.writeInt(output,MSG_POST_201_FIELD_HOSTID_2, fieldHostId);
		        PipeWriter.writeLong(output,MSG_POST_201_FIELD_CONNECTIONID_20, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_POST_201_FIELD_DESTINATION_11, fieldDestination);
		        PipeWriter.writeUTF8(output,MSG_POST_201_FIELD_PATH_3, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_POST_201_FIELD_HEADERS_7, fieldHeaders);
		        PipeWriter.writeBytes(output,MSG_POST_201_FIELD_PAYLOAD_5, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishCLOSEConnection(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, int fieldHostId, long fieldConnectionId) {
		        PipeWriter.presumeWriteFragment(output, MSG_CLOSECONNECTION_104);
		        PipeWriter.writeInt(output,MSG_CLOSECONNECTION_104_FIELD_SESSION_10, fieldSession);
		        PipeWriter.writeInt(output,MSG_CLOSECONNECTION_104_FIELD_PORT_1, fieldPort);
		        PipeWriter.writeInt(output,MSG_CLOSECONNECTION_104_FIELD_HOSTID_2, fieldHostId);
		        PipeWriter.writeLong(output,MSG_CLOSECONNECTION_104_FIELD_CONNECTIONID_20, fieldConnectionId);
		        PipeWriter.publishWrites(output);
		}
		public static void publishHEAD(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, int fieldHostId, long fieldConnectionId, int fieldDestination, CharSequence fieldPath, CharSequence fieldHeaders) {
		        PipeWriter.presumeWriteFragment(output, MSG_HEAD_202);
		        PipeWriter.writeInt(output,MSG_HEAD_202_FIELD_SESSION_10, fieldSession);
		        PipeWriter.writeInt(output,MSG_HEAD_202_FIELD_PORT_1, fieldPort);
		        PipeWriter.writeInt(output,MSG_HEAD_202_FIELD_HOSTID_2, fieldHostId);
		        PipeWriter.writeLong(output,MSG_HEAD_202_FIELD_CONNECTIONID_20, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_HEAD_202_FIELD_DESTINATION_11, fieldDestination);
		        PipeWriter.writeUTF8(output,MSG_HEAD_202_FIELD_PATH_3, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_HEAD_202_FIELD_HEADERS_7, fieldHeaders);
		        PipeWriter.publishWrites(output);
		}
		public static void publishDELETE(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, int fieldHostId, long fieldConnectionId, int fieldDestination, CharSequence fieldPath, CharSequence fieldHeaders) {
		        PipeWriter.presumeWriteFragment(output, MSG_DELETE_203);
		        PipeWriter.writeInt(output,MSG_DELETE_203_FIELD_SESSION_10, fieldSession);
		        PipeWriter.writeInt(output,MSG_DELETE_203_FIELD_PORT_1, fieldPort);
		        PipeWriter.writeInt(output,MSG_DELETE_203_FIELD_HOSTID_2, fieldHostId);
		        PipeWriter.writeLong(output,MSG_DELETE_203_FIELD_CONNECTIONID_20, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_DELETE_203_FIELD_DESTINATION_11, fieldDestination);
		        PipeWriter.writeUTF8(output,MSG_DELETE_203_FIELD_PATH_3, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_DELETE_203_FIELD_HEADERS_7, fieldHeaders);
		        PipeWriter.publishWrites(output);
		}
		public static void publishPUT(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, int fieldHostId, long fieldConnectionId, int fieldDestination, CharSequence fieldPath, CharSequence fieldHeaders, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_PUT_204);
		        PipeWriter.writeInt(output,MSG_PUT_204_FIELD_SESSION_10, fieldSession);
		        PipeWriter.writeInt(output,MSG_PUT_204_FIELD_PORT_1, fieldPort);
		        PipeWriter.writeInt(output,MSG_PUT_204_FIELD_HOSTID_2, fieldHostId);
		        PipeWriter.writeLong(output,MSG_PUT_204_FIELD_CONNECTIONID_20, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_PUT_204_FIELD_DESTINATION_11, fieldDestination);
		        PipeWriter.writeUTF8(output,MSG_PUT_204_FIELD_PATH_3, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_PUT_204_FIELD_HEADERS_7, fieldHeaders);
		        PipeWriter.writeBytes(output,MSG_PUT_204_FIELD_PAYLOAD_5, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		}
		public static void publishPATCH(Pipe<ClientHTTPRequestSchema> output, int fieldSession, int fieldPort, int fieldHostId, long fieldConnectionId, int fieldDestination, CharSequence fieldPath, CharSequence fieldHeaders, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		        PipeWriter.presumeWriteFragment(output, MSG_PATCH_205);
		        PipeWriter.writeInt(output,MSG_PATCH_205_FIELD_SESSION_10, fieldSession);
		        PipeWriter.writeInt(output,MSG_PATCH_205_FIELD_PORT_1, fieldPort);
		        PipeWriter.writeInt(output,MSG_PATCH_205_FIELD_HOSTID_2, fieldHostId);
		        PipeWriter.writeLong(output,MSG_PATCH_205_FIELD_CONNECTIONID_20, fieldConnectionId);
		        PipeWriter.writeInt(output,MSG_PATCH_205_FIELD_DESTINATION_11, fieldDestination);
		        PipeWriter.writeUTF8(output,MSG_PATCH_205_FIELD_PATH_3, fieldPath);
		        PipeWriter.writeUTF8(output,MSG_PATCH_205_FIELD_HEADERS_7, fieldHeaders);
		        PipeWriter.writeBytes(output,MSG_PATCH_205_FIELD_PAYLOAD_5, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		}

}
