package com.ociweb.pronghorn.stage.file.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Defines a request to the block manager.
 * @author Nathan Tippy
 */
public class BlockManagerRequestSchema extends MessageSchema<BlockManagerRequestSchema> {


	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400002,0x88000000,0xc0200002,0xc0400003,0x98000000,0xb8000000,0xc0200003,0xc0400003,0x98000000,0xb8000001,0xc0200003,0xc0400002,0x98000000,0xc0200002,0xc0400001,0xc0200001,0xc0400002,0xb8000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400002,0xb8000000,0xc0200002},
		    (short)0,
		    new String[]{"SubRequest","bits",null,"SubRelease","Id","Route",null,"SubWrite","Id","Payload",
		    null,"SubRead","id",null,"ReadHeader",null,"BlockMount","Route",null,"BlockRequest",
		    "bits",null,"BlockRelease","Route",null},
		    new long[]{1, 10, 0, 2, 11, 14, 0, 3, 11, 12, 0, 4, 11, 0, 8, 0, 5, 14, 0, 6, 10, 0, 7, 14, 0},
		    new String[]{"global",null,null,"global",null,null,null,"global",null,null,null,"global",null,
		    null,"global",null,"global",null,null,"global",null,null,"global",null,null},
		    "BlockManagerRequest.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected BlockManagerRequestSchema() { 
		    super(FROM);
		}

		public static final BlockManagerRequestSchema instance = new BlockManagerRequestSchema();

		public static final int MSG_SUBREQUEST_1 = 0x00000000;
		public static final int MSG_SUBREQUEST_1_FIELD_BITS_10 = 0x00400001;
		public static final int MSG_SUBRELEASE_2 = 0x00000003;
		public static final int MSG_SUBRELEASE_2_FIELD_ID_11 = 0x00c00001;
		public static final int MSG_SUBRELEASE_2_FIELD_ROUTE_14 = 0x01c00003;
		public static final int MSG_SUBWRITE_3 = 0x00000007;
		public static final int MSG_SUBWRITE_3_FIELD_ID_11 = 0x00c00001;
		public static final int MSG_SUBWRITE_3_FIELD_PAYLOAD_12 = 0x01c00003;
		public static final int MSG_SUBREAD_4 = 0x0000000b;
		public static final int MSG_SUBREAD_4_FIELD_ID_11 = 0x00c00001;
		public static final int MSG_READHEADER_8 = 0x0000000e;
		public static final int MSG_BLOCKMOUNT_5 = 0x00000010;
		public static final int MSG_BLOCKMOUNT_5_FIELD_ROUTE_14 = 0x01c00001;
		public static final int MSG_BLOCKREQUEST_6 = 0x00000013;
		public static final int MSG_BLOCKREQUEST_6_FIELD_BITS_10 = 0x00400001;
		public static final int MSG_BLOCKRELEASE_7 = 0x00000016;
		public static final int MSG_BLOCKRELEASE_7_FIELD_ROUTE_14 = 0x01c00001;


		public static void consume(Pipe<BlockManagerRequestSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_SUBREQUEST_1:
		                consumeSubRequest(input);
		            break;
		            case MSG_SUBRELEASE_2:
		                consumeSubRelease(input);
		            break;
		            case MSG_SUBWRITE_3:
		                consumeSubWrite(input);
		            break;
		            case MSG_SUBREAD_4:
		                consumeSubRead(input);
		            break;
		            case MSG_READHEADER_8:
		                consumeReadHeader(input);
		            break;
		            case MSG_BLOCKMOUNT_5:
		                consumeBlockMount(input);
		            break;
		            case MSG_BLOCKREQUEST_6:
		                consumeBlockRequest(input);
		            break;
		            case MSG_BLOCKRELEASE_7:
		                consumeBlockRelease(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeSubRequest(Pipe<BlockManagerRequestSchema> input) {
		    int fieldbits = PipeReader.readInt(input,MSG_SUBREQUEST_1_FIELD_BITS_10);
		}
		public static void consumeSubRelease(Pipe<BlockManagerRequestSchema> input) {
		    long fieldId = PipeReader.readLong(input,MSG_SUBRELEASE_2_FIELD_ID_11);
		    ByteBuffer fieldRoute = PipeReader.readBytes(input,MSG_SUBRELEASE_2_FIELD_ROUTE_14,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_SUBRELEASE_2_FIELD_ROUTE_14)));
		}
		public static void consumeSubWrite(Pipe<BlockManagerRequestSchema> input) {
		    long fieldId = PipeReader.readLong(input,MSG_SUBWRITE_3_FIELD_ID_11);
		    ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_SUBWRITE_3_FIELD_PAYLOAD_12,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_SUBWRITE_3_FIELD_PAYLOAD_12)));
		}
		public static void consumeSubRead(Pipe<BlockManagerRequestSchema> input) {
		    long fieldid = PipeReader.readLong(input,MSG_SUBREAD_4_FIELD_ID_11);
		}
		public static void consumeReadHeader(Pipe<BlockManagerRequestSchema> input) {
		}
		public static void consumeBlockMount(Pipe<BlockManagerRequestSchema> input) {
		    ByteBuffer fieldRoute = PipeReader.readBytes(input,MSG_BLOCKMOUNT_5_FIELD_ROUTE_14,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_BLOCKMOUNT_5_FIELD_ROUTE_14)));
		}
		public static void consumeBlockRequest(Pipe<BlockManagerRequestSchema> input) {
		    int fieldbits = PipeReader.readInt(input,MSG_BLOCKREQUEST_6_FIELD_BITS_10);
		}
		public static void consumeBlockRelease(Pipe<BlockManagerRequestSchema> input) {
		    ByteBuffer fieldRoute = PipeReader.readBytes(input,MSG_BLOCKRELEASE_7_FIELD_ROUTE_14,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_BLOCKRELEASE_7_FIELD_ROUTE_14)));
		}

		public static boolean publishSubRequest(Pipe<BlockManagerRequestSchema> output, int fieldbits) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBREQUEST_1)) {
		        PipeWriter.writeInt(output,MSG_SUBREQUEST_1_FIELD_BITS_10, fieldbits);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishSubRelease(Pipe<BlockManagerRequestSchema> output, long fieldId, byte[] fieldRouteBacking, int fieldRoutePosition, int fieldRouteLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBRELEASE_2)) {
		        PipeWriter.writeLong(output,MSG_SUBRELEASE_2_FIELD_ID_11, fieldId);
		        PipeWriter.writeBytes(output,MSG_SUBRELEASE_2_FIELD_ROUTE_14, fieldRouteBacking, fieldRoutePosition, fieldRouteLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishSubWrite(Pipe<BlockManagerRequestSchema> output, long fieldId, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBWRITE_3)) {
		        PipeWriter.writeLong(output,MSG_SUBWRITE_3_FIELD_ID_11, fieldId);
		        PipeWriter.writeBytes(output,MSG_SUBWRITE_3_FIELD_PAYLOAD_12, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishSubRead(Pipe<BlockManagerRequestSchema> output, long fieldid) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBREAD_4)) {
		        PipeWriter.writeLong(output,MSG_SUBREAD_4_FIELD_ID_11, fieldid);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishReadHeader(Pipe<BlockManagerRequestSchema> output) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_READHEADER_8)) {
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishBlockMount(Pipe<BlockManagerRequestSchema> output, byte[] fieldRouteBacking, int fieldRoutePosition, int fieldRouteLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_BLOCKMOUNT_5)) {
		        PipeWriter.writeBytes(output,MSG_BLOCKMOUNT_5_FIELD_ROUTE_14, fieldRouteBacking, fieldRoutePosition, fieldRouteLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishBlockRequest(Pipe<BlockManagerRequestSchema> output, int fieldbits) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_BLOCKREQUEST_6)) {
		        PipeWriter.writeInt(output,MSG_BLOCKREQUEST_6_FIELD_BITS_10, fieldbits);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishBlockRelease(Pipe<BlockManagerRequestSchema> output, byte[] fieldRouteBacking, int fieldRoutePosition, int fieldRouteLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_BLOCKRELEASE_7)) {
		        PipeWriter.writeBytes(output,MSG_BLOCKRELEASE_7_FIELD_ROUTE_14, fieldRouteBacking, fieldRoutePosition, fieldRouteLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
}
