package com.ociweb.pronghorn.stage.file.schema;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

/**
 * Defines block responses.
 * @author Nathan Tippy
 */
public class BlockManagerResponseSchema extends MessageSchema<BlockManagerResponseSchema> {


	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400003,0x98000000,0xb8000000,0xc0200003,0xc0400002,0x88000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400002,0xb8000001,0xc0200002,0xc0400003,0x98000000,0xb8000001,0xc0200003,0xc0400002,0x88000000,0xc0200002,0xc0400002,0xb8000000,0xc0200002,0xc0400002,0x88000000,0xc0200002,0xc0400002,0x88000000,0xc0200002},
		    (short)0,
		    new String[]{"SubAllocated","Id","Route",null,"SubAllocatedFailure","Status",null,"SubReleased",
		    "Status",null,"SubWritten","Status",null,"SubRead","Payload",null,"ReadHeader","Id",
		    "Payload",null,"BlockMounted","Status",null,"BlockAllocated","Route",null,"BlockAllocatedFailure",
		    "Status",null,"BlockReleased","Status",null},
		    new long[]{1, 11, 14, 0, 21, 12, 0, 2, 12, 0, 3, 12, 0, 4, 13, 0, 8, 11, 13, 0, 5, 12, 0, 6, 14, 0, 26, 12, 0, 7, 12, 0},
		    new String[]{"global",null,null,null,"global",null,null,"global",null,null,"global",null,null,
		    "global",null,null,"global",null,null,null,"global",null,null,"global",null,null,
		    "global",null,null,"global",null,null},
		    "BlockManagerResponse.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});

		protected BlockManagerResponseSchema() { 
		    super(FROM);
		}

		public static final BlockManagerResponseSchema instance = new BlockManagerResponseSchema();
		
		public static final int MSG_SUBALLOCATED_1 = 0x00000000;
		public static final int MSG_SUBALLOCATED_1_FIELD_ID_11 = 0x00c00001;
		public static final int MSG_SUBALLOCATED_1_FIELD_ROUTE_14 = 0x01c00003;
		public static final int MSG_SUBALLOCATEDFAILURE_21 = 0x00000004;
		public static final int MSG_SUBALLOCATEDFAILURE_21_FIELD_STATUS_12 = 0x00400001;
		public static final int MSG_SUBRELEASED_2 = 0x00000007;
		public static final int MSG_SUBRELEASED_2_FIELD_STATUS_12 = 0x00400001;
		public static final int MSG_SUBWRITTEN_3 = 0x0000000a;
		public static final int MSG_SUBWRITTEN_3_FIELD_STATUS_12 = 0x00400001;
		public static final int MSG_SUBREAD_4 = 0x0000000d;
		public static final int MSG_SUBREAD_4_FIELD_PAYLOAD_13 = 0x01c00001;
		public static final int MSG_READHEADER_8 = 0x00000010;
		public static final int MSG_READHEADER_8_FIELD_ID_11 = 0x00c00001;
		public static final int MSG_READHEADER_8_FIELD_PAYLOAD_13 = 0x01c00003;
		public static final int MSG_BLOCKMOUNTED_5 = 0x00000014;
		public static final int MSG_BLOCKMOUNTED_5_FIELD_STATUS_12 = 0x00400001;
		public static final int MSG_BLOCKALLOCATED_6 = 0x00000017;
		public static final int MSG_BLOCKALLOCATED_6_FIELD_ROUTE_14 = 0x01c00001;
		public static final int MSG_BLOCKALLOCATEDFAILURE_26 = 0x0000001a;
		public static final int MSG_BLOCKALLOCATEDFAILURE_26_FIELD_STATUS_12 = 0x00400001;
		public static final int MSG_BLOCKRELEASED_7 = 0x0000001d;
		public static final int MSG_BLOCKRELEASED_7_FIELD_STATUS_12 = 0x00400001;


		public static void consume(Pipe<BlockManagerResponseSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_SUBALLOCATED_1:
		                consumeSubAllocated(input);
		            break;
		            case MSG_SUBALLOCATEDFAILURE_21:
		                consumeSubAllocatedFailure(input);
		            break;
		            case MSG_SUBRELEASED_2:
		                consumeSubReleased(input);
		            break;
		            case MSG_SUBWRITTEN_3:
		                consumeSubWritten(input);
		            break;
		            case MSG_SUBREAD_4:
		                consumeSubRead(input);
		            break;
		            case MSG_READHEADER_8:
		                consumeReadHeader(input);
		            break;
		            case MSG_BLOCKMOUNTED_5:
		                consumeBlockMounted(input);
		            break;
		            case MSG_BLOCKALLOCATED_6:
		                consumeBlockAllocated(input);
		            break;
		            case MSG_BLOCKALLOCATEDFAILURE_26:
		                consumeBlockAllocatedFailure(input);
		            break;
		            case MSG_BLOCKRELEASED_7:
		                consumeBlockReleased(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeSubAllocated(Pipe<BlockManagerResponseSchema> input) {
		    long fieldId = PipeReader.readLong(input,MSG_SUBALLOCATED_1_FIELD_ID_11);
		    ByteBuffer fieldRoute = PipeReader.readBytes(input,MSG_SUBALLOCATED_1_FIELD_ROUTE_14,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_SUBALLOCATED_1_FIELD_ROUTE_14)));
		}
		public static void consumeSubAllocatedFailure(Pipe<BlockManagerResponseSchema> input) {
		    int fieldStatus = PipeReader.readInt(input,MSG_SUBALLOCATEDFAILURE_21_FIELD_STATUS_12);
		}
		public static void consumeSubReleased(Pipe<BlockManagerResponseSchema> input) {
		    int fieldStatus = PipeReader.readInt(input,MSG_SUBRELEASED_2_FIELD_STATUS_12);
		}
		public static void consumeSubWritten(Pipe<BlockManagerResponseSchema> input) {
		    int fieldStatus = PipeReader.readInt(input,MSG_SUBWRITTEN_3_FIELD_STATUS_12);
		}
		public static void consumeSubRead(Pipe<BlockManagerResponseSchema> input) {
		    ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_SUBREAD_4_FIELD_PAYLOAD_13,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_SUBREAD_4_FIELD_PAYLOAD_13)));
		}
		public static void consumeReadHeader(Pipe<BlockManagerResponseSchema> input) {
		    long fieldId = PipeReader.readLong(input,MSG_READHEADER_8_FIELD_ID_11);
		    ByteBuffer fieldPayload = PipeReader.readBytes(input,MSG_READHEADER_8_FIELD_PAYLOAD_13,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_READHEADER_8_FIELD_PAYLOAD_13)));
		}
		public static void consumeBlockMounted(Pipe<BlockManagerResponseSchema> input) {
		    int fieldStatus = PipeReader.readInt(input,MSG_BLOCKMOUNTED_5_FIELD_STATUS_12);
		}
		public static void consumeBlockAllocated(Pipe<BlockManagerResponseSchema> input) {
		    ByteBuffer fieldRoute = PipeReader.readBytes(input,MSG_BLOCKALLOCATED_6_FIELD_ROUTE_14,ByteBuffer.allocate(PipeReader.readBytesLength(input,MSG_BLOCKALLOCATED_6_FIELD_ROUTE_14)));
		}
		public static void consumeBlockAllocatedFailure(Pipe<BlockManagerResponseSchema> input) {
		    int fieldStatus = PipeReader.readInt(input,MSG_BLOCKALLOCATEDFAILURE_26_FIELD_STATUS_12);
		}
		public static void consumeBlockReleased(Pipe<BlockManagerResponseSchema> input) {
		    int fieldStatus = PipeReader.readInt(input,MSG_BLOCKRELEASED_7_FIELD_STATUS_12);
		}

		public static boolean publishSubAllocated(Pipe<BlockManagerResponseSchema> output, long fieldId, byte[] fieldRouteBacking, int fieldRoutePosition, int fieldRouteLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBALLOCATED_1)) {
		        PipeWriter.writeLong(output,MSG_SUBALLOCATED_1_FIELD_ID_11, fieldId);
		        PipeWriter.writeBytes(output,MSG_SUBALLOCATED_1_FIELD_ROUTE_14, fieldRouteBacking, fieldRoutePosition, fieldRouteLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishSubAllocatedFailure(Pipe<BlockManagerResponseSchema> output, int fieldStatus) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBALLOCATEDFAILURE_21)) {
		        PipeWriter.writeInt(output,MSG_SUBALLOCATEDFAILURE_21_FIELD_STATUS_12, fieldStatus);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishSubReleased(Pipe<BlockManagerResponseSchema> output, int fieldStatus) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBRELEASED_2)) {
		        PipeWriter.writeInt(output,MSG_SUBRELEASED_2_FIELD_STATUS_12, fieldStatus);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishSubWritten(Pipe<BlockManagerResponseSchema> output, int fieldStatus) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBWRITTEN_3)) {
		        PipeWriter.writeInt(output,MSG_SUBWRITTEN_3_FIELD_STATUS_12, fieldStatus);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishSubRead(Pipe<BlockManagerResponseSchema> output, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_SUBREAD_4)) {
		        PipeWriter.writeBytes(output,MSG_SUBREAD_4_FIELD_PAYLOAD_13, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishReadHeader(Pipe<BlockManagerResponseSchema> output, long fieldId, byte[] fieldPayloadBacking, int fieldPayloadPosition, int fieldPayloadLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_READHEADER_8)) {
		        PipeWriter.writeLong(output,MSG_READHEADER_8_FIELD_ID_11, fieldId);
		        PipeWriter.writeBytes(output,MSG_READHEADER_8_FIELD_PAYLOAD_13, fieldPayloadBacking, fieldPayloadPosition, fieldPayloadLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishBlockMounted(Pipe<BlockManagerResponseSchema> output, int fieldStatus) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_BLOCKMOUNTED_5)) {
		        PipeWriter.writeInt(output,MSG_BLOCKMOUNTED_5_FIELD_STATUS_12, fieldStatus);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishBlockAllocated(Pipe<BlockManagerResponseSchema> output, byte[] fieldRouteBacking, int fieldRoutePosition, int fieldRouteLength) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_BLOCKALLOCATED_6)) {
		        PipeWriter.writeBytes(output,MSG_BLOCKALLOCATED_6_FIELD_ROUTE_14, fieldRouteBacking, fieldRoutePosition, fieldRouteLength);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishBlockAllocatedFailure(Pipe<BlockManagerResponseSchema> output, int fieldStatus) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_BLOCKALLOCATEDFAILURE_26)) {
		        PipeWriter.writeInt(output,MSG_BLOCKALLOCATEDFAILURE_26_FIELD_STATUS_12, fieldStatus);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
		public static boolean publishBlockReleased(Pipe<BlockManagerResponseSchema> output, int fieldStatus) {
		    boolean result = false;
		    if (PipeWriter.tryWriteFragment(output, MSG_BLOCKRELEASED_7)) {
		        PipeWriter.writeInt(output,MSG_BLOCKRELEASED_7_FIELD_STATUS_12, fieldStatus);
		        PipeWriter.publishWrites(output);
		        result = true;
		    }
		    return result;
		}
}
