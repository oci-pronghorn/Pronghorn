package com.ociweb.jpgRaster;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class JPGSchema extends MessageSchema<JPGSchema> {
	protected JPGSchema(FieldReferenceOffsetManager from) { 
	    super(from);
	}

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
	    new int[]{0xc0400004,0x88000000,0x88000001,0xa0000000,0xc0200004,0xc0400003,0x88000002,0xb8000001,0xc0200003,0xc0400004,0x88000003,0x88000004,0xb8000002,0xc0200004,0xc0400003,0x88000005,0xb8000003,0xc0200003,0xc0400004,0xb8000004,0xb8000005,0xb8000006,0xc0200004,0xc0400002,0xb8000007,0xc0200002},
	    (short)0,
	    new String[]{"HeaderMessage","height","width","filename",null,"CompressedMessageData","length",
	    "data",null,"HuffmanTableMessage","tableId","length","table",null,"QuantizationTableMessage",
	    "tableId","table",null,"MCUMessage","y","cb","cr",null,"PixelRowMessage","pixels",
	    null},
	    new long[]{1, 101, 201, 301, 0, 2, 102, 202, 0, 3, 103, 203, 303, 0, 4, 104, 204, 0, 5, 105, 205, 305, 0, 6, 106, 0},
	    new String[]{"global",null,null,null,null,"global",null,null,null,"global",null,null,null,null,
	    "global",null,null,null,"global",null,null,null,null,"global",null,null},
	    "JPGSchema.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});

	protected JPGSchema() { 
	    super(FROM);
	}

	public static final JPGSchema instance = new JPGSchema();

	public static final int MSG_HEADERMESSAGE_1 = 0x00000000; //Group/OpenTempl/4
	public static final int MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101 = 0x00400001; //IntegerSigned/None/0
	public static final int MSG_HEADERMESSAGE_1_FIELD_WIDTH_201 = 0x00400002; //IntegerSigned/None/1
	public static final int MSG_HEADERMESSAGE_1_FIELD_FILENAME_301 = 0x01000003; //ASCII/None/0
	public static final int MSG_COMPRESSEDMESSAGEDATA_2 = 0x00000005; //Group/OpenTempl/3
	public static final int MSG_COMPRESSEDMESSAGEDATA_2_FIELD_LENGTH_102 = 0x00400001; //IntegerSigned/None/2
	public static final int MSG_COMPRESSEDMESSAGEDATA_2_FIELD_DATA_202 = 0x01c00002; //ByteVector/None/1
	public static final int MSG_HUFFMANTABLEMESSAGE_3 = 0x00000009; //Group/OpenTempl/4
	public static final int MSG_HUFFMANTABLEMESSAGE_3_FIELD_TABLEID_103 = 0x00400001; //IntegerSigned/None/3
	public static final int MSG_HUFFMANTABLEMESSAGE_3_FIELD_LENGTH_203 = 0x00400002; //IntegerSigned/None/4
	public static final int MSG_HUFFMANTABLEMESSAGE_3_FIELD_TABLE_303 = 0x01c00003; //ByteVector/None/2
	public static final int MSG_QUANTIZATIONTABLEMESSAGE_4 = 0x0000000e; //Group/OpenTempl/3
	public static final int MSG_QUANTIZATIONTABLEMESSAGE_4_FIELD_TABLEID_104 = 0x00400001; //IntegerSigned/None/5
	public static final int MSG_QUANTIZATIONTABLEMESSAGE_4_FIELD_TABLE_204 = 0x01c00002; //ByteVector/None/3
	public static final int MSG_MCUMESSAGE_5 = 0x00000012; //Group/OpenTempl/4
	public static final int MSG_MCUMESSAGE_5_FIELD_Y_105 = 0x01c00001; //ByteVector/None/4
	public static final int MSG_MCUMESSAGE_5_FIELD_CB_205 = 0x01c00003; //ByteVector/None/5
	public static final int MSG_MCUMESSAGE_5_FIELD_CR_305 = 0x01c00005; //ByteVector/None/6
	public static final int MSG_PIXELROWMESSAGE_6 = 0x00000017; //Group/OpenTempl/2
	public static final int MSG_PIXELROWMESSAGE_6_FIELD_PIXELS_106 = 0x01c00001; //ByteVector/None/7

	public static void consume(Pipe<JPGSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_HEADERMESSAGE_1:
	                consumeHeaderMessage(input);
	            break;
	            case MSG_COMPRESSEDMESSAGEDATA_2:
	                consumeCompressedMessageData(input);
	            break;
	            case MSG_HUFFMANTABLEMESSAGE_3:
	                consumeHuffmanTableMessage(input);
	            break;
	            case MSG_QUANTIZATIONTABLEMESSAGE_4:
	                consumeQuantizationTableMessage(input);
	            break;
	            case MSG_MCUMESSAGE_5:
	                consumeMCUMessage(input);
	            break;
	            case MSG_PIXELROWMESSAGE_6:
	                consumePixelRowMessage(input);
	            break;
	            case -1:
	               //requestShutdown();
	            break;
	        }
	        PipeReader.releaseReadLock(input);
	    }
	}

	public static void consumeHeaderMessage(Pipe<JPGSchema> input) {
	    int fieldheight = PipeReader.readInt(input,MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
	    int fieldwidth = PipeReader.readInt(input,MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
	    StringBuilder fieldfilename = PipeReader.readUTF8(input,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301,new StringBuilder(PipeReader.readBytesLength(input,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301)));
	}
	public static void consumeCompressedMessageData(Pipe<JPGSchema> input) {
	    int fieldlength = PipeReader.readInt(input,MSG_COMPRESSEDMESSAGEDATA_2_FIELD_LENGTH_102);
	    DataInputBlobReader<JPGSchema> fielddata = PipeReader.inputStream(input, MSG_COMPRESSEDMESSAGEDATA_2_FIELD_DATA_202);
	}
	public static void consumeHuffmanTableMessage(Pipe<JPGSchema> input) {
	    int fieldtableId = PipeReader.readInt(input,MSG_HUFFMANTABLEMESSAGE_3_FIELD_TABLEID_103);
	    int fieldlength = PipeReader.readInt(input,MSG_HUFFMANTABLEMESSAGE_3_FIELD_LENGTH_203);
	    DataInputBlobReader<JPGSchema> fieldtable = PipeReader.inputStream(input, MSG_HUFFMANTABLEMESSAGE_3_FIELD_TABLE_303);
	}
	public static void consumeQuantizationTableMessage(Pipe<JPGSchema> input) {
	    int fieldtableId = PipeReader.readInt(input,MSG_QUANTIZATIONTABLEMESSAGE_4_FIELD_TABLEID_104);
	    DataInputBlobReader<JPGSchema> fieldtable = PipeReader.inputStream(input, MSG_QUANTIZATIONTABLEMESSAGE_4_FIELD_TABLE_204);
	}
	public static void consumeMCUMessage(Pipe<JPGSchema> input) {
	    DataInputBlobReader<JPGSchema> fieldy = PipeReader.inputStream(input, MSG_MCUMESSAGE_5_FIELD_Y_105);
	    DataInputBlobReader<JPGSchema> fieldcb = PipeReader.inputStream(input, MSG_MCUMESSAGE_5_FIELD_CB_205);
	    DataInputBlobReader<JPGSchema> fieldcr = PipeReader.inputStream(input, MSG_MCUMESSAGE_5_FIELD_CR_305);
	}
	public static void consumePixelRowMessage(Pipe<JPGSchema> input) {
	    DataInputBlobReader<JPGSchema> fieldpixels = PipeReader.inputStream(input, MSG_PIXELROWMESSAGE_6_FIELD_PIXELS_106);
	}

	public static void publishHeaderMessage(Pipe<JPGSchema> output, int fieldheight, int fieldwidth, CharSequence fieldfilename) {
	        PipeWriter.presumeWriteFragment(output, MSG_HEADERMESSAGE_1);
	        PipeWriter.writeInt(output,MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, fieldheight);
	        PipeWriter.writeInt(output,MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, fieldwidth);
	        PipeWriter.writeUTF8(output,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, fieldfilename);
	        PipeWriter.publishWrites(output);
	}
	public static void publishCompressedMessageData(Pipe<JPGSchema> output, int fieldlength, byte[] fielddataBacking, int fielddataPosition, int fielddataLength) {
	        PipeWriter.presumeWriteFragment(output, MSG_COMPRESSEDMESSAGEDATA_2);
	        PipeWriter.writeInt(output,MSG_COMPRESSEDMESSAGEDATA_2_FIELD_LENGTH_102, fieldlength);
	        PipeWriter.writeBytes(output,MSG_COMPRESSEDMESSAGEDATA_2_FIELD_DATA_202, fielddataBacking, fielddataPosition, fielddataLength);
	        PipeWriter.publishWrites(output);
	}
	public static void publishHuffmanTableMessage(Pipe<JPGSchema> output, int fieldtableId, int fieldlength, byte[] fieldtableBacking, int fieldtablePosition, int fieldtableLength) {
	        PipeWriter.presumeWriteFragment(output, MSG_HUFFMANTABLEMESSAGE_3);
	        PipeWriter.writeInt(output,MSG_HUFFMANTABLEMESSAGE_3_FIELD_TABLEID_103, fieldtableId);
	        PipeWriter.writeInt(output,MSG_HUFFMANTABLEMESSAGE_3_FIELD_LENGTH_203, fieldlength);
	        PipeWriter.writeBytes(output,MSG_HUFFMANTABLEMESSAGE_3_FIELD_TABLE_303, fieldtableBacking, fieldtablePosition, fieldtableLength);
	        PipeWriter.publishWrites(output);
	}
	public static void publishQuantizationTableMessage(Pipe<JPGSchema> output, int fieldtableId, byte[] fieldtableBacking, int fieldtablePosition, int fieldtableLength) {
	        PipeWriter.presumeWriteFragment(output, MSG_QUANTIZATIONTABLEMESSAGE_4);
	        PipeWriter.writeInt(output,MSG_QUANTIZATIONTABLEMESSAGE_4_FIELD_TABLEID_104, fieldtableId);
	        PipeWriter.writeBytes(output,MSG_QUANTIZATIONTABLEMESSAGE_4_FIELD_TABLE_204, fieldtableBacking, fieldtablePosition, fieldtableLength);
	        PipeWriter.publishWrites(output);
	}
	public static void publishMCUMessage(Pipe<JPGSchema> output, byte[] fieldyBacking, int fieldyPosition, int fieldyLength, byte[] fieldcbBacking, int fieldcbPosition, int fieldcbLength, byte[] fieldcrBacking, int fieldcrPosition, int fieldcrLength) {
	        PipeWriter.presumeWriteFragment(output, MSG_MCUMESSAGE_5);
	        PipeWriter.writeBytes(output,MSG_MCUMESSAGE_5_FIELD_Y_105, fieldyBacking, fieldyPosition, fieldyLength);
	        PipeWriter.writeBytes(output,MSG_MCUMESSAGE_5_FIELD_CB_205, fieldcbBacking, fieldcbPosition, fieldcbLength);
	        PipeWriter.writeBytes(output,MSG_MCUMESSAGE_5_FIELD_CR_305, fieldcrBacking, fieldcrPosition, fieldcrLength);
	        PipeWriter.publishWrites(output);
	}
	public static void publishPixelRowMessage(Pipe<JPGSchema> output, byte[] fieldpixelsBacking, int fieldpixelsPosition, int fieldpixelsLength) {
	        PipeWriter.presumeWriteFragment(output, MSG_PIXELROWMESSAGE_6);
	        PipeWriter.writeBytes(output,MSG_PIXELROWMESSAGE_6_FIELD_PIXELS_106, fieldpixelsBacking, fieldpixelsPosition, fieldpixelsLength);
	        PipeWriter.publishWrites(output);
	}
}
