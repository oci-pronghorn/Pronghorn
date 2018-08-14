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
	    new int[]{0xc0400005,0x88000000,0x88000001,0xa0000000,0x88000002,0xc0200005,0xc0400005,0x88000003,0x88000004,0x88000005,0x88000006,0xc0200005,0xc0400004,0x88000007,0x88000008,0xb8000001,0xc0200004,0xc0400004,0xb8000002,0xb8000003,0xb8000004,0xc0200004},
	    (short)0,
	    new String[]{"HeaderMessage","height","width","filename","final",null,"ColorComponentMessage",
	    "componentID","horizontalSamplingFactor","verticalSamplingFactor","quantizationTableID",
	    null,"QuantizationTableMessage","tableId","precision","table",null,"MCUMessage","y",
	    "cb","cr",null},
	    new long[]{1, 101, 201, 301, 401, 0, 2, 102, 202, 302, 402, 0, 3, 103, 203, 303, 0, 4, 104, 204, 304, 0},
	    new String[]{"global",null,null,null,null,null,"global",null,null,null,null,null,"global",null,
	    null,null,null,"global",null,null,null,null},
	    "JPGSchema.xml",
	    new long[]{2, 2, 0},
	    new int[]{2, 2, 0});

	protected JPGSchema() {
	    super(FROM);
	}

	public static final JPGSchema instance = new JPGSchema();

	public static final int MSG_HEADERMESSAGE_1 = 0x00000000; //Group/OpenTempl/5
	public static final int MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101 = 0x00400001; //IntegerSigned/None/0
	public static final int MSG_HEADERMESSAGE_1_FIELD_WIDTH_201 = 0x00400002; //IntegerSigned/None/1
	public static final int MSG_HEADERMESSAGE_1_FIELD_FILENAME_301 = 0x01000003; //ASCII/None/0
	public static final int MSG_HEADERMESSAGE_1_FIELD_FINAL_401 = 0x00400005; //IntegerSigned/None/2
	public static final int MSG_COLORCOMPONENTMESSAGE_2 = 0x00000006; //Group/OpenTempl/5
	public static final int MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102 = 0x00400001; //IntegerSigned/None/3
	public static final int MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202 = 0x00400002; //IntegerSigned/None/4
	public static final int MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302 = 0x00400003; //IntegerSigned/None/5
	public static final int MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402 = 0x00400004; //IntegerSigned/None/6
	public static final int MSG_QUANTIZATIONTABLEMESSAGE_3 = 0x0000000c; //Group/OpenTempl/4
	public static final int MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLEID_103 = 0x00400001; //IntegerSigned/None/7
	public static final int MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_PRECISION_203 = 0x00400002; //IntegerSigned/None/8
	public static final int MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLE_303 = 0x01c00003; //ByteVector/None/1
	public static final int MSG_MCUMESSAGE_4 = 0x00000011; //Group/OpenTempl/4
	public static final int MSG_MCUMESSAGE_4_FIELD_Y_104 = 0x01c00001; //ByteVector/None/2
	public static final int MSG_MCUMESSAGE_4_FIELD_CB_204 = 0x01c00003; //ByteVector/None/3
	public static final int MSG_MCUMESSAGE_4_FIELD_CR_304 = 0x01c00005; //ByteVector/None/4

	public static void consume(Pipe<JPGSchema> input) {
	    while (PipeReader.tryReadFragment(input)) {
	        int msgIdx = PipeReader.getMsgIdx(input);
	        switch(msgIdx) {
	            case MSG_HEADERMESSAGE_1:
	                consumeHeaderMessage(input);
	            break;
	            case MSG_COLORCOMPONENTMESSAGE_2:
	                consumeColorComponentMessage(input);
	            break;
	            case MSG_QUANTIZATIONTABLEMESSAGE_3:
	                consumeQuantizationTableMessage(input);
	            break;
	            case MSG_MCUMESSAGE_4:
	                consumeMCUMessage(input);
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
	    int fieldfinal = PipeReader.readInt(input,MSG_HEADERMESSAGE_1_FIELD_FINAL_401);
	}
	public static void consumeColorComponentMessage(Pipe<JPGSchema> input) {
	    int fieldcomponentID = PipeReader.readInt(input,MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102);
	    int fieldhorizontalSamplingFactor = PipeReader.readInt(input,MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202);
	    int fieldverticalSamplingFactor = PipeReader.readInt(input,MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302);
	    int fieldquantizationTableID = PipeReader.readInt(input,MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402);
	}
	public static void consumeQuantizationTableMessage(Pipe<JPGSchema> input) {
	    int fieldtableId = PipeReader.readInt(input,MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLEID_103);
	    int fieldprecision = PipeReader.readInt(input,MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_PRECISION_203);
	    DataInputBlobReader<JPGSchema> fieldtable = PipeReader.inputStream(input, MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLE_303);
	}
	public static void consumeMCUMessage(Pipe<JPGSchema> input) {
	    DataInputBlobReader<JPGSchema> fieldy = PipeReader.inputStream(input, MSG_MCUMESSAGE_4_FIELD_Y_104);
	    DataInputBlobReader<JPGSchema> fieldcb = PipeReader.inputStream(input, MSG_MCUMESSAGE_4_FIELD_CB_204);
	    DataInputBlobReader<JPGSchema> fieldcr = PipeReader.inputStream(input, MSG_MCUMESSAGE_4_FIELD_CR_304);
	}

	public static void publishHeaderMessage(Pipe<JPGSchema> output, int fieldheight, int fieldwidth, CharSequence fieldfilename, int fieldfinal) {
	    PipeWriter.presumeWriteFragment(output, MSG_HEADERMESSAGE_1);
	    PipeWriter.writeInt(output,MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, fieldheight);
	    PipeWriter.writeInt(output,MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, fieldwidth);
	    PipeWriter.writeUTF8(output,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, fieldfilename);
	    PipeWriter.writeInt(output,MSG_HEADERMESSAGE_1_FIELD_FINAL_401, fieldfinal);
	    PipeWriter.publishWrites(output);
	}
	public static void publishColorComponentMessage(Pipe<JPGSchema> output, int fieldcomponentID, int fieldhorizontalSamplingFactor, int fieldverticalSamplingFactor, int fieldquantizationTableID) {
	    PipeWriter.presumeWriteFragment(output, MSG_COLORCOMPONENTMESSAGE_2);
	    PipeWriter.writeInt(output,MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102, fieldcomponentID);
	    PipeWriter.writeInt(output,MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202, fieldhorizontalSamplingFactor);
	    PipeWriter.writeInt(output,MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302, fieldverticalSamplingFactor);
	    PipeWriter.writeInt(output,MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402, fieldquantizationTableID);
	    PipeWriter.publishWrites(output);
	}
	public static void publishQuantizationTableMessage(Pipe<JPGSchema> output, int fieldtableId, int fieldprecision, byte[] fieldtableBacking, int fieldtablePosition, int fieldtableLength) {
	    PipeWriter.presumeWriteFragment(output, MSG_QUANTIZATIONTABLEMESSAGE_3);
	    PipeWriter.writeInt(output,MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLEID_103, fieldtableId);
	    PipeWriter.writeInt(output,MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_PRECISION_203, fieldprecision);
	    PipeWriter.writeBytes(output,MSG_QUANTIZATIONTABLEMESSAGE_3_FIELD_TABLE_303, fieldtableBacking, fieldtablePosition, fieldtableLength);
	    PipeWriter.publishWrites(output);
	}
	public static void publishMCUMessage(Pipe<JPGSchema> output, byte[] fieldyBacking, int fieldyPosition, int fieldyLength, byte[] fieldcbBacking, int fieldcbPosition, int fieldcbLength, byte[] fieldcrBacking, int fieldcrPosition, int fieldcrLength) {
	    PipeWriter.presumeWriteFragment(output, MSG_MCUMESSAGE_4);
	    PipeWriter.writeBytes(output,MSG_MCUMESSAGE_4_FIELD_Y_104, fieldyBacking, fieldyPosition, fieldyLength);
	    PipeWriter.writeBytes(output,MSG_MCUMESSAGE_4_FIELD_CB_204, fieldcbBacking, fieldcbPosition, fieldcbLength);
	    PipeWriter.writeBytes(output,MSG_MCUMESSAGE_4_FIELD_CR_304, fieldcrBacking, fieldcrPosition, fieldcrLength);
	    PipeWriter.publishWrites(output);
	}

}
