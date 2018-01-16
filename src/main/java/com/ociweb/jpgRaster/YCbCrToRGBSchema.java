package com.ociweb.jpgRaster;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;

public class YCbCrToRGBSchema extends MessageSchema<YCbCrToRGBSchema> {
	protected YCbCrToRGBSchema(FieldReferenceOffsetManager FROM) { 
	    super(FROM);
	}
	
	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400004,0x88000000,0x88000001,0xa0000000,0xc0200004,0xc0400004,0x88000002,0x88000003,0x88000004,0xc0200004},
		    (short)0,
		    new String[]{"HeaderMessage","height","width","filename",null,"PixelMessage","red","green","blue",
		    null},
		    new long[]{1, 101, 201, 301, 0, 2, 102, 202, 302, 0},
		    new String[]{"global",null,null,null,null,"global",null,null,null,null},
		    "YCbCrToRGB.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected YCbCrToRGBSchema() { 
		    super(FROM);
		}

		public static final YCbCrToRGBSchema instance = new YCbCrToRGBSchema();
		
		public static final int MSG_HEADERMESSAGE_1 = 0x00000000; //Group/OpenTempl/4
		public static final int MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101 = 0x00400001; //IntegerSigned/None/0
		public static final int MSG_HEADERMESSAGE_1_FIELD_WIDTH_201 = 0x00400002; //IntegerSigned/None/1
		public static final int MSG_HEADERMESSAGE_1_FIELD_FILENAME_301 = 0x01000003; //ASCII/None/0
		public static final int MSG_PIXELMESSAGE_2 = 0x00000005; //Group/OpenTempl/4
		public static final int MSG_PIXELMESSAGE_2_FIELD_RED_102 = 0x00400001; //IntegerSigned/None/2
		public static final int MSG_PIXELMESSAGE_2_FIELD_GREEN_202 = 0x00400002; //IntegerSigned/None/3
		public static final int MSG_PIXELMESSAGE_2_FIELD_BLUE_302 = 0x00400003; //IntegerSigned/None/4


		public static void consume(Pipe<YCbCrToRGBSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_HEADERMESSAGE_1:
		                consumeHeaderMessage(input);
		            break;
		            case MSG_PIXELMESSAGE_2:
		                consumePixelMessage(input);
		            break;
		            case -1:
		               //requestShutdown();
		            break;
		        }
		        PipeReader.releaseReadLock(input);
		    }
		}

		public static void consumeHeaderMessage(Pipe<YCbCrToRGBSchema> input) {
		    int fieldheight = PipeReader.readInt(input,MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
		    int fieldwidth = PipeReader.readInt(input,MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
		    StringBuilder fieldfilename = PipeReader.readUTF8(input,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301,new StringBuilder(PipeReader.readBytesLength(input,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301)));
		}
		public static void consumePixelMessage(Pipe<YCbCrToRGBSchema> input) {
		    int fieldred = PipeReader.readInt(input,MSG_PIXELMESSAGE_2_FIELD_RED_102);
		    int fieldgreen = PipeReader.readInt(input,MSG_PIXELMESSAGE_2_FIELD_GREEN_202);
		    int fieldblue = PipeReader.readInt(input,MSG_PIXELMESSAGE_2_FIELD_BLUE_302);
		}

		public static void publishHeaderMessage(Pipe<YCbCrToRGBSchema> output, int fieldheight, int fieldwidth, CharSequence fieldfilename) {
		        PipeWriter.presumeWriteFragment(output, MSG_HEADERMESSAGE_1);
		        PipeWriter.writeInt(output,MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, fieldheight);
		        PipeWriter.writeInt(output,MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, fieldwidth);
		        PipeWriter.writeUTF8(output,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, fieldfilename);
		        PipeWriter.publishWrites(output);
		}
		public static void publishPixelMessage(Pipe<YCbCrToRGBSchema> output, int fieldred, int fieldgreen, int fieldblue) {
		        PipeWriter.presumeWriteFragment(output, MSG_PIXELMESSAGE_2);
		        PipeWriter.writeInt(output,MSG_PIXELMESSAGE_2_FIELD_RED_102, fieldred);
		        PipeWriter.writeInt(output,MSG_PIXELMESSAGE_2_FIELD_GREEN_202, fieldgreen);
		        PipeWriter.writeInt(output,MSG_PIXELMESSAGE_2_FIELD_BLUE_302, fieldblue);
		        PipeWriter.publishWrites(output);
		}
}
