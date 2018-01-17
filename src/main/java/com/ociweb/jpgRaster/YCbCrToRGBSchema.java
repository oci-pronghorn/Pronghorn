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
		
		public static final int MSG_HEADER = 0x00000000; //Group/OpenTempl/4
		public static final int FIELD_HEIGHT = 0x00400001; //IntegerSigned/None/0
		public static final int FIELD_WIDTH = 0x00400002; //IntegerSigned/None/1
		public static final int FIELD_FILENAME = 0x01000003; //ASCII/None/0
		public static final int MSG_PIXEL = 0x00000005; //Group/OpenTempl/4
		public static final int FIELD_RED = 0x00400001; //IntegerSigned/None/2
		public static final int FIELD_GREEN = 0x00400002; //IntegerSigned/None/3
		public static final int FIELD_BLUE = 0x00400003; //IntegerSigned/None/4


		public static void consume(Pipe<YCbCrToRGBSchema> input) {
		    while (PipeReader.tryReadFragment(input)) {
		        int msgIdx = PipeReader.getMsgIdx(input);
		        switch(msgIdx) {
		            case MSG_HEADER:
		                consumeHeaderMessage(input);
		            break;
		            case MSG_PIXEL:
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
		    int fieldheight = PipeReader.readInt(input,FIELD_HEIGHT);
		    int fieldwidth = PipeReader.readInt(input,FIELD_WIDTH);
		    StringBuilder fieldfilename = PipeReader.readUTF8(input,FIELD_FILENAME,new StringBuilder(PipeReader.readBytesLength(input,FIELD_FILENAME)));
		}
		public static void consumePixelMessage(Pipe<YCbCrToRGBSchema> input) {
		    int fieldred = PipeReader.readInt(input,FIELD_RED);
		    int fieldgreen = PipeReader.readInt(input,FIELD_GREEN);
		    int fieldblue = PipeReader.readInt(input,FIELD_BLUE);
		}

		public static void publishHeaderMessage(Pipe<YCbCrToRGBSchema> output, int fieldheight, int fieldwidth, CharSequence fieldfilename) {
		        PipeWriter.presumeWriteFragment(output, MSG_HEADER);
		        PipeWriter.writeInt(output,FIELD_HEIGHT, fieldheight);
		        PipeWriter.writeInt(output,FIELD_WIDTH, fieldwidth);
		        PipeWriter.writeUTF8(output,FIELD_FILENAME, fieldfilename);
		        PipeWriter.publishWrites(output);
		}
		public static void publishPixelMessage(Pipe<YCbCrToRGBSchema> output, int fieldred, int fieldgreen, int fieldblue) {
		        PipeWriter.presumeWriteFragment(output, MSG_PIXEL);
		        PipeWriter.writeInt(output,FIELD_RED, fieldred);
		        PipeWriter.writeInt(output,FIELD_GREEN, fieldgreen);
		        PipeWriter.writeInt(output,FIELD_BLUE, fieldblue);
		        PipeWriter.publishWrites(output);
		}
}
