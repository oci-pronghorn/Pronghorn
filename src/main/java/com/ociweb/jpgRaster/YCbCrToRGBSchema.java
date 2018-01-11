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
		    new int[]{0xc0400002,0xa0000000,0xc0200002,0xc0400001,0xc0200001},
		    (short)0,
		    new String[]{"HeaderMessage","filename",null,"PixelMessage",null},
		    new long[]{1, 301, 0, 2, 0},
		    new String[]{"global",null,null,"global",null},
		    "YCbCrToRGB.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected YCbCrToRGBSchema() { 
		    super(FROM);
		}

		public static final YCbCrToRGBSchema instance = new YCbCrToRGBSchema();
		
		public static final int MSG_HEADERMESSAGE_1 = 0x00000000; //Group/OpenTempl/2
		public static final int MSG_HEADERMESSAGE_1_FIELD_FILENAME_301 = 0x01000001; //ASCII/None/0
		public static final int MSG_PIXELMESSAGE_2 = 0x00000003; //Group/OpenTempl/1


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
		    StringBuilder fieldfilename = PipeReader.readUTF8(input,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301,new StringBuilder(PipeReader.readBytesLength(input,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301)));
		}
		public static void consumePixelMessage(Pipe<YCbCrToRGBSchema> input) {
		}

		public static void publishHeaderMessage(Pipe<YCbCrToRGBSchema> output, CharSequence fieldfilename) {
		    PipeWriter.presumeWriteFragment(output, MSG_HEADERMESSAGE_1);
		    PipeWriter.writeUTF8(output,MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, fieldfilename);
		    PipeWriter.publishWrites(output);
		}
		public static void publishPixelMessage(Pipe<YCbCrToRGBSchema> output) {
		    PipeWriter.presumeWriteFragment(output, MSG_PIXELMESSAGE_2);
		    PipeWriter.publishWrites(output);
		}


}
