package com.ociweb.pronghorn.pipe.build;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;

public class BlankSchemaExample2 extends MessageSchema<BlankSchemaExample2> {

	public final static FieldReferenceOffsetManager FROM = new FieldReferenceOffsetManager(
		    new int[]{0xc0400002,0xb8000000,0xc0200002},
		    (short)0,
		    new String[]{"ChunkedStream","ByteArray",null},
		    new long[]{1, 2, 0},
		    new String[]{"global",null,null},
		    "rawDataSchema.xml",
		    new long[]{2, 2, 0},
		    new int[]{2, 2, 0});


		protected BlankSchemaExample2() { 
		    super(FROM);
		}

		public static final BlankSchemaExample2 instance = new BlankSchemaExample2();
}
