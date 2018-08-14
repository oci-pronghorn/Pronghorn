package com.ociweb.pronghorn.struct;

import java.io.IOException;

import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.StructuredReader;
import com.ociweb.pronghorn.util.Appendables;

public enum StructType {
	////////////////////////////////////////////////////
	//do not modify this order since the ordinal values may be saved
	////////////////////////////////////////////////////
	Blob {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			throw new UnsupportedOperationException("Unable to convert unknown blob into string");
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			throw new UnsupportedOperationException("Unable to convert unknown blob into string");
		}
	},    //             http post payload
	Boolean {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			try {
				if (activeReader.readBoolean(id)) {
					target.append("true");
				} else {
					target.append("false");
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return target;
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			try {
				if (reader.readBoolean()) {
					target.append("true");
				} else {
					target.append("false");
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			return target;
		}
	}, //JSON 
	Text {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			return activeReader.readText(id,target);
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			return reader.readUTF(target);
		}
	},    //JSON URL     UTF8 encoded with packed int length
	Decimal {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			return activeReader.readDecimalAsText(id, target);
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			return reader.readDecimalAsText(target);
		}
	}, //JSON URL
	Long {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			return Appendables.appendValue(target, activeReader.readLong(id));
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			return Appendables.appendValue(target, reader.readPackedInt());
		}
	},    //JSON URL     Packed
	Integer {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {			
			return Appendables.appendValue(target, activeReader.readInt(id));
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			return Appendables.appendValue(target, reader.readPackedInt());
		}
	}, //             Packed
	Short {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			return Appendables.appendValue(target, activeReader.readInt(id));
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			return Appendables.appendValue(target, reader.readPackedInt());
		}
	},
	Byte {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			return Appendables.appendValue(target, activeReader.read(id).readByte());
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			return Appendables.appendValue(target, reader.readByte());
		}
	},
	Rational {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			return activeReader.readRationalAsText(id, target);			
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			return reader.readRationalAsText(target);
		}
	},//     URL
	Double {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			
			try {
				target.append(String.valueOf(java.lang.Double.longBitsToDouble(activeReader.readLong(id))));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			return target;
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			
			try {
				target.append(String.valueOf(java.lang.Double.longBitsToDouble(reader.readPackedLong())));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			return target;
		}
	},
	Float {
		@Override
		public <A extends Appendable> A appendTo(StructuredReader activeReader, long id, A target) {
			
			try {
				target.append(String.valueOf(java.lang.Float.intBitsToFloat(activeReader.readInt(id))));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			return target;
		}

		@Override
		public <A extends Appendable> A appendTo(ChannelReader reader, A target) {
			
			try {
				target.append(String.valueOf(java.lang.Float.intBitsToFloat(reader.readPackedInt())));
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			
			return target;
		}
	},;

	public abstract <A extends Appendable> A appendTo(StructuredReader reader, long id, A target);
	public abstract <A extends Appendable> A appendTo(ChannelReader reader, A target);
	
}
