package com.ociweb.pronghorn.ring.proxy;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.ring.token.TypeMask;

public abstract class OutputRingWriterMethod {
	
	public abstract void write(Object[] args);
	
	static final char[] EMPTY_CHAR = new char[0];
	static final byte[] EMPTY_BYTES = new byte[0];
	
	static OutputRingWriterMethod buildWriteForYourType(final RingBuffer outputRing, final int decimalPlaces, final int fieldLoc, final int extractedType, final FieldReferenceOffsetManager from) {
		final int absent32 = FieldReferenceOffsetManager.getAbsent32Value(from);
		final long absent64 = FieldReferenceOffsetManager.getAbsent64Value(from);
	
		//NOTE: the code in these anonymous classes is the same code that must be injected when the compile time code generation is done.
		
		switch (extractedType) {
			case 0:
			case 2:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());
					}
				};
			case 1:
			case 3:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null == args[0]) {
							RingWriter.writeInt(outputRing, fieldLoc, absent32);
						} else {
							RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());	
						}
					}
				};
			case 4:
			case 6:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());
					}
				};
			case 5:
			case 7:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null == args[0]) {
							RingWriter.writeLong(outputRing, fieldLoc, absent64);
						} else {
							RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());	
						}
					}
				};
			case 8:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						RingWriter.writeASCII(outputRing, fieldLoc, args[0].toString());
					}
				};
			case 9:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							RingWriter.writeASCII(outputRing, fieldLoc, EMPTY_CHAR, 0, -1);
						} else {
							RingWriter.writeASCII(outputRing, fieldLoc, args[0].toString());
						}
					}
				};
			case 10:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						RingWriter.writeUTF8(outputRing, fieldLoc, args[0].toString());
					}
				};
			case 11:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							RingWriter.writeUTF8(outputRing, fieldLoc, EMPTY_CHAR, 0, -1);
						} else {
							RingWriter.writeUTF8(outputRing, fieldLoc, args[0].toString());
						}
					}
				};
			case 12:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(), decimalPlaces);
					}
				};
			case 13:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							RingWriter.writeDecimal(outputRing, fieldLoc, absent32, absent64);
						} else {
							RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(), decimalPlaces);	
						}
					}
				};
			case 14:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (args[0] instanceof ByteBuffer) {
							RingWriter.writeBytes(outputRing, fieldLoc, (ByteBuffer)args[0], args.length>1 ? ((Number)args[1]).intValue() : ((ByteBuffer)args[0]).remaining());			
						} else {
							RingWriter.writeBytes(outputRing, fieldLoc, (byte[])args[0]);					
						}
					}
				};
			case 15:
				return new OutputRingWriterMethod() {
					@Override
					public final void write(Object[] args) {
						if (null==args[0]) {
							RingWriter.writeBytes(outputRing, fieldLoc, EMPTY_BYTES, 0, -1, 1);
						} else {
							if (args[0] instanceof ByteBuffer) {//NOTE: investigate returning wraper of backing array.
								RingWriter.writeBytes(outputRing, fieldLoc, (ByteBuffer)args[0], args.length>1 ? ((Number)args[1]).intValue() : ((ByteBuffer)args[0]).remaining());			
							} else {
								RingWriter.writeBytes(outputRing, fieldLoc, (byte[])args[0]);					
							}
						}
					}
				};
			default:
				throw new UnsupportedOperationException("No support yet for "
						+ TypeMask.xmlTypeName[extractedType]);
	
		}
	}

}
