package com.ociweb.pronghorn.ring.proxy;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.ring.token.TypeMask;

public abstract class InputRingReaderMethod {
	
	public abstract Object read(Object[] args);
	
	static InputRingReaderMethod buildReadForYourType(final RingBuffer inputRing, final int fieldLoc, final int extractedType, final FieldReferenceOffsetManager from) {
		
		final int absent32 = FieldReferenceOffsetManager.getAbsent32Value(from);
		final long absent64 = FieldReferenceOffsetManager.getAbsent64Value(from);
		
		//NOTE: the code in these anonymous classes is the same code that must be injected when the compile time code generation is done.

		//WARNING: unlike the other classes this one is NOT garbage free, transfer objects are built to hand back values
		//         this will no longer be the problem after code generation is applied.
		
		switch (extractedType) {
			case 0:
			case 2:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return new Integer(RingReader.readInt(inputRing, fieldLoc));
					}
				};
			case 1:
			case 3:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						int value = RingReader.readInt(inputRing, fieldLoc);
						return value==absent32 ? null : new Integer(value);
					}
				};
			case 4:
			case 6:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return new Long(RingReader.readLong(inputRing, fieldLoc));
					}
				};
			case 5:
			case 7:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						long value = RingReader.readLong(inputRing, fieldLoc);
						return value==absent64 ? null : new Long(value);
					}
				};
			case 8:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return RingReader.readASCII(inputRing, fieldLoc, (Appendable)args[0]);
					}
				};
			case 9:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						if (RingReader.readDataLength(inputRing, fieldLoc)<0) {
							return null;
						} else {
							return RingReader.readASCII(inputRing, fieldLoc, (Appendable)args[0]);
						}
					}
				};
			case 10:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return RingReader.readUTF8(inputRing, fieldLoc, (Appendable)args[0]);
					}
				};
			case 11:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						if (RingReader.readDataLength(inputRing, fieldLoc)<0) {
							return null;
						} else {
							return RingReader.readUTF8(inputRing, fieldLoc, (Appendable)args[0]);
						}
					}
				};
			case 12:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {						
						return new Double(RingReader.readDouble(inputRing, fieldLoc));
					}
				};
			case 13:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						
						if (RingReader.readDecimalExponent(inputRing, fieldLoc)==absent32) {
							return null;
						} else {
							return new Double(RingReader.readDouble(inputRing, fieldLoc));
						}
					}
				};
			case 14:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						switch (args.length) {
								case 1:
									return RingReader.readBytes(inputRing, fieldLoc, (ByteBuffer)args[0]);
								case 2:
									return RingReader.readBytes(inputRing, fieldLoc, (byte[])args[0],((Number)args[1]).intValue());
								case 3:
									return RingReader.readBytes(inputRing, fieldLoc, (byte[])args[0],((Number)args[1]).intValue(),((Number)args[2]).intValue());
						}
						return null;
					}
				};
			case 15:
				return new InputRingReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						if (RingReader.readDataLength(inputRing, fieldLoc)>0) {
							switch (args.length) {
									case 1:
										return RingReader.readBytes(inputRing, fieldLoc, (ByteBuffer)args[0]);
									case 2:
										return RingReader.readBytes(inputRing, fieldLoc, (byte[])args[0],((Number)args[1]).intValue());
									case 3:
										return RingReader.readBytes(inputRing, fieldLoc, (byte[])args[0],((Number)args[1]).intValue(),((Number)args[2]).intValue());
							}
						}
						return null;
					}
				};
			default:
				throw new UnsupportedOperationException("No support yet for "
						+ TypeMask.xmlTypeName[extractedType]);
	
		}
	}


}
