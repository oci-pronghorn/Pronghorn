package com.ociweb.pronghorn.pipe.proxy;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.token.TypeMask;

public abstract class InputPipeReaderMethod {
	
	public abstract Object read(Object[] args);
	
	static InputPipeReaderMethod buildReadForYourType(final Pipe pipe, final int fieldLoc, final int extractedType, final FieldReferenceOffsetManager from) {
		
		final int absent32 = FieldReferenceOffsetManager.getAbsent32Value(from);
		final long absent64 = FieldReferenceOffsetManager.getAbsent64Value(from);
		
		//NOTE: the code in these anonymous classes is the same code that must be injected when the compile time code generation is done.

		//WARNING: unlike the other classes this one is NOT garbage free, transfer objects are built to hand back values
		//         this will no longer be the problem after code generation is applied.
		
		switch (extractedType) {
			case 0:
			case 2:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return new Integer(PipeReader.readInt(pipe, fieldLoc));
					}
				};
			case 1:
			case 3:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						int value = PipeReader.readInt(pipe, fieldLoc);
						return value==absent32 ? null : new Integer(value);
					}
				};
			case 4:
			case 6:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return new Long(PipeReader.readLong(pipe, fieldLoc));
					}
				};
			case 5:
			case 7:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						long value = PipeReader.readLong(pipe, fieldLoc);
						return value==absent64 ? null : new Long(value);
					}
				};
			case 8:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return PipeReader.readASCII(pipe, fieldLoc, (Appendable)args[0]);
					}
				};
			case 9:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						if (PipeReader.readDataLength(pipe, fieldLoc)<0) {
							return null;
						} else {
							return PipeReader.readASCII(pipe, fieldLoc, (Appendable)args[0]);
						}
					}
				};
			case 10:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						return PipeReader.readUTF8(pipe, fieldLoc, (Appendable)args[0]);
					}
				};
			case 11:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						if (PipeReader.readDataLength(pipe, fieldLoc)<0) {
							return null;
						} else {
							return PipeReader.readUTF8(pipe, fieldLoc, (Appendable)args[0]);
						}
					}
				};
			case 12:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {						
						return new Double(PipeReader.readDouble(pipe, fieldLoc));
					}
				};
			case 13:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						
						if (PipeReader.readDecimalExponent(pipe, fieldLoc)==absent32) {
							return null;
						} else {
							return new Double(PipeReader.readDouble(pipe, fieldLoc));
						}
					}
				};
			case 14:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						switch (args.length) {
								case 1:
									return PipeReader.readBytes(pipe, fieldLoc, (ByteBuffer)args[0]);
								case 2:
									return PipeReader.readBytes(pipe, fieldLoc, (byte[])args[0],((Number)args[1]).intValue());
								case 3:
									return PipeReader.readBytes(pipe, fieldLoc, (byte[])args[0],((Number)args[1]).intValue(),((Number)args[2]).intValue());
						}
						return null;
					}
				};
			case 15:
				return new InputPipeReaderMethod() {
					@Override
					public final Object read(Object[] args) {
						if (PipeReader.readDataLength(pipe, fieldLoc)>0) {
							switch (args.length) {
									case 1:
										return PipeReader.readBytes(pipe, fieldLoc, (ByteBuffer)args[0]);
									case 2:
										return PipeReader.readBytes(pipe, fieldLoc, (byte[])args[0],((Number)args[1]).intValue());
									case 3:
										return PipeReader.readBytes(pipe, fieldLoc, (byte[])args[0],((Number)args[1]).intValue(),((Number)args[2]).intValue());
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
