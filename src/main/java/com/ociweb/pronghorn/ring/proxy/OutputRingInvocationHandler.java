package com.ociweb.pronghorn.ring.proxy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;
import com.ociweb.pronghorn.ring.util.hash.IntHashTable;
import com.ociweb.pronghorn.ring.util.hash.LongHashTable;

public class OutputRingInvocationHandler implements InvocationHandler {
	//TODO: NOTE: this approach does NOT support nested structures at all.

	private static final char[] EMPTY_CHAR = new char[0];
	private static final byte[] EMPTY_BYTES = new byte[0];
	private final RingBuffer outputRing;
	private final FieldReferenceOffsetManager from;
	private final int msgIdx;
	//This only supports one template message

	private final LongHashTable fieldIdTable = new LongHashTable(7); //no need to use messageId in the keys
	
	
	public OutputRingInvocationHandler(RingBuffer outputRing, int msgIdx, Class<?> clazz) {
		this.outputRing = outputRing;
		this.from = RingBuffer.from(outputRing);
		this.msgIdx = msgIdx;
					
		int fields = this.from.fragScriptSize[msgIdx];
		int c = 0;
		while (++c<fields) {
			
			int fieldCursor = msgIdx+c;
			long fieldId = this.from.fieldIdScript[fieldCursor];
			int fieldLoc = FieldReferenceOffsetManager.lookupFieldLocator(fieldId, msgIdx, from);
			
			LongHashTable.setItem(fieldIdTable, fieldId,  fieldLoc);
			
			int extractedType = (fieldLoc >> FieldReferenceOffsetManager.RW_FIELD_OFF_BITS) & TokenBuilder.MASK_TYPE;
			if (TypeMask.Decimal==extractedType || TypeMask.DecimalOptional==extractedType) {
				c++;//one extra field for decimals
			}
		}		
	}
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
					
		ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
		int fieldLoc =  LongHashTable.getItem(fieldIdTable, fieldAnnonation.fieldId());		
		writeForYourType(args, fieldAnnonation, fieldLoc, (fieldLoc >> FieldReferenceOffsetManager.RW_FIELD_OFF_BITS) & TokenBuilder.MASK_TYPE);		
		
		return null;
	}

	private void writeForYourType(Object[] args,
			ProngTemplateField fieldAnnonation, int fieldLoc, int extractedType) {
		switch (extractedType) {
			case 0:
			case 2:
				RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());		
				break;
			case 1:
			case 3:
				writeIntOptional(args, fieldLoc);
				break;
			case 4:
			case 6:
				RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());		
				break;
			case 5:
			case 7:
				writeLongOptional(args, fieldLoc);
				break;
			case 8:
				RingWriter.writeASCII(outputRing, fieldLoc, args[0].toString());
				break;
			case 9:
				writeOptionalASCII(args, fieldLoc);
				break;
			case 10:
				RingWriter.writeUTF8(outputRing, fieldLoc, args[0].toString());
				break;
			case 11:
				writeOptionalUTF8(args, fieldLoc);
				break;	
			case 12:
				RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(),fieldAnnonation.decimalPlaces());	
				break;
			case 13:
				writeOptionalDecimal(args, fieldAnnonation, fieldLoc);
				break;
			case 14:
				writeBytes(args, fieldLoc);				
				break;
			case 15:
				writeOptionalBytes(args, fieldLoc);
				break;	
			default:
				throw new UnsupportedOperationException("No support yet for "+TypeMask.xmlTypeName[extractedType]);
		
		}
	}

	private void writeOptionalBytes(Object[] args, int fieldLoc) {
		if (null==args[0]) {
			RingWriter.writeBytes(outputRing, fieldLoc, EMPTY_BYTES, 0, -1, 1);
		} else {
			writeBytes(args, fieldLoc);
		}
	}

	private void writeBytes(Object[] args, int fieldLoc) {
		if (args[0] instanceof ByteBuffer) {
			RingWriter.writeBytes(outputRing, fieldLoc, (ByteBuffer)args[0], args.length>1 ? ((Number)args[1]).intValue() : ((ByteBuffer)args[0]).remaining());			
		} else {
			RingWriter.writeBytes(outputRing, fieldLoc, (byte[])args[0]);					
		}
	}

	private void writeOptionalDecimal(Object[] args, ProngTemplateField fieldAnnonation, int fieldLoc) {
		if (null==args[0]) {
			RingWriter.writeDecimal(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent32Value(from), FieldReferenceOffsetManager.getAbsent64Value(from));
		} else {
			RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(),fieldAnnonation.decimalPlaces());	
		}
	}

	private void writeOptionalUTF8(Object[] args, int fieldLoc) {
		if (null==args[0]) {
			RingWriter.writeUTF8(outputRing, fieldLoc, EMPTY_CHAR, 0, -1);
		} else {
			RingWriter.writeUTF8(outputRing, fieldLoc, args[0].toString());
		}
	}

	private void writeOptionalASCII(Object[] args, int fieldLoc) {
		if (null==args[0]) {
			RingWriter.writeASCII(outputRing, fieldLoc, EMPTY_CHAR, 0, -1);
		} else {
			RingWriter.writeASCII(outputRing, fieldLoc, args[0].toString());
		}
	}

	private void writeLongOptional(Object[] args, int fieldLoc) {
		if (null == args[0]) {
			RingWriter.writeLong(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent64Value(from));
		} else {
			RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());	
		}
	}

	private void writeIntOptional(Object[] args, int fieldLoc) {
		if (null == args[0]) {
			RingWriter.writeInt(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent32Value(from));
		} else {
			RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());	
		}
	}

}
