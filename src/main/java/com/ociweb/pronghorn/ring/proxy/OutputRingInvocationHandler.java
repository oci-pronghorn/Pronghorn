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
	
	private final IntHashTable fieldHash = new IntHashTable(7);
	
	public OutputRingInvocationHandler(RingBuffer outputRing, int msgIdx, Class<?> clazz) {
		this.outputRing = outputRing;
		this.from = RingBuffer.from(outputRing);
		this.msgIdx = msgIdx;
		
		//  NEW IDEA BUT ITS NOT FULLY DONE YET
//		Method[] methods = clazz.getMethods();
//		int j = methods.length;
//		while (--j>=0) {
//			ProngTemplateField fieldAnnonation = methods[j].getAnnotation(ProngTemplateField.class);
//			if (null!=fieldAnnonation) {
//				//save these fields so we need not look them up again later.
//		
//				int fieldLoc = FieldReferenceOffsetManager.lookupFieldLocator(fieldAnnonation.fieldId(), msgIdx, from);				
//				int hashCode = System.identityHashCode(methods[j]);
//								
//				IntHashTable.setItem(fieldHash, hashCode, fieldLoc);
//								
//			}			
//		}
		
		
					
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
	
//	Map<Method,Integer> temp = new HashMap<Method,Integer>();
	
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
					
		int fieldLoc;
		//Integer fieldLoc = temp.get(method);
		//if (null==fieldLoc) {
			ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
			fieldLoc =  LongHashTable.getItem(fieldIdTable, fieldAnnonation.fieldId());
		//	temp.put(method, fieldLoc);
		//}
		
		
		int extractedType = (fieldLoc >> FieldReferenceOffsetManager.RW_FIELD_OFF_BITS) & TokenBuilder.MASK_TYPE;
		
		switch (extractedType) {
			case 0:
				RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());		
				break;
			case 1:
				if (null == args[0]) {
					RingWriter.writeInt(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent32Value(from));
				} else {
					RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());	
				}
				break;
			case 2:
				RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());		
				break;
			case 3:
				if (null == args[0]) {
					RingWriter.writeInt(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent32Value(from));
				} else {
					RingWriter.writeInt(outputRing, fieldLoc, ((Number)args[0]).intValue());	
				}
				break;
			case 4:
				RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());		
				break;
			case 5:
				if (null == args[0]) {
					RingWriter.writeLong(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent64Value(from));
				} else {
					RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());	
				}
				break;
			case 6:
				RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());		
				break;
			case 7:
				if (null == args[0]) {
					RingWriter.writeLong(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent64Value(from));
				} else {
					RingWriter.writeLong(outputRing, fieldLoc, ((Number)args[0]).longValue());	
				}
				break;
			case 8:
				RingWriter.writeASCII(outputRing, fieldLoc, args[0].toString());
				break;
			case 9:
				if (null==args[0]) {
					RingWriter.writeASCII(outputRing, fieldLoc, EMPTY_CHAR, 0, -1);
				} else {
					RingWriter.writeASCII(outputRing, fieldLoc, args[0].toString());
				}
				break;
			case 10:
				RingWriter.writeUTF8(outputRing, fieldLoc, args[0].toString());
				break;
			case 11:
				if (null==args[0]) {
					RingWriter.writeUTF8(outputRing, fieldLoc, EMPTY_CHAR, 0, -1);
				} else {
					RingWriter.writeUTF8(outputRing, fieldLoc, args[0].toString());
				}
				break;	
			case 12:
				RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(),method.getAnnotation(ProngTemplateField.class).decimalPlaces());	
				break;
			case 13:
				if (null==args[0]) {
					RingWriter.writeDecimal(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent32Value(from), FieldReferenceOffsetManager.getAbsent64Value(from));
				} else {
					RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(),method.getAnnotation(ProngTemplateField.class).decimalPlaces());	
				}
				break;
			case 14:
				if (args[0] instanceof ByteBuffer) {
					RingWriter.writeBytes(outputRing, fieldLoc, (ByteBuffer)args[0], args.length>1 ? ((Number)args[1]).intValue() : ((ByteBuffer)args[0]).remaining());			
				} else {
					RingWriter.writeBytes(outputRing, fieldLoc, (byte[])args[0]);					
				}				
				break;
			case 15:
				if (null==args[0]) {
					RingWriter.writeBytes(outputRing, fieldLoc, EMPTY_BYTES, 0, -1, 1);
				} else {
					if (args[0] instanceof ByteBuffer) {
						RingWriter.writeBytes(outputRing, fieldLoc, (ByteBuffer)args[0], args.length>1 ? ((Number)args[1]).intValue() : ((ByteBuffer)args[0]).remaining());			
					} else {
						RingWriter.writeBytes(outputRing, fieldLoc, (byte[])args[0]);					
					}
				}
				break;	
			default:
				throw new UnsupportedOperationException("No support yet for "+TypeMask.xmlTypeName[extractedType]);
		
		}		
		
		return null;
	}

}
