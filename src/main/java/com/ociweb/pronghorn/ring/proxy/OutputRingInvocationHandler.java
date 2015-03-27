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
import com.ociweb.pronghorn.ring.util.hash.MurmurHash;

public class OutputRingInvocationHandler implements InvocationHandler {
	//TODO: NOTE: this approach does NOT support nested structures at all.

	private static final char[] EMPTY_CHAR = new char[0];
	private static final byte[] EMPTY_BYTES = new byte[0];
	private final RingBuffer outputRing;
	private final FieldReferenceOffsetManager from;
	private final int msgIdx;
	//This only supports one template message


	private final IntHashTable hashTable;
	private final int[] fieldLocs;
	private final int[] decimalPlaces;
	private final int[] types;
	
	private int c1 = 0;
	private int c2 = 0;
	private int c3 = 0;
	
	
	public OutputRingInvocationHandler(RingBuffer outputRing, int msgIdx, Class<?> clazz) {
		this.outputRing = outputRing;
		this.from = RingBuffer.from(outputRing);
		this.msgIdx = msgIdx;
							
		int j;
				
		//Compute the shortest name and
		//computed the leading chars that all match
		
		int minMethodNameLength = Integer.MAX_VALUE;
		int leadingMatchingChars = Integer.MAX_VALUE;
		
		final Method[] methods = clazz.getMethods();
		
		hashTable = new IntHashTable(8);
		fieldLocs = new int[methods.length];
		decimalPlaces = new int[methods.length];	
		types = new int[methods.length];
		
		j = methods.length;
		String lastName = null;
		while (--j>=0) {
			Method method = methods[j];			
			ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
			if (null!=fieldAnnonation) {
				String methodName = method.getName();
				minMethodNameLength = Math.min(minMethodNameLength, methodName.length());
				if (null!=lastName) {
					int limit = Math.min(methodName.length(), lastName.length());
					int i = 0;
					while (i<limit && lastName.charAt(i)==methodName.charAt(i)) {
						i++;
					}
					leadingMatchingChars = Math.min(leadingMatchingChars, i);					
				}
				lastName = methodName;
			}
		}
				
		int posToCheck = minMethodNameLength-leadingMatchingChars;
		
		if (posToCheck <= 3) {
			throw new UnsupportedOperationException("Method names are too similar. Every annotated field must have a different field name.");
		}
		
		//TODO: need to try different values, but for now this will work fine.
		//This is all done at runtime so it still works with obfuscation.
		c1 = leadingMatchingChars+1;
		c2 = leadingMatchingChars+2; 
		c3 = leadingMatchingChars+3; 
		
		
		//TODO: need to confirm that values are less than minMethodNameLength and together lead to a unique value.
		
		//find those with the same name length
		//of those check that c1 makes them unique
		//once c1 is set must move to c2 to make the next unique.
		//loop here and make nested loop call?
		j = methods.length;
		while (--j>=0) {
			Method method = methods[j];			
			ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
			if (null!=fieldAnnonation) {
				String methodName = method.getName();
				
				//scan all the method names down from this one to check for another with the same length
				int k = j;
				while (--k>=0) {
					Method method2 = methods[k];			
					ProngTemplateField fieldAnnonation2 = method.getAnnotation(ProngTemplateField.class);
					if (null!=fieldAnnonation2) {
						String methodName2 = method.getName();		
						if (methodName2.length()== methodName.length()) {
							//these two methods have a name of the same length
							
							//must find first column that sets them apart.
							
							
						}
					}
				}
				
				
				
			}
		}
					
		
		j = methods.length;
		while (--j>=0) {
			Method method = methods[j];			
			ProngTemplateField fieldAnnonation = method.getAnnotation(ProngTemplateField.class);
			if (null!=fieldAnnonation) {
				
				String name = method.getName();
				
				int fieldLoc = FieldReferenceOffsetManager.lookupFieldLocator(fieldAnnonation.fieldId(), msgIdx, from);		
				
				int key = buildKey(name, c1, c2, c3);
				
				fieldLocs[j] = fieldLoc;
				decimalPlaces[j] = fieldAnnonation.decimalPlaces();
				types[j] = (fieldLoc >> FieldReferenceOffsetManager.RW_FIELD_OFF_BITS) & TokenBuilder.MASK_TYPE;
								
				IntHashTable.setItem(hashTable, key, j);	
								
			}
		}
		
		
	}
	
	//could make this long however do we really need to?
	public int buildKey(String value, int c1, int c2, int c3) {
		
		return ((int)value.length()<<24) | ((int)value.charAt(c1)<<16) | ((int)value.charAt(c2)<<8) | ((int)value.charAt(c3));
				
	}
	
	
	@Override
	public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {							
		int idx = IntHashTable.getItem(hashTable, buildKey(method.getName(), c1, c2, c3));
		writeForYourType(args, decimalPlaces[idx], fieldLocs[idx], types[idx]);				
		return null;
	}

	private void writeForYourType(Object[] args, int decimalPlaces, int fieldLoc, int extractedType) {
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
				RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(), decimalPlaces);	
				break;
			case 13:
				writeOptionalDecimal(args, decimalPlaces, fieldLoc);
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

	private void writeOptionalDecimal(Object[] args, int decimalPlaces, int fieldLoc) {
		if (null==args[0]) {
			RingWriter.writeDecimal(outputRing, fieldLoc, FieldReferenceOffsetManager.getAbsent32Value(from), FieldReferenceOffsetManager.getAbsent64Value(from));
		} else {
			RingWriter.writeDouble(outputRing, fieldLoc, ((Number)args[0]).doubleValue(), decimalPlaces);	
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
