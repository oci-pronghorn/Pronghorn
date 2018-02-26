package com.ociweb.pronghorn.struct;

import java.io.IOException;
import java.lang.reflect.Method;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class BStructSchema {
	
	//selected headers, route params, plus json payloads makes up a fixed record structure
	private int structCount = 0;
	
	private TrieParser[]     fields = new TrieParser[4]; //grow as needed for fields
	private int[]            fieldCount = new int[4]; //count of fields (size of index)
	private String[][]       fieldNames = new String[4][];
	private BStructTypes[][] fieldTypes = new BStructTypes[4][];
	private int[][]          fieldDims  = new int[4][];
		
	
	public BStructSchema merge(BStructSchema source) {
		
		BStructSchema result = new BStructSchema();
		
		for(int i=0; i<structCount; i++) {			
			result.addStruct(fieldNames[i], fieldTypes[i], fieldDims[i]);			
		}
		for(int i=0; i<source.structCount; i++) {			
			result.addStruct(source.fieldNames[i], source.fieldTypes[i], source.fieldDims[i]);			
		}	
		
		return result;
	}
	
	public String toString() {
		return asSource(new StringBuilder()).toString();	
	}

	public <A extends Appendable> A asSource(A target) {
		
		try {
			target.append(BStructSchema.class.getSimpleName());
			target.append(" bss = new ").append(BStructSchema.class.getSimpleName()).append("();\n");
			
			String methodName = null;
			Method[] m = BStructSchema.class.getMethods();
			int i = m.length;
			while (--i>=0) {
				Class<?>[] parameterTypes = m[i].getParameterTypes();
				if (3 == parameterTypes.length 
						
					&& parameterTypes[0].getComponentType().isArray()
					&& parameterTypes[0].getComponentType() == String.class
					&& parameterTypes[1].getComponentType().isArray()
					&& parameterTypes[1].getComponentType() == BStructTypes.class
					&& parameterTypes[2].getComponentType().isArray()
					&& parameterTypes[2].getComponentType() == Integer.TYPE
				 ) {					
					methodName = m[i].getName();					
				}
			}
			
			for(int j = 0; j<structCount; j++) {
				target.append("bss.").append(methodName).append("(");
				target.append("new String[]{");
				
				String[] names = fieldNames[j];				
				for(int n=1; n<names.length; n++) {					
					if (n==0) {
						target.append("\"");
					} else {
						target.append(",\"");
					}
					target.append(names[n]).append("\"");					
				}
				target.append("}, new ").append(BStructTypes.class.getName()).append("[]{");
				
				
				BStructTypes[] type = fieldTypes[j];				
				for(int t=1; t<type.length; t++) {					
					if (t!=0) {
						target.append(",");
					}
					target.append(BStructTypes.class.getName()).append('.');
					target.append(type[t].name());			
				}
				target.append("}, new int[]{");
				
				
				int[] dims = fieldDims[j];				
				for(int t=1; t<dims.length; t++) {					
					if (t!=0) {
						target.append(",");
					}
					Appendables.appendValue(target, dims[t]);
				}
				target.append("});\n");
				
			}
			
			
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		
		return target;
	}
	

	
	public void addStruct(String[] fieldNames, 
			              BStructTypes[] fieldTypes, //all fields are precede by array count byte
			              int[] fieldDims //Dimensionality, should be 0 for simple objects.
						 ) {
		
		assert(fieldNames.length == fieldTypes.length);
		assert(fieldNames.length == fieldDims.length);
		
		int idx = structCount++;
		grow(structCount);
		
		//TODO: types need to match Phast and all others??
		
		int n = fieldNames.length;
		TrieParser fieldParser = new TrieParser(n*20,2,true,false);
		while (--n>=0) {
			fieldParser.setUTF8Value(fieldNames[n], n);
		}
		this.fields[idx] = fieldParser;
		
		this.fieldCount[idx] = fieldNames.length;
		this.fieldNames[idx] = fieldNames;
		this.fieldTypes[idx] = fieldTypes;
		this.fieldDims[idx] = fieldDims;
	}
	
	
	public BStructTypes fieldType(int record, int fieldId) {
		return fieldTypes[record][fieldId];
	}
	
	public String fieldName(int record, int fieldId) {
		return fieldNames[record][fieldId];
	}
	
	public int fieldCount(int record) {
		return fieldCount[record];
	}
	    	
	public long fieldLookup(TrieParserReader reader, CharSequence sequence, int record) {
		return TrieParserReader.query(reader, fields[record], sequence);
	}
	
	public long fieldLookup(TrieParserReader reader, byte[] source, int pos, int len, int mask, int record) {
		return TrieParserReader.query(reader, fields[record], source, pos, len, mask);
	}
	
	private void grow(int records) {
		if (records>fields.length) {
			int newSize = records*2;
			
			fields = grow(newSize, fields);
			fieldCount = grow(newSize, fieldCount);
			fieldNames = grow(newSize, fieldNames);
			fieldTypes = grow(newSize, fieldTypes);
			
		}
	}


	private BStructTypes[][] grow(int newSize, BStructTypes[][] source) {
		BStructTypes[][] result = new BStructTypes[newSize][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}


	private String[][] grow(int newSize, String[][] source) {
		String[][] result = new String[newSize][];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}


	private int[] grow(int newSize, int[] source) {
		int[] result = new int[newSize];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}


	private TrieParser[] grow(int newSize, TrieParser[] source) {
		TrieParser[] result = new TrieParser[newSize];
		System.arraycopy(source, 0, result, 0, source.length);
		return result;
	}
	
	
}