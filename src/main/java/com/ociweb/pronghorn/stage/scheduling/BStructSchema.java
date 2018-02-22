package com.ociweb.pronghorn.stage.scheduling;

import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class BStructSchema {
	
	//selected headers, route params, plus json payloads makes up a fixed record structure
	private int recordCount = 0;
	
	private TrieParser[] fields = new TrieParser[4]; //grow as needed for fields
	private int[]        fieldCount = new int[4]; //count of fields (size of index)
	private String[][]   fieldNames = new String[4][];
	private byte[][]     fieldTypes = new byte[4][];
	
	
	//TODO: field type needs to map to fast types?
	public byte fieldType(int record, int fieldId) {
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
	
	public void addRecord(String[] fieldNames, byte[] fieldTypes) {
		
		assert(fieldNames.length == fieldTypes.length);
		
		int idx = recordCount++;
		grow(recordCount);
		
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


	private byte[][] grow(int newSize, byte[][] source) {
		byte[][] result = new byte[newSize][];
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