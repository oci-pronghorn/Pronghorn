package com.ociweb.pronghorn.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONType;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.struct.StructRegistry;
import com.ociweb.pronghorn.struct.StructTypes;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;
import com.ociweb.pronghorn.util.TrieParserReaderLocal;

public class JSONFieldSchema {

	 private static final Logger logger = LoggerFactory.getLogger(JSONFieldSchema.class);

	 private static final int PAYLOAD_INDEX_LOCATION = 1; //TODO: remove...
	 
	 private final TrieParser parser;  //immutable once established

	 private int totalCount;  //immutable once established
	 private int maxPathLength;	  //immutable once established 
	 
	 private final int maxFields = 5;
	 private final boolean completeFields = true;
	 private JSONFieldMapping[] mappings;  //immutable once established

	 public JSONFieldSchema(int nullPosition) {
		 
		 this.mappings = new JSONFieldMapping[0];
		 
		 this.parser = new TrieParser(256,2,false,true);
		 JSONStreamParser.populateWithJSONTokens(parser);
			 
	 }	 

	 
	 public int mappingCount() {
		 return mappings.length;
	 }
	 
	 public JSONFieldMapping getMapping(int idx) {
		 return mappings[idx];
	 }
	 
	 public void addMappings(JSONFieldMapping mapping) {
		 int newLen = mappings.length+1;
		 JSONFieldMapping[] newArray = new JSONFieldMapping[newLen];
		 System.arraycopy(mappings, 0, 
				 		  newArray, 0, 
				          mappings.length);
		 newArray[mappings.length] = mapping;
		 mappings = newArray;		 
	 }

	 public TrieParser parser() {
		return parser;
	 }
	 
	 public int lookupId(String text) {
		 //adds new one if it is not found.
		 	
		 long idx = TrieParserReader.query(TrieParserReaderLocal.get(), 
				                           parser, 
				                           text);
		 
		 if (idx < 0) {
			 idx = ++totalCount;
			 
			 int hashVal = JSONStreamParser.toValue((int)idx);
			
			 parser.setUTF8Value(text, hashVal);
			 
			 //This pattern may cause an alt check of string capture
			 //NOTE: this is only true for the keys
			 parser.setUTF8Value("\"", text, "\"", hashVal);
		
			 //logger.info("added token {} with value {} to parser", value, hashVal);
			 
		 } else {
			 idx = JSONStreamParser.fromValue((int)idx);
		 }
		 
		 return (int)idx;
	 }

	//used for moving all the fields down as we generate a hash for this unique path 
	public long maxFieldUnits() {
		return totalCount + maxPathLength;
	}
	
	public int uniqueFieldsCount() {
		return totalCount;
	}

	public void recordMaxPathLength(int length) {
		maxPathLength = Math.max(maxPathLength, length);
	}


	public void addToStruct(StructRegistry struct, int structId) {
		
		int length = mappings.length;
						
		int i = length;
		while (--i>=0) {
			JSONFieldMapping mapping = mappings[i];		
			StructTypes fieldType = null;
			switch(mapping.type) {
				case TypeString:
					fieldType = StructTypes.Text;
				break;
				case TypeInteger:
					fieldType = StructTypes.Long;
				break;
				case TypeDecimal:
					fieldType = StructTypes.Decimal;
				break;
				case TypeBoolean:
					fieldType = StructTypes.Boolean;
				break;					
			}
			long fieldId = struct.growStruct(structId, fieldType, mapping.dimensions(), mapping.getName().getBytes());
			Object assoc = mapping.getAssociatedObject();
			if (null!=assoc) {
				if (!struct.setAssociatedObject(fieldId, assoc)) {
					throw new UnsupportedOperationException("An object with the same identity hash is already held, can not add "+assoc);
				}
			}
		}
	}

	public int[] indexTable(StructRegistry typeData, int structId) {
		if ((StructRegistry.IS_STRUCT_BIT&structId) == 0  || structId<0) {
			throw new UnsupportedOperationException("invalid structId");
		}		
		
		int[] table = new int[mappings.length];
		
		int t = table.length;
		while(--t>0) {
			long fieldId = typeData.fieldLookup(mappings[t].getName(), structId);
			assert(fieldId!=-1) : "bad field name "+mappings[t].getName()+" not found in struct";
			table[t] = (StructRegistry.FIELD_MASK & (int)fieldId);
		}
		return table;

	}
	 
	 
}
