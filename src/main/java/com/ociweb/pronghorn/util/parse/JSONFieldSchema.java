package com.ociweb.pronghorn.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONType;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.struct.BStructSchema;
import com.ociweb.pronghorn.struct.BStructTypes;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class JSONFieldSchema implements JSONReader {

	 private static final Logger logger = LoggerFactory.getLogger(JSONFieldSchema.class);

	 private static final int PAYLOAD_INDEX_LOCATION = 1; //TODO: remove...
	 
	 private final TrieParser parser;  //immutable once established

	 private int totalCount;  //immutable once established
	 private int maxPathLength;	  //immutable once established 
	 
	 private final int maxFields = 5;
	 private final boolean completeFields = true;
	 private JSONFieldMapping[] mappings;  //immutable once established

	 private final TrieParserReader reader = new TrieParserReader(maxFields, completeFields); //only on startup
	 
	 //TODO: how do we find the position of the nulls when we have headers or params?
	 
	 public JSONFieldSchema(int nullPosition) {
		 
		 this.mappings = new JSONFieldMapping[0];
		 
		 this.parser = new TrieParser(256,2,false,true);
		 JSONStreamParser.populateWithJSONTokens(parser);
			 
	 }

	 @Override
	 public long getDecimalMantissa(int fieldId, ChannelReader reader) {
		
		 long result = 0;
		 
		 int nullPosition = ((DataInputBlobReader<?>)reader).readFromEndLastInt(PAYLOAD_INDEX_LOCATION);
		 if (nullPosition<0) {
			 nullPosition = 0;
		 }
		 
		 if (nullPosition==reader.position()
		    && (mappings[fieldId].type == JSONType.TypeDecimal)) {
			 
			 long nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if (0==(1&nulls)) {
				 result = reader.readPackedLong();
			 }
		 } else {
			
			 int pos = reader.position();
			 reader.position(nullPosition);
			 long nulls = reader.readPackedLong();
			 reader.position(pos);
			 			 
			 if ((0==( (1<<fieldId) & nulls))) {
				 result = reader.readPackedLong();
			 }
		 }
		 
		 return result;
	 }

	 @Override
	 public byte getDecimalPosition(int fieldId, ChannelReader reader) {

		 int nullPosition = ((DataInputBlobReader<?>)reader).readFromEndLastInt(PAYLOAD_INDEX_LOCATION);
		 if (nullPosition<0) {
			 nullPosition = 0;
		 }
		 
		 byte result = 0;
		 if (nullPosition==reader.position()
			  && (mappings[fieldId].type == JSONType.TypeDecimal)) {
			 			 
			 long nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if ((0==(1&nulls))) {
				 long m = reader.readPackedLong();
				 result = reader.readByte();

			 }
			 
		 } else {
			 
			 int pos = reader.position();
			 reader.position(nullPosition);
			 long nulls = reader.readPackedLong();
			 reader.position(pos);
			 			 
			 if ((0==( (1<<fieldId) & nulls))) {
				 long m = reader.readPackedLong();
				 result = reader.readByte();
			 }
			 
		 }
		 
		 return result;
	 }

	 
	 @Override
	 public long getLong(int fieldId, ChannelReader reader) {
	
		 int nullPosition = ((DataInputBlobReader<?>)reader).readFromEndLastInt(PAYLOAD_INDEX_LOCATION);
		 if (nullPosition<0) {
			 nullPosition = 0;
		 }
		 
		 long result = 0;
		 if (nullPosition==reader.position()
		     && (mappings[0].type == JSONType.TypeInteger)) {
			 			 
			 long nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if ((0==(1&nulls))) {
				 result = reader.readPackedLong();
			 }

		 } else {
			 
			 int pos = reader.position();
			 reader.position(nullPosition);
			 long nulls = reader.readPackedLong();
			 reader.position(pos);
			 
			 if ((0==( (1<<fieldId) & nulls))) {
				 result = reader.readPackedLong();
			 }			 
		 }
		 return result;
	 }

	@Override
	 public <A extends Appendable> A getText(int fieldId, ChannelReader reader, A target) {
		
		 int nullPosition = ((DataInputBlobReader<?>)reader).readFromEndLastInt(PAYLOAD_INDEX_LOCATION);
		 if (nullPosition<0) {
			 nullPosition = 0;
		 }
		
		 if (nullPosition==reader.position() && (mappings[0].type == JSONType.TypeString)) {
			 long nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if ((0==(1&nulls))) {
				 int len = reader.readPackedInt();
				 if (len>0) {
					 reader.readUTFOfLength(len, target);
				 }
			 }
			 
		 } else {
			 int pos = reader.position();
			 reader.position(nullPosition);
			 long nulls = reader.readPackedLong();
			 reader.position(pos);
			 
			 if ((0==( (1<<fieldId) & nulls))) {
				 reader.readUTFOfLength(reader.readPackedInt(), target);
			 }
			 
		 }
				 
		 return target;
	 }
	 
	 @Override
	 public boolean wasAbsent(int fieldId, ChannelReader reader) {
		 
		 int nullPosition = ((DataInputBlobReader<?>)reader).readFromEndLastInt(PAYLOAD_INDEX_LOCATION);
		 if (nullPosition<0) {
			 nullPosition = 0;
		 }
		 
		 int pos = reader.position();
		 reader.position(nullPosition);
		 long nulls = reader.readPackedLong();
		 reader.position(pos);
		 
		 return (0!=( (1<<fieldId) & nulls));
	 }
	 
	 @Override
	 public boolean getBoolean(int fieldId, ChannelReader reader) {
		 
		 int nullPosition = ((DataInputBlobReader<?>)reader).readFromEndLastInt(PAYLOAD_INDEX_LOCATION);
		 if (nullPosition<0) {
			 nullPosition = 0;
		 }
		 
		 boolean result = false;
		 if (nullPosition==reader.position() && (mappings[0].type == JSONType.TypeBoolean)) {
			 
			 long nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if (!(0!=(1&nulls))) {
				 result = reader.readByte()>0;
			 }
		 } else {
			 int pos = reader.position();
			 reader.position(nullPosition);
			 long nulls = reader.readPackedLong();
			 reader.position(pos);
			 
			 if (!(0!=( (1<<fieldId) & nulls))) {
				 result = reader.readByte()>0;
			 }
		 }
		
		 return result;
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
		 	
		 long idx = TrieParserReader.query(reader, 
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

	@Override
	public <A extends Appendable> A dump(ChannelReader reader, A out) {
		final int initialPosition = reader.absolutePosition();
		
		long localNulls = reader.readPackedLong();
		try {
			out.append("Null map: ").append(Long.toBinaryString(localNulls)).append("\n");//. NOT CG free..
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		for(int i = 0 ;i<mappings.length; i++) {
			mappings[i].dump(reader, out);
		}
		
		reader.absolutePosition(initialPosition);
		return out;
	}

	@Override
	public void clear() {
	}

	public int toStruct(BStructSchema struct) {
		
		int length = mappings.length;
				
		String[] fieldNames = new String[length];
		BStructTypes[] fieldTypes = new BStructTypes[length];
		int[] fieldDims = new int[length];
		
		int i = length;
		while (--i>=0) {
			JSONFieldMapping mapping = mappings[i];			
			fieldNames[i] = mapping.getName();//TODO: make final and remove getter?
			switch(mapping.type) {
				case TypeString:
					fieldTypes[i] = BStructTypes.Text;
				break;
				case TypeInteger:
					fieldTypes[i] = BStructTypes.Long;
				break;
				case TypeDecimal:
					fieldTypes[i] = BStructTypes.Decimal;
				break;
				case TypeBoolean:
					fieldTypes[i] = BStructTypes.Boolean;
				break;					
			}
			fieldDims[i] = mapping.dimensions();
		}
		return struct.addStruct(fieldNames, fieldTypes, fieldDims);
		
	}

		
	
	 
	 
}
