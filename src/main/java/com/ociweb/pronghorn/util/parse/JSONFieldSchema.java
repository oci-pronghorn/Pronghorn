package com.ociweb.pronghorn.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.json.JSONType;
import com.ociweb.pronghorn.pipe.ChannelReader;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class JSONFieldSchema implements JSONReader{

	 private static final Logger logger = LoggerFactory.getLogger(JSONFieldSchema.class);
	 
	 private final TrieParserReader parserReader; //only used for the lookup of Id values
	 
	 private final TrieParser parser;  //immutable once established

	 private int totalCount;  //immutable once established
	 private int maxPathLength;	  //immutable once established 
	 
	 private final int maxFields = 5;
	 private final boolean completeFields = true;
	 private JSONFieldMapping[] mappings;  //immutable once established
	 
	 //can read 0 or nextReadField
	 private int nextReadField = 0;
	 private long nulls=0;
	 private byte decimalLatch = 0;

	 @Override
	 public long getDecimalMantissa(byte[] field, ChannelReader reader) {
		 long result = 0;
		 if (mappings[0].nameEquals(field) && (mappings[0].type == JSONType.TypeDecimal)) {
			 
			 //TODO: revist later
			 ((DataInputBlobReader)reader).setPositionBytesFromStart(0);
			 
			 nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if (!(0!=(1&nulls))) {
				 result = reader.readPackedLong();
			 }
			 nextReadField = 0;
			 
			 decimalLatch |= 1;
			 
			 if (3==decimalLatch) {
				 decimalLatch = 0;
				 nextReadField++;				 
			 }
			 
		 } else if (mappings[nextReadField].nameEquals(field) && (mappings[nextReadField].type == JSONType.TypeDecimal)) {
			 if (!(0!=( (1<<nextReadField) & nulls))) {
				 result = reader.readPackedLong();
			 }
			 
			 decimalLatch |= 1;
			 
			 if (3==decimalLatch) {
				 decimalLatch = 0;
				 nextReadField++;				 
			 }
			 
		 } else {
			 throw new UnsupportedOperationException("Fields must be called for in the same order and type as defined");
		 }
		 
		 return result;
	 }

	 @Override
	 public byte getDecimalPosition(byte[] field, ChannelReader reader) {
		 
		 if (1!=decimalLatch) {
			 throw new UnsupportedOperationException("must call for DecimalMantissa value first");
		 }
		 
		 byte result = 0;
		 if (mappings[0].nameEquals(field) && (mappings[0].type == JSONType.TypeDecimal)) {
			 			 
			 //no need to read nulls since that was done by Mantissa read
			 
			 if (!(0!=(1&nulls))) {
				 result = reader.readByte();
			 }
			 nextReadField = 0;
			 
			 decimalLatch |= 2;
			 
			 if (3==decimalLatch) {
				 decimalLatch = 0;
				 nextReadField++;				 
			 }
			 
		 } else if (mappings[nextReadField].nameEquals(field) && (mappings[nextReadField].type == JSONType.TypeDecimal)) {
			 if (!(0!=( (1<<nextReadField) & nulls))) {
				 result = reader.readByte();
			 }
			 
			 decimalLatch |= 2;
			 
			 if (3==decimalLatch) {
				 decimalLatch = 0;
				 nextReadField++;				 
			 }
			 
		 } else {
			 throw new UnsupportedOperationException("Fields must be called for in the same order and type as defined");
		 }
		 
		 return result;
	 }
	 
	 
	 @Override
	 public long getLong(byte[] field, ChannelReader reader) {
		 long result = 0;
		 if (mappings[0].nameEquals(field) && (mappings[0].type == JSONType.TypeInteger)) {
			 			 
			 //TODO: revist later
			 ((DataInputBlobReader)reader).setPositionBytesFromStart(0);
			 
			 nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if (!(0!=(1&nulls))) {
				 result = reader.readPackedLong();
			 }
			 nextReadField = 1;
		 } else if (mappings[nextReadField].nameEquals(field) && (mappings[nextReadField].type == JSONType.TypeInteger)) {
			 if (!(0!=( (1<<nextReadField) & nulls))) {
				 result = reader.readPackedLong();
			 }
			 nextReadField++;
		 } else {
			 throw new UnsupportedOperationException("Fields must be called for in the same order and type as defined");
		 }
		 
		 return result;
	 }
	 
	 @Override
	 public <A extends Appendable> A getText(byte[] field, ChannelReader reader, A target) {
		 
		 if (mappings[0].nameEquals(field) && (mappings[0].type == JSONType.TypeString)) {
			 			 
			 //TODO: revist later
			 ((DataInputBlobReader)reader).setPositionBytesFromStart(0);
			 
			 nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if (!(0!=(1&nulls))) {
				 int len = reader.readPackedInt();
				 if (len>0) {
					 reader.readUTFOfLength(len, target);
				 }
			 }
			 nextReadField = 1;
		 } else if (mappings[nextReadField].nameEquals(field) && (mappings[nextReadField].type == JSONType.TypeString)) {
			 if (!(0!=( (1<<nextReadField) & nulls))) {
				 reader.readUTFOfLength(reader.readPackedInt(), target);
			 }
			 nextReadField++;
		 } else {
			 throw new UnsupportedOperationException("Fields must be called for in the same order and type as defined");
		 }
		 
		 return target;
	 }
	 
	 @Override
	 public boolean wasAbsent(ChannelReader reader) {
		 assert(nextReadField>0);
		 return (0!=( (1<<(nextReadField-1)) & nulls));
	 }
	 
	 @Override
	 public boolean getBoolean(byte[] field, ChannelReader reader) {
		 boolean result = false;
		 if (mappings[0].nameEquals(field) && (mappings[0].type == JSONType.TypeBoolean)) {
			 nulls = reader.readPackedLong();//only supports 64 fields.
			 
			 if (!(0!=(1&nulls))) {
				 result = reader.readByte()>0;
			 }
			 nextReadField = 1;
		 } else if (mappings[nextReadField].nameEquals(field) && (mappings[nextReadField].type == JSONType.TypeBoolean)) {
			 if (!(0!=( (1<<nextReadField) & nulls))) {
				 result = reader.readByte()>0;
			 }
			 nextReadField++;
		 } else {
			 throw new UnsupportedOperationException("Fields must be called for in the same order and type as defined");
		 }
		 
		 return result;
	 }
	 
	 
	 public JSONFieldSchema() {
		 
		 mappings = new JSONFieldMapping[0];
		 
		 parser = new TrieParser(256,2,false,true);
		 JSONStreamParser.populateWithJSONTokens(parser);
		 parser.enableCache(true);

 		 parserReader = new TrieParserReader(maxFields, completeFields);
		 
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
		 	
		 long idx = TrieParserReader.query(parserReader, 
				                           parser, 
				                           text);
		 
		 if (idx < 0) {
			 idx = ++totalCount;
			 
			 int hashVal = JSONStreamParser.toValue((int)idx);
			 parser.enableCache(false);
			 parser.setUTF8Value(text, hashVal);
			 
			 //This pattern may cause an alt check of string capture
			 //NOTE: this is only true for the keys
			 parser.setUTF8Value("\"", text, "\"", hashVal);
			 parser.enableCache(true);
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

		
	
	 
	 
}
