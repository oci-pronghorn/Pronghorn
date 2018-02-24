package com.ociweb.pronghorn.util.parse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.TrieKeyable;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class JSONStreamParser {	
	
	private static final Logger logger = LoggerFactory.getLogger(JSONStreamParser.class);

	private static final int WHITE_SPACE = 1;
	private static final int WHITE_SPACE_4 = WHITE_SPACE | ((int)0x0D)<<8;
	private static final int WHITE_SPACE_3 = WHITE_SPACE | ((int)0x0A)<<8;
	private static final int WHITE_SPACE_2 = WHITE_SPACE | ((int)0x09)<<8;
	private static final int WHITE_SPACE_1 = WHITE_SPACE | ((int)0x20)<<8;
	private static final int STRING_PART = 3;
	private static final int STRING_PART_75 = STRING_PART | 0x7500;
	private static final int STRING_PART_74 = STRING_PART | 0x0900;
	private static final int STRING_PART_72 = STRING_PART | 0x0D00;
	private static final int STRING_PART_6E = STRING_PART | 0x0A00;
	private static final int STRING_PART_66 = STRING_PART | 0x0C00;
	private static final int STRING_PART_62 = STRING_PART | 0x0800;
	private static final int STRING_PART_2F = STRING_PART | 0x2F00;
	private static final int STRING_PART_5C = STRING_PART | 0x5C00;
	private static final int STRING_PART_22 = STRING_PART | 0x2200;
	private static final int STRING_END = 4;
	private static final int STRING_END_75 = STRING_END | 0x7500;
	private static final int STRING_END_74 = STRING_END | 0x0900;
	private static final int STRING_END_72 = STRING_END | 0x0D00;
	private static final int STRING_END_6E = STRING_END | 0x0A00;
	private static final int STRING_END_66 = STRING_END | 0x0C00;
	private static final int STRING_END_62 = STRING_END | 0x0800;
	private static final int STRING_END_2F = STRING_END | 0x2F00;
	private static final int STRING_END_5C = STRING_END | 0x5C00;
	private static final int STRING_END_22 = STRING_END  | 0x2200;
	private static final int CONTINUED_STRING = 5;
	private static final int NAME_SEPARATOR = 6;
	private static final int END_OBJECT = 7;
	private static final int VALUE_SEPARATOR = 8;
	private static final int BEGIN_ARRAY = 9;
	private static final int BEGIN_OBJECT = 10;
	private static final int END_ARRAY = 11;
	private static final int NUMBER_ID = 12;
	private static final int FALSE_ID = 13;
	private static final int NULL_ID = 14;
	private static final int TRUE_ID = 15;
	
	private static final byte DEFAULT_STATE = 0;
	private static final byte TEXT_STATE = 1;
	
	
	private static final TrieParser defaultParser = defaultParser();
	private static final TrieParser stringEndParser = stringEndParser();
	
	private final ByteConsumerCodePointConverter converter = new ByteConsumerCodePointConverter();
		
	
	public static <T extends Enum<T> & TrieKeyable> TrieParser customParser(Class<T> keys) {

		//2 because we need 2 shorts for the number
		TrieParser trie = new TrieParser(256,2,false,true);
		
		
		for (T key: keys.getEnumConstants()) {			
			int value = toValue(key.ordinal());
			assert(value>=0);
			
			trie.setUTF8Value("\"", key.getKey(), "\"", value);
			//TODO: should we add the same key without quotes ??
			
		}
		populateWithJSONTokens(trie);
		
		return trie;
	}


	public static int toValue(int idx) {
		return idx<<8;
	}

	public static int fromValue(int idx) {
		return idx>>8;
	}

	
	public static void populateWithJSONTokens(TrieParser trie) {
		//code for strings with escape sequences
		trie.setValue(JSONConstants.string221, STRING_PART);
		trie.setValue(JSONConstants.continuedString, CONTINUED_STRING);
		/////
		
		trie.setValue(JSONConstants.ws2, WHITE_SPACE_2);
		trie.setValue(JSONConstants.ws3, WHITE_SPACE_3);
		trie.setValue(JSONConstants.ws4, WHITE_SPACE_4);
		
		trie.setValue(JSONConstants.falseLiteral, FALSE_ID);
		trie.setValue(JSONConstants.nullLiteral, NULL_ID);
		trie.setValue(JSONConstants.trueLiteral, TRUE_ID);
				
		trie.setValue(JSONConstants.beginArray, BEGIN_ARRAY);
		trie.setValue(JSONConstants.endArray, END_ARRAY);			
		
		trie.setValue(JSONConstants.beginObject, BEGIN_OBJECT);
		trie.setValue(JSONConstants.endObject, END_OBJECT);
		trie.setValue(JSONConstants.ws1, WHITE_SPACE_1);
		
		trie.setValue(JSONConstants.number, NUMBER_ID);		
		trie.setValue(JSONConstants.string222, STRING_END); //to captures quoted values
		
		trie.setValue(JSONConstants.valueSeparator, VALUE_SEPARATOR);
		trie.setValue(JSONConstants.nameSeparator, NAME_SEPARATOR);			


	}
	
	
	
	private static TrieParser defaultParser() {
			
	    	TrieParser trie = new TrieParser(256,1,false,true);

			populateWithJSONTokens(trie);
			
			return trie;
	}


	private static TrieParser stringEndParser() {
		
    	TrieParser trie = new TrieParser(256,1,false,true);

		trie.setValue(JSONConstants.string5C1, STRING_PART_5C);
		trie.setValue(JSONConstants.string5C2, STRING_END_5C);
		
		trie.setValue(JSONConstants.string2F1, STRING_PART_2F);
		trie.setValue(JSONConstants.string2F2, STRING_END_2F);
		
		trie.setValue(JSONConstants.string621, STRING_PART_62);
		trie.setValue(JSONConstants.string622, STRING_END_62);
		
		trie.setValue(JSONConstants.string661, STRING_PART_66);
		trie.setValue(JSONConstants.string662, STRING_END_66);
		
		trie.setValue(JSONConstants.string6E1, STRING_PART_6E);
		trie.setValue(JSONConstants.string6E2, STRING_END_6E);
		
		trie.setValue(JSONConstants.string721, STRING_PART_72);
		trie.setValue(JSONConstants.string722, STRING_END_72);
		
		trie.setValue(JSONConstants.string741, STRING_PART_74);
		trie.setValue(JSONConstants.string742, STRING_END_74);
		
		trie.setValue(JSONConstants.string751, STRING_PART_75);
		trie.setValue(JSONConstants.string752, STRING_END_75);
				
		trie.setValue(JSONConstants.string221, STRING_PART_22);
		trie.setValue(JSONConstants.string222, STRING_END_22);
						
		return trie;
	}
	
    
	public static TrieParserReader newReader() {
		return new TrieParserReader(2);
	}


	public void parse(TrieParserReader reader, TrieParser customParser, JSONStreamVisitor visitor) {
		
		byte state = DEFAULT_STATE;
		
		while (visitor.isReady()) {
			if (DEFAULT_STATE == state) {
				
				//StringBuilder builder = new StringBuilder();
				//TrieParserReader.debugAsUTF8(reader, builder, 180);
				
				int pos = reader.sourcePos;
				
				final int id  = (int)TrieParserReader.parseNext(reader, customParser);
				
				//logger.info("start pos {} position is now {} vs ring buffer len {}", 
				//		pos, reader.sourcePos, reader.sourceLen);
				
				if (-1 == id) {
					assert(pos == reader.sourcePos) : "did not return to start position";
				}
				
				//logger.info("log event {}  ",id);
				
				
				//customParser.toDOT(System.out);
				
				switch (id) {
					case STRING_PART: //start of string change mode
						state = TEXT_STATE;
						visitor.stringBegin();
						TrieParserReader.capturedFieldBytes(reader, 0, visitor.stringAccumulator());
						break;
		            case CONTINUED_STRING: //continue string change mode
						//we have no string captured this is just a flag to change modes
		            	state = TEXT_STATE;	            	
						break;					
					case STRING_END: //full string
						visitor.stringBegin();
						TrieParserReader.capturedFieldBytes(reader, 0, visitor.stringAccumulator());
						visitor.stringEnd();
						break;
					case NAME_SEPARATOR:        // :
						visitor.nameSeparator();
						break;
					case BEGIN_OBJECT:	
						visitor.beginObject(); // {
						break;
					case END_OBJECT:
						visitor.endObject(); // }
						break;				
					case BEGIN_ARRAY:
						visitor.beginArray(); // [
						break;					
					case END_ARRAY:
						visitor.endArray();  // ]
						break;					
					case VALUE_SEPARATOR:    // ,
						visitor.valueSeparator();
						break;					
					case WHITE_SPACE_1:
					case WHITE_SPACE_2:
					case WHITE_SPACE_3:
					case WHITE_SPACE_4:						
						visitor.whiteSpace((byte)(id>>8));  // white space
						break;					
					case NUMBER_ID:
				   	    visitor.numberValue(TrieParserReader.capturedDecimalMField(reader, 0),TrieParserReader.capturedDecimalEField(reader, 0));
						break;					
					case FALSE_ID:
						visitor.literalFalse();
						break;					
					case NULL_ID:
						visitor.literalNull();
						break;					
					case TRUE_ID:
						visitor.literalTrue();
						break;
					case -1:
						//if less than longest known this is not an error we just need more data...
						//TODO:confirm grows.
						if (reader.parseHasContentLength(reader) > customParser.longestKnown()) {
							System.err.println("at position "+reader.sourcePos);
							System.err.print("Unable to parse: '");
							TrieParserReader.debugAsUTF8(reader, System.err,100,false);
							System.err.println("'");
							
							TrieParserReader.debugAsArray(reader, System.err, 80);
							System.err.println();
							if (reader.sourceLen>80) {
								System.err.println("warning we have "+reader.sourceLen+" total.");
							}
							
							throw new RuntimeException("check that JSON tags are expected.");
						}
						
						return;
					default:
					
						//the only values here are the ones matching the custom strings 	
						visitor.customString(fromValue(id));	
				}			
				
			} else {
				//text state;
				
				int id = (int)TrieParserReader.parseNext(reader, stringEndParser);
				
				//logger.info("log text {} ",id);
				
				if (id!=-1) {
					int type = 0xFF&id;
					int value = (fromValue(id));
					
					if (0x75!=value) {
					
						visitor.stringAccumulator().consume((byte)value);
						TrieParserReader.capturedFieldBytes(reader, 0, visitor.stringAccumulator());				
					
					} else {				
												
						// uXXXX 4HexDig conversion
						converter.setTarget(visitor.stringAccumulator());
						TrieParserReader.capturedFieldBytes(reader, 0, converter);
									
					}
					
					if (STRING_END == type) {
						state = DEFAULT_STATE;
						visitor.stringEnd();
					} 
				} else {
					//TrieParserReader.debugAsUTF8(reader, System.err);
					reader.moveBack(1);//we need the new call to see teh slash
					return;
				}
				
			}
			
		};
		
	}
	
    public void parse(TrieParserReader reader, JSONStreamVisitor visitor) {

		
		byte state = DEFAULT_STATE;
		
		while (visitor.isReady()) {
			if (DEFAULT_STATE == state) {
				
				int id  = (int)TrieParserReader.parseNext(reader, defaultParser);
				
				//logger.info("log event {} ",id);
				
				switch (id) {
					case STRING_PART: //start of string change mode
						state = TEXT_STATE;
						visitor.stringBegin();
						TrieParserReader.capturedFieldBytes(reader, 0, visitor.stringAccumulator());
						break;
		            case CONTINUED_STRING: //continue string change mode
						//we have no string captured this is just a flag to change modes
		            	state = TEXT_STATE;	            	
						break;					
					case STRING_END: //full string
						visitor.stringBegin();
						TrieParserReader.capturedFieldBytes(reader, 0, visitor.stringAccumulator());
						visitor.stringEnd();
						break;
					case NAME_SEPARATOR:        // :
						visitor.nameSeparator();
						break;
					case BEGIN_OBJECT:	
						visitor.beginObject(); // {
						break;
					case END_OBJECT:
						visitor.endObject(); // }
						break;				
					case BEGIN_ARRAY:
						visitor.beginArray(); // [
						break;					
					case END_ARRAY:
						visitor.endArray();  // ]
						break;					
					case VALUE_SEPARATOR:    // ,
						visitor.valueSeparator();
						break;					
					case WHITE_SPACE_1:
					case WHITE_SPACE_2:
					case WHITE_SPACE_3:
					case WHITE_SPACE_4:						
						visitor.whiteSpace((byte)(id>>8));  // white space
						break;					
					case NUMBER_ID:
					    visitor.numberValue(TrieParserReader.capturedDecimalMField(reader, 0),TrieParserReader.capturedDecimalEField(reader, 0));
						break;					
					case FALSE_ID:
						visitor.literalFalse();
						break;					
					case NULL_ID:
						visitor.literalNull();
						break;					
					case TRUE_ID:
						visitor.literalTrue();
						break;
					case -1:						
						//TrieParserReader.debugAsUTF8(reader, System.err);						
						return;
				}			
				
			} else {
				//text state;
				
				int id = (int)TrieParserReader.parseNext(reader, stringEndParser);
				
				//logger.info("log text {} ",id);
				
				if (id!=-1) {
					int type = 0xFF&id;
					int value = (fromValue(id));
					
					if (0x75 != value) {
						
						visitor.stringAccumulator().consume((byte)value);
						TrieParserReader.capturedFieldBytes(reader, 0, visitor.stringAccumulator());
						
					} else {			
						
						// uXXXX 4HexDig conversion
						converter.setTarget(visitor.stringAccumulator());
						TrieParserReader.capturedFieldBytes(reader, 0, converter);
						
					}
					
					if (STRING_END == type) {
						state = DEFAULT_STATE;
						visitor.stringEnd();
					} 
				} else {
					//TrieParserReader.debugAsUTF8(reader, System.err);
					reader.moveBack(1);//we need the new call to see teh slash
					//exit the parse because we have run out of data, will continue later
					return;  
				}
				
			}
		}
		
	}



}
