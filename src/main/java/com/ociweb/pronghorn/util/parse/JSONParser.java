package com.ociweb.pronghorn.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;

public class JSONParser {

	// Documentation
	// http://rfc7159.net/rfc7159
	//////////////////
	
	////////////
	//structural
    ///////////
	
	private static final byte[] beginArray = new byte[]{0x5B}; //      [
	private static final byte[] beginObject = new byte[]{(byte)'{'};//0x7B}; //     {
	private static final byte[] endArray = new byte[]{0x5D}; //        ]
	private static final byte[] endObject = new byte[]{(byte)'}'};//0x7D}; //       }
	private static final byte[] nameSeparator = new byte[]{0x3A}; //   :
	private static final byte[] valueSeparator = new byte[]{0x2C}; //  ,
	
    private static final int BEGIN_ARRAY = 1;
    private static final int BEGIN_OBJECT = 2;
    
    private static final int END_ARRAY = 3;
    private static final int END_OBJECT = 4;
    
    private static final int NAME_SEPARATOR = 5;
    private static final int VALUE_SEPARATOR = 6;
	
    //////////////
    //literals
    /////////////
    
    private static final byte[] number = "%i%.".getBytes();//WARNING: this does not match spec exatcly 
                                                           //         no support for scientific notiation
    
    private static final byte[] falseLiteral = new byte[]{0x66,0x61,0x6c,0x73,0x65}; //false
    private static final byte[] nullLiteral = new byte[]{0x6e,0x75,0x6c,0x6c}; //null
    private static final byte[] trueLiteral = new byte[]{0x74,0x72,0x75,0x65}; //true
	
    private static final int NUMBER_ID = 1;    
    private static final int FALSE_ID = 2;
    private static final int NULL_ID = 3;
    private static final int TRUE_ID = 4;
    
    		
	///////////
	//white space
	///////////
	
	private static final byte[] ws1 = new byte[]{0x20}; //Space
	private static final byte[] ws2 = new byte[]{0x09}; //Horizontal tab
	private static final byte[] ws3 = new byte[]{0x0A}; //Line feed or New line
	private static final byte[] ws4 = new byte[]{0x0D}; //Carriage return
	
	////////////
	//strings
	////////////
	
	private static final byte[] string221 = new byte[]{0x22,'%','b',0x5C}; // "
	private static final byte[] string222 = new byte[]{0x22,'%','b',0x22}; // "
		
	private static final byte[] string5C1 = new byte[]{0x5C,'%','b',0x5C}; // \
	private static final byte[] string5C2 = new byte[]{0x5C,'%','b',0x22}; // \
			
	private static final byte[] string2F1 = new byte[]{0x2F,'%','b',0x5C}; // \
	private static final byte[] string2F2 = new byte[]{0x2F,'%','b',0x22}; // \
	
	private static final byte[] string621 = new byte[]{0x62,'%','b',0x5C}; // backspace
	private static final byte[] string622 = new byte[]{0x62,'%','b',0x22}; // backspace
	
	private static final byte[] string661 = new byte[]{0x66,'%','b',0x5C}; // form feed
	private static final byte[] string662 = new byte[]{0x66,'%','b',0x22}; // form feed

	private static final byte[] string6E1 = new byte[]{0x6E,'%','b',0x5C}; // line feed
	private static final byte[] string6E2 = new byte[]{0x6E,'%','b',0x22}; // line feed

	private static final byte[] string721 = new byte[]{0x72,'%','b',0x5C}; // carriage return
	private static final byte[] string722 = new byte[]{0x72,'%','b',0x22}; // carriage return
	
	private static final byte[] string741 = new byte[]{0x74,'%','b',0x5C}; // tab
	private static final byte[] string742 = new byte[]{0x74,'%','b',0x22}; // tab
	
	private static final byte[] string751 = new byte[]{0x75,'%','b',0x5C}; // uXXXX 4HexDig
	private static final byte[] string752 = new byte[]{0x75,'%','b',0x22}; // uXXXX 4HexDig	
	

	private static final int STRING_END = 2;
	private static final int STRING_PART = 3;	
	
	
	private static TrieParser whiteSpaceParser             = whiteSpaceParser();

	private static TrieParser structureBeginParser         = structureBeginParser();
	private static TrieParser structureArrayEndParser      = structureArrayEndParser();
	private static TrieParser structureObjectEndParser     = structureObjectEndParser();
	private static TrieParser structureNameSeparatorParser = structureNameSeparatorParser();
	
	
	private static TrieParser literalParser                = literalParser();
	
	private static TrieParser stringBeginParser            = stringBeginParser();
	private static TrieParser stringEndParser              = stringEndParser();
	
	
	
	private static TrieParser whiteSpaceParser() {
		
		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(ws1, 1);
		trie.setValue(ws2, 2);
		trie.setValue(ws3, 3);
		trie.setValue(ws4, 4);
			
		return trie;
	}
	
    private static TrieParser stringBeginParser() {
		
		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(string221, STRING_PART | 0x2200);
		trie.setValue(string222, STRING_END  | 0x2200);
						
		return trie;
	}
    
    private static TrieParser stringEndParser() {
		
    	TrieParser trie = new TrieParser(256,1,false,true);

		trie.setValue(string5C1, STRING_PART | 0x5C00);
		trie.setValue(string5C2, STRING_END | 0x5C00);
		
		trie.setValue(string2F1, STRING_PART | 0x2F00);
		trie.setValue(string2F2, STRING_END | 0x2F00);
		
		trie.setValue(string621, STRING_PART | 0x6200);
		trie.setValue(string622, STRING_END | 0x6200);
		
		trie.setValue(string661, STRING_PART | 0x6600);
		trie.setValue(string662, STRING_END | 0x6600);
		
		trie.setValue(string6E1, STRING_PART | 0x6E00);
		trie.setValue(string6E2, STRING_END | 0x6E00);
		
		trie.setValue(string721, STRING_PART | 0x7200);
		trie.setValue(string722, STRING_END | 0x7200);
		
		trie.setValue(string741, STRING_PART | 0x7400);
		trie.setValue(string742, STRING_END | 0x7400);
		
		trie.setValue(string751, STRING_PART | 0x7500);
		trie.setValue(string752, STRING_END | 0x7500);
						
		return trie;
	}
	
	private static TrieParser structureNameSeparatorParser() {
		
		TrieParser trie = new TrieParser(256,1,false,true);
		trie.setValue(nameSeparator, NAME_SEPARATOR);		
		return trie;
	}
	
	private static TrieParser structureObjectEndParser() {
		
		TrieParser trie = new TrieParser(128,1,true,false);
		trie.setValue(endObject, END_OBJECT);
		trie.setValue(valueSeparator, VALUE_SEPARATOR);
		return trie;
	}
	
	private static TrieParser structureBeginParser() {
		
		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(beginArray, BEGIN_ARRAY);
		trie.setValue(beginObject, BEGIN_OBJECT);
		
		return trie;
	}
	
	private static TrieParser structureArrayEndParser() {
		
		TrieParser trie = new TrieParser(128,1,true,false);
		
		trie.setValue(endArray, END_ARRAY);
		trie.setValue(valueSeparator, VALUE_SEPARATOR);
		
		return trie;
	}
	
	
	private static TrieParser literalParser() {

		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(number, NUMBER_ID);
		trie.setValue(falseLiteral, FALSE_ID);
		trie.setValue(nullLiteral, NULL_ID);
		trie.setValue(trueLiteral, TRUE_ID);
		
		return trie;
		
	}
	
	/////////////////////////////////////////////////
	/////////////////////////////////////////////////
	
	
	public static TrieParserReader newReader() {
		return new TrieParserReader(4);
	}
	
	public static <A extends Appendable> void parse(Pipe pipe, TrieParserReader reader, JSONVisitor<A> visitor) {
		
		TrieParserReader.parseSetup(reader, pipe);
		
		do {		
			parseValueToken(reader, visitor);		
		} while (TrieParserReader.parseHasContent(reader));
		
	}
	
    public static <A extends Appendable> void parse(DataInputBlobReader<?> input, TrieParserReader reader, JSONVisitor<A> visitor) {

    	DataInputBlobReader.setupParser(input, reader);
    
		do {		
			parseValueToken(reader, visitor);		
		} while (TrieParserReader.parseHasContent(reader));
		
	}
	
	public static void parse(ByteBuffer byteBuffer, TrieParserReader reader, JSONVisitor visitor) {
		
		TrieParserReader.parseSetup(reader, byteBuffer.array(), byteBuffer.position(),  byteBuffer.remaining(), Integer.MAX_VALUE);
	    
		do {		
			parseValueToken(reader, visitor);		
		} while (TrieParserReader.parseHasContent(reader));
			
	}

	public static <A extends Appendable> void parse(Pipe pipe, int loc, TrieParserReader reader, JSONVisitor<A> visitor) {
		
		TrieParserReader.parseSetup(reader, loc, pipe);
		do {
			parseValueToken(reader, visitor);
		} while (TrieParserReader.parseHasContent(reader));
		
	}
	
	
	private static <A extends Appendable> boolean parseStringValueToken(TrieParserReader reader, JSONVisitor<A> visitor) {
		
		int p = reader.sourcePos;
		long stringId = TrieParserReader.parseNext(reader, stringBeginParser);

		if (-1 == stringId) {
			//this is not a string, no match.
			return false;
		}
		return consumeEscapedString(reader, stringId, visitor.stringValue());
	}

	private static <A extends Appendable> boolean parseStringNameToken(TrieParserReader reader, int instance, JSONVisitor<A> visitor) {
		
		int p = reader.sourcePos;
		long stringId = TrieParserReader.parseNext(reader, stringBeginParser);
		
		if (-1 == stringId) {
			//this is not a string, no match.
			return false;
		}
		return consumeEscapedString(reader, stringId, visitor.stringName(instance));
	}
	
	private static <A extends Appendable> boolean consumeEscapedString(TrieParserReader reader, long stringId, A target) {
		TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, target);
		if (STRING_END != (0xFF&stringId)) {
			do {
				stringId = reader.parseNext(reader, stringEndParser);
				if (-1 == stringId) {
					throw new UnsupportedOperationException("Unable to parse text string");
				}		
				try {
					target.append((char)(stringId>>8));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
				TrieParserReader.capturedFieldBytesAsUTF8(reader, 0, target);
				
			} while (STRING_END != (0xFF&stringId));
		}
		
		return true;
	}
	
	
	private static <A extends Appendable> void parseValueToken(TrieParserReader reader, JSONVisitor<A> visitor) {
		
				
		//is literal or number
		long tokenId = TrieParserReader.parseNext(reader, literalParser);
		
        if (-1 == tokenId) {
        	if (parseStringValueToken(reader, visitor)) {
        		visitor.stringValueComplete();
        	} else {
    			/////////////
    		    //white space
    			/////////////
    			do {
    			} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
        		
        		//look for object or array beginning        		
        		tokenId = TrieParserReader.parseNext(reader, structureBeginParser);
        		        		
        		if (BEGIN_OBJECT == tokenId) {
        			      
        			parseObject(reader, visitor);
            		
        		} else if (BEGIN_ARRAY == tokenId) {
        			
        			parseArray(reader, visitor);       	
        			
        		} else {
        			throw new UnsupportedOperationException("Unable to parse  "+reader);
        		}
        	}
        } else {
        	if (NUMBER_ID == tokenId) {
        		
        		long m = TrieParserReader.capturedDecimalMField(reader, 0);
        		byte e = TrieParserReader.capturedDecimalEField(reader, 0);
        		visitor.numberValue(m,e);
        		        		
        	} else {
        		
        		if (NULL_ID == tokenId) {
        			visitor.nullValue();
        		} else {
        			visitor.booleanValue(TRUE_ID == tokenId);
        		}
        	}
        	
        }
	}

	private static <A extends Appendable> void parseArray(TrieParserReader reader, JSONVisitor<A> visitor) {
		long tokenId;
		visitor.arrayBegin();
		/////////////
		//white space
		/////////////
		do {
		} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
		
		int instance = 0;
		do {
			
			visitor.arrayIndexBegin(instance++);
			
			//////////
		    //value (recursive)
			parseValueToken(reader, visitor);
		    //////////
			
			/////////////
		    //white space
			/////////////
			do {
			} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
			
			/////////
			//comma or end
			tokenId = TrieParserReader.parseNext(reader, structureArrayEndParser);
			
			if (-1==tokenId) {
				throw new UnsupportedOperationException("Unable to parse, expected end of array");
			}
			
			/////////////
		    //white space
			/////////////
			do {
			} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
			
		} while (VALUE_SEPARATOR == tokenId);
		visitor.arrayEnd();
	}

	private static <A extends Appendable> void parseObject(TrieParserReader reader, JSONVisitor<A> visitor) {
		visitor.objectBegin();
		/////////////
		//white space
		/////////////
		do {
		} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
		
		long tokenId;
		int instance = 0;
		do {
		
		
			/////////////
			//grab name
			////////////
			if (parseStringNameToken(reader, instance++, visitor)) {
				visitor.stringNameComplete();
			} else {
				throw new UnsupportedOperationException("Unable to parse, expected name");
			}
			
			/////////////
			//white space
			/////////////
			do {
			} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
	        	
			////////////////
			//name divider
			/////////////
			if (-1 == TrieParserReader.parseNext(reader, structureNameSeparatorParser)) {
				throw new UnsupportedOperationException("Unable to parse, expected end of object");
			}
	        	
			/////////////
			//white space
			/////////////
			do {
			} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
	        	
			//////////////
			// grab value (recursive)
			//////////////
			parseValueToken(reader, visitor);
			
			/////////////
			//white space
			/////////////
			do {
			} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
	      
			/////////////////
			//end of object or next field.
			/////////////////
			tokenId = TrieParserReader.parseNext(reader, structureObjectEndParser);
			
			if (-1==tokenId) {
				throw new UnsupportedOperationException("Unable to parse, expected end of object");
			}
			
			/////////////
		    //white space
			/////////////
			do {
			} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);
		
		} while (VALUE_SEPARATOR == tokenId);

		
		visitor.objectEnd();
	}


	
	
}
