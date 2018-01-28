package com.ociweb.pronghorn.util.parse;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.util.ByteConsumer;
import com.ociweb.pronghorn.util.TrieParser;
import com.ociweb.pronghorn.util.TrieParserReader;

public class JSONParser {


	
	
	////////////
	//structural
    ///////////
	
	
    private static final int BEGIN_ARRAY = 1;
    private static final int BEGIN_OBJECT = 2;
    
    private static final int END_ARRAY = 3;
    private static final int END_OBJECT = 4;
    
    private static final int NAME_SEPARATOR = 5;
    private static final int VALUE_SEPARATOR = 6;
	
    //////////////
    //literals
    /////////////
    
    private static final int NUMBER_ID = 1;    
    private static final int FALSE_ID = 2;
    private static final int NULL_ID = 3;
    private static final int TRUE_ID = 4;

	////////////
	//strings
	////////////	

	private static final int STRING_END = 2;
	private static final int STRING_PART = 3;	
	
	
	private static TrieParser whiteSpaceParser             = whiteSpaceParser();            //  1 char

	private static TrieParser structureBeginParser         = structureBeginParser();        //  1 char
	private static TrieParser structureArrayEndParser      = structureArrayEndParser();     //  1 char
	private static TrieParser structureObjectEndParser     = structureObjectEndParser();    //  1 char
	private static TrieParser structureNameSeparatorParser = structureNameSeparatorParser();//  1 char
	
	
	//////////////////
	//for the following if we have a -1 we must wait for more data unless blob is full
	//////////////////
	
	private static TrieParser literalParser                = literalParser();               //  unknown length number? max 19?	
	private static TrieParser stringBeginParser            = stringBeginParser();           //  unknown length string? - no string can be larger than pipe blob
	private static TrieParser stringEndParser              = stringEndParser();             //  unknown length string?
	
	
	
	private static TrieParser whiteSpaceParser() {
		
		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(JSONConstants.ws1, 1);
		trie.setValue(JSONConstants.ws2, 2);
		trie.setValue(JSONConstants.ws3, 3);
		trie.setValue(JSONConstants.ws4, 4);
			
		return trie;
	}
	
    private static TrieParser stringBeginParser() { //TODO: double check that slash and " if appearing inside UTF8 encoding do not get picked up as an escape..
		
		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(JSONConstants.string221, STRING_PART | 0x2200);
		trie.setValue(JSONConstants.string222, STRING_END  | 0x2200);
						
		return trie;
	}
    
    private static TrieParser stringEndParser() {
		
    	TrieParser trie = new TrieParser(256,1,false,true);

		trie.setValue(JSONConstants.string5C1, STRING_PART | 0x5C00);
		trie.setValue(JSONConstants.string5C2, STRING_END | 0x5C00);
		
		trie.setValue(JSONConstants.string2F1, STRING_PART | 0x2F00);
		trie.setValue(JSONConstants.string2F2, STRING_END | 0x2F00);
		
		trie.setValue(JSONConstants.string621, STRING_PART | 0x0800); //backspace
		trie.setValue(JSONConstants.string622, STRING_END | 0x0800);
		
		trie.setValue(JSONConstants.string661, STRING_PART | 0x0C00); //FF
		trie.setValue(JSONConstants.string662, STRING_END | 0x0C00);  //FF
		
		trie.setValue(JSONConstants.string6E1, STRING_PART | 0x0A00); //NL
		trie.setValue(JSONConstants.string6E2, STRING_END | 0x0A00);  //NL
		
		trie.setValue(JSONConstants.string721, STRING_PART | 0x0D00); //CR
		trie.setValue(JSONConstants.string722, STRING_END | 0x0D00);  //CR
		
		trie.setValue(JSONConstants.string741, STRING_PART | 0x0900); //tab
		trie.setValue(JSONConstants.string742, STRING_END | 0x0900); //tab
		
		trie.setValue(JSONConstants.string751, STRING_PART | 0x7500);
		trie.setValue(JSONConstants.string752, STRING_END | 0x7500);
		
		trie.setValue(JSONConstants.string221, STRING_PART | 0x2200);
		trie.setValue(JSONConstants.string222, STRING_END  | 0x2200);
						
		return trie;
	}
	
	private static TrieParser structureNameSeparatorParser() {
		
		TrieParser trie = new TrieParser(256,1,false,true);
		trie.setValue(JSONConstants.nameSeparator, NAME_SEPARATOR);		
		return trie;
	}
	
	private static TrieParser structureObjectEndParser() {
		
		TrieParser trie = new TrieParser(128,1,true,false);
		trie.setValue(JSONConstants.endObject, END_OBJECT);
		trie.setValue(JSONConstants.valueSeparator, VALUE_SEPARATOR);
		return trie;
	}
	
	private static TrieParser structureBeginParser() {
		
		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(JSONConstants.beginArray, BEGIN_ARRAY);
		trie.setValue(JSONConstants.beginObject, BEGIN_OBJECT);
		
		return trie;
	}
	
	private static TrieParser structureArrayEndParser() {
		
		TrieParser trie = new TrieParser(128,1,true,false);
		
		trie.setValue(JSONConstants.endArray, END_ARRAY);
		trie.setValue(JSONConstants.valueSeparator, VALUE_SEPARATOR);
		
		return trie;
	}
	
	
	private static TrieParser literalParser() {

		TrieParser trie = new TrieParser(256,1,false,true);
		
		trie.setValue(JSONConstants.number, NUMBER_ID);
		trie.setValue(JSONConstants.falseLiteral, FALSE_ID);
		trie.setValue(JSONConstants.nullLiteral, NULL_ID);
		trie.setValue(JSONConstants.trueLiteral, TRUE_ID);
		
		return trie;
		
	}
	
	/////////////////////////////////////////////////
	/////////////////////////////////////////////////
	
	
	public static TrieParserReader newReader() {
		return new TrieParserReader(4);
	}
	
	public static <A extends Appendable> void parse(Pipe<?> pipe, TrieParserReader reader, JSONVisitor visitor) {
		
		TrieParserReader.parseSetup(reader, pipe);
		
		do {		
			parseValueToken(reader, visitor);		
		} while (TrieParserReader.parseHasContent(reader));
		
	}
	
    public static <A extends Appendable> void parse(DataInputBlobReader<?> input, TrieParserReader reader, JSONVisitor visitor) {

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

	public static <A extends Appendable> void parse(Pipe pipe, int loc, TrieParserReader reader, JSONVisitor visitor) {
		
		TrieParserReader.parseSetup(reader, loc, pipe);
		do {
			parseValueToken(reader, visitor);
		} while (TrieParserReader.parseHasContent(reader));
		
	}
	
	
	private static <A extends Appendable> boolean parseStringValueToken(TrieParserReader reader, JSONVisitor visitor) {
		
		int p = reader.sourcePos;
		long stringId = TrieParserReader.parseNext(reader, stringBeginParser);

		if (-1 == stringId) {
			//this is not a string, no match.
			return false;
		}
		return consumeEscapedString(reader, stringId, visitor.stringValue());
	}

	private static <A extends Appendable> boolean parseStringNameToken(TrieParserReader reader, int instance, JSONVisitor visitor) {
		
		int p = reader.sourcePos;
		long stringId = TrieParserReader.parseNext(reader, stringBeginParser);
		
		if (-1 == stringId) {
			//this is not a string, no match.
			return false;
		}
		return consumeEscapedString(reader, stringId, visitor.stringName(instance));
	}
	
	private static <A extends Appendable> boolean consumeEscapedString(TrieParserReader reader, long stringId, ByteConsumer target) {
		
		TrieParserReader.capturedFieldBytes(reader, 0, target);
		if (STRING_END != (0xFF&stringId)) {
			do {
				stringId = reader.parseNext(reader, stringEndParser);
				if (-1 == stringId) {
					throw new UnsupportedOperationException("Unable to parse text string");
				}		
				target.consume((byte)(stringId>>8));
				TrieParserReader.capturedFieldBytes(reader, 0, target);
				
			} while (STRING_END != (0xFF&stringId));
		}
		
		return true;
	}
	
	
	private static <A extends Appendable> void parseValueToken(TrieParserReader reader, JSONVisitor visitor) {
		
				
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

	private static <A extends Appendable> void parseArray(TrieParserReader reader, JSONVisitor visitor) {
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

	private static <A extends Appendable> void parseObject(TrieParserReader reader, JSONVisitor visitor) {
		visitor.objectBegin();
		/////////////
		//white space
		/////////////
		do {
		} while (TrieParserReader.parseNext(reader, whiteSpaceParser)!=-1);

		parseObjectFields(reader, visitor, 0);
		visitor.objectEnd();
	}
	
	private static <A extends Appendable> void parseObjectFields(TrieParserReader reader, JSONVisitor visitor, int instance) {
		long tokenId;
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
	
	}


	
	
}
