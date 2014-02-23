//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class TemplateHandler extends DefaultHandler {

	/*
	 * <template dictionary="1" id="1" name="MDIncRefreshSample_1">
    <string id="35" name="MessageType">
        <constant value="X"></constant>
    </string>     <string id="49" name="SenderCompID">
        <constant value="Client"></constant>
    </string>     <string id="56" name="TargetCompID">
        <constant value="Server"></constant>
    </string>     <uint32 id="34" name="MsgSeqNum"></uint32>
    <uint64 id="52" name="SendingTime"></uint64>
    <sequence name="MDEntries">
        <length id="268" name="NoMDEntries"></length>
        <uint32 id="279" name="MDUpdateAction">
            <copy value="1"></copy>
        </uint32>
        <string id="269" name="MDEntryType">
            <copy value="0"></copy>
        </string>
        <uint32 id="278" name="MDEntryID"></uint32>
        <uint32 id="48" name="SecurityID">
            <delta></delta>
        </uint32>
        <decimal id="270" name="MDEntryPx">
            <exponent>
                <default value="-2"></default>
            </exponent>
            <mantissa>
                <delta></delta>
            </mantissa>
        </decimal>
        <int32 id="271" name="MDEntrySize">
            <delta></delta>
        </int32>
        <string id="37" name="OrderID"></string>
        <uint32 id="273" name="MDEntryTime">
            <copy></copy>
        </uint32>
    </sequence>
</template>
	 */
	
	DictionaryFactory factory; //create and save as needed.
	
	final PrimitiveWriter writer;
		
    public TemplateHandler(FASTOutput output) {
    	writer = new PrimitiveWriter(output);
	}

   
    int id;
    String name;
    int type;
    int operator;
    long absent;

    final int MAX_FIELDS = 1<<20;//TODO: unify with token builder.
    
    String templateReset;
    String templateDictionary;
    String templateXMLns;
    
    //must contain field id, group open, group close, sequence open close, load repl.
    //high bit flags token and low 18 are field id so the others are free??
    //may need to use long to avoid token lookup?
    
    //these templates scripts are compressed and are expanded to longs upon load.
    
    // 000 field
    // 010 group open
    // 011 group close
    // 110 seq open
    // 111 seq close
    // 001 load repl
    
    //3  bits - group open/close seq open/close, field, repl, (6 operators)
    //18 bits - field id
    //21 bits total  
    
    int[] templateScript = new int[MAX_FIELDS];//Does not need to be this big.
    int templateScriptIdx = 0;
    
    String templatesXMLns;
    
    //groups and sequence need a stack, all others can not be nested.
    
    int sequenceLength;
    
    int intCount;
    int longCount;
    int textCount;
    int byteCount;
    int decimalCount; 

    
    //Catalog data needed for factory
    int[] templateLookup = new int[MAX_FIELDS];
    int[] tokenLookup = new int[MAX_FIELDS];
    long[] absentValue = new long[MAX_FIELDS];
    int biggestId = 0;
    int uniqueIds = 0;
    
    
    int[][] catalogScripts = new int[MAX_FIELDS][];
    int biggestTemplateId;
    int uniqueTemplateIds;
    
    
    
    
    public void startElement(String uri, String localName,
                  String qName, Attributes attributes) throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerUnsignedOptional:
    				TypeMask.IntegerUnsigned;
    		//default value for absent of this type
    		absent = Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
    		//may update to specific absent value
    		commonIdAttributes(attributes);
    		

    		
    	} else if (qName.equalsIgnoreCase("int32")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerSignedOptional:
    				TypeMask.IntegerSigned;
    		
    		commonIdAttributes(attributes);  	
    	} else if (qName.equalsIgnoreCase("uint64")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongUnsignedOptional:
    				TypeMask.LongUnsigned;
    		
    		commonIdAttributes(attributes);
    	} else if (qName.equalsIgnoreCase("int64")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongSignedOptional:
    				TypeMask.LongSigned;
    		
    		commonIdAttributes(attributes);
    	} else if (qName.equalsIgnoreCase("string")) {
    		
    		if ("unicode".equals(attributes.getValue("charset"))) {
    			//default is required
    			type = "optional".equals(attributes.getValue("presence")) ?
    					TypeMask.TextUTF8Optional:
    					TypeMask.TextUTF8;	

    		} else {
    			//default is ascii 
    			type = "optional".equals(attributes.getValue("presence")) ?
    					TypeMask.TextASCIIOptional:
    					TypeMask.TextASCII;	
    			
    		}
    		
    		commonIdAttributes(attributes);
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.DecimalOptional:
    				TypeMask.Decimal;
    		
    		commonIdAttributes(attributes);
    		
    		//clear exponent and mantissa fields
    		//
    		
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		//set exponent attributes
    		//
    		
    	} else if (qName.equalsIgnoreCase("mantissa")) {
    		//set mantissa attributes
    		//
    		
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		type = TypeMask.ByteArray;
    		id = Integer.valueOf(attributes.getValue("id"));
    		name = attributes.getValue("name");
    	} else if (qName.equalsIgnoreCase("copy")) {
    		operator = OperatorMask.Field_Copy;
    	} else if (qName.equalsIgnoreCase("constant")) {
    		operator = OperatorMask.Field_Constant;
    	} else if (qName.equalsIgnoreCase("default")) {
    	    operator = OperatorMask.Field_Default;
    	} else if (qName.equalsIgnoreCase("delta")) {
    		operator = OperatorMask.Field_Delta;
    	} else if (qName.equalsIgnoreCase("increment")) {
    	    operator = OperatorMask.Field_Increment;
    	} else if (qName.equalsIgnoreCase("tail")) {
    		operator = OperatorMask.Field_Tail;
    	} else if (qName.equalsIgnoreCase("group")) {
    		//TODO: create open group token for script
    		templateScript[templateScriptIdx++] = id; //open group in script
    		
    	} else if (qName.equalsIgnoreCase("sequence")) {   		
    		
    		sequenceLength = -1;
    		name = attributes.getValue("name");
    		
    		//TODO: create open group token for script
    		
    		templateScript[templateScriptIdx++] = id; //open sequence in script
    		//TODO: record optional length or -1
    		
    	} else if (qName.equalsIgnoreCase("length")) { 
    		
    		type = TypeMask.GroupLength;
    		//TODO: sequenceLength = Integer.valueOf(attributes.getValue("length"));
    		commonIdAttributes(attributes);
    		
    	} else if (qName.equalsIgnoreCase("template")) {
    		
    	    templateReset = attributes.getValue("reset");
    	    templateDictionary = attributes.getValue("dictionary");
    	    templateXMLns = attributes.getValue("xmlns");
    	    templateScriptIdx = 0;
    	    
    	    //CREATE NEW TEMPLATE OBJECT FOR ADDING STUFF.
    	    commonIdAttributes(attributes);
    	    
    	} else if (qName.equalsIgnoreCase("templates")) {
    		
    		templatesXMLns = attributes.getValue("xmlns");
    		
    		//SAVE DATA THAT MAY BE HELPFULL FOR TEMPLATES
    		
    	}
    }

	private void commonIdAttributes(Attributes attributes) throws SAXException {
		id = Integer.valueOf(attributes.getValue("id"));
		if (id<0) {
			throw new SAXException("Field Id must be positive: "+id);
		} else {
			biggestId = Math.max(biggestId,id);
		}
		name = attributes.getValue("name"); 
		
		//rare: used when we want special dictionary.
		String dictionary = attributes.getValue("dictionary");
		//more rare: used when we want to read last value from another field.
		String key = attributes.getValue("key");
		
		String absentString = attributes.getValue("nt_absent_const");
		if (null!=absentString && absentString.trim().length()>0) {
			absent = Long.parseLong(absentString.trim());
		}
	}

    public void endElement(String uri, String localName, String qName)
                  throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32") || qName.equalsIgnoreCase("int32")) {
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, intCount++);
    			uniqueIds++;
    		} else {
    			validate();   			
    		}
    		
    		templateScript[templateScriptIdx++] = id;
    		
    	} else if (qName.equalsIgnoreCase("uint64") || qName.equalsIgnoreCase("int64")) {
       		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, longCount++);
    			uniqueIds++;
    		} else {
    			validate();   			
    		}
    		
    		templateScript[templateScriptIdx++] = id;
    	
    	} else if (qName.equalsIgnoreCase("string")) {
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, textCount++);
    			uniqueIds++;
    		} else {
    			validate();   			
    		}
    		
    		templateScript[templateScriptIdx++] = id;
    		
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		
    		//TODO: check for expontent and mantissa data for double fields/
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, decimalCount++);
    			uniqueIds++;
    		} else {
    			validate();   			
    		}
    		
    		templateScript[templateScriptIdx++] = id;
    		
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		
    		//close exponent accum

    	} else if (qName.equalsIgnoreCase("mantissa")) {
    	
    		//close mantissa accum
    		
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, byteCount++);
    			uniqueIds++;
    		} else {
    			validate();   			
    		}
    		
    		templateScript[templateScriptIdx++] = id;
    		    	
    	} else if (qName.equalsIgnoreCase("group")) {
    		//TODO: create close group token for script
    		templateScript[templateScriptIdx++] = id; //close group in script
    		
    	} else if (qName.equalsIgnoreCase("sequence")) {   		
    		//TODO: create close group token for script
    		templateScript[templateScriptIdx++] = id; //close sequence in script
    		
    	} else if (qName.equalsIgnoreCase("template")) {
    		
    		//TODO: is this a group with its own open? Only if it need pmap and it might!!!
    		
    		if (0!=templateLookup[id]) {
    			throw new SAXException("Duplicate template id: "+id);
    		}
    		templateLookup[id] = 1;
    		
    		//give this script to the catalog
    		int[] script = new int[templateScriptIdx];
    		System.arraycopy(templateScript, 0, script, 0, templateScriptIdx);
    		catalogScripts[id] = script;
    		biggestTemplateId = Math.max(biggestTemplateId, id);
    		uniqueTemplateIds++;
    		
    		
    		
    	} else if (qName.equalsIgnoreCase("templates")) {
    		//CLEAR TEMPLATES DATA?
    		
    		
    	}

    }


	private void validate() throws SAXException {
		//if defined use old token but validate it
		int expectedType = TokenBuilder.extractType(tokenLookup[id]);
		if (expectedType != type) {
			throw new SAXException("id: "+id+" can not be defined for both types "+expectedType+" and "+type);
		}
	}
	
	public void postProcessing() {
		

		
		//write catalog data.
		//TODO: write catalog id.
		Catalog.save(writer, uniqueIds, biggestId, tokenLookup, absentValue, uniqueTemplateIds, biggestTemplateId, catalogScripts);
		
		
		//close stream.
		writer.flush();
		
	}


}
