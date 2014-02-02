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
	
	PrimitiveWriter writer;
	
	
	
    public TemplateHandler(FASTOutput output) {
    	writer = new PrimitiveWriter(output);
	}

//	public void characters(char[] buffer, int start, int length) {
//    }
   
    int id;
    String name;
    int type;
    int operator;
    
    String templateReset;
    String templateDictionary;
    String templateXMLns;
    String templatesXMLns;
    
    //groups and sequence need a stack, all others can not be nested.
    
    int sequenceLength;
    
    int intCount;
    int longCount;
    int textCount;
    int byteCount;
    int decimalCount; 

    final int MAX_FIELDS = 1<<20;//TODO: unify with token builder.
    int[] tokenLookup = new int[MAX_FIELDS];
    int[] templateLookup = new int[MAX_FIELDS];
    int biggestId = 0;
    
    public void startElement(String uri, String localName,
                  String qName, Attributes attributes) throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerUnsignedOptional:
    				TypeMask.IntegerUnsigned;
    		
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
    		operator = OperatorMask.Copy;
    	} else if (qName.equalsIgnoreCase("constant")) {
    		operator = OperatorMask.Constant;
    	} else if (qName.equalsIgnoreCase("default")) {
    	    operator = OperatorMask.Default;
    	} else if (qName.equalsIgnoreCase("delta")) {
    		operator = OperatorMask.Delta;
    	} else if (qName.equalsIgnoreCase("increment")) {
    	    operator = OperatorMask.Increment;
    	} else if (qName.equalsIgnoreCase("tail")) {
    		operator = OperatorMask.Tail;
    	} else if (qName.equalsIgnoreCase("group")) {

    	} else if (qName.equalsIgnoreCase("sequence")) {   		
    		
    		sequenceLength = -1;
    		name = attributes.getValue("name");
    		
    	} else if (qName.equalsIgnoreCase("length")) { 
    		
    		sequenceLength = Integer.valueOf(attributes.getValue("length"));
    		
    	} else if (qName.equalsIgnoreCase("template")) {
    		
    	    templateReset = attributes.getValue("reset");
    	    templateDictionary = attributes.getValue("dictionary");
    	    templateXMLns = attributes.getValue("xmlns");
    	    
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
	}

    public void endElement(String uri, String localName, String qName)
                  throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32") || qName.equalsIgnoreCase("int32")) {
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, intCount++);
    		} else {
    			validate();   			
    		}
    		
    	} else if (qName.equalsIgnoreCase("uint64") || qName.equalsIgnoreCase("int64")) {
       		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, longCount++);
    		} else {
    			validate();   			
    		}
    	
    	} else if (qName.equalsIgnoreCase("string")) {
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, textCount++);
    		} else {
    			validate();   			
    		}
    		
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		
    		//TODO: check for expontent and mantissa data for double fields/
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, decimalCount++);
    		} else {
    			validate();   			
    		}
    		
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		
    		//close exponent accum

    	} else if (qName.equalsIgnoreCase("mantissa")) {
    	
    		//close mantissa accum
    		
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		
    		if (0==tokenLookup[id]) {
    			//if undefined create new token
    			tokenLookup[id] = TokenBuilder.buildToken(type, operator, byteCount++);
    		} else {
    			validate();   			
    		}
    		    	
    	} else if (qName.equalsIgnoreCase("group")) {

    	} else if (qName.equalsIgnoreCase("sequence")) {   		
    		
    	} else if (qName.equalsIgnoreCase("template")) {
    		
    		if (0!=templateLookup[id]) {
    			throw new SAXException("Duplicate template id: "+id);
    		}
    		templateLookup[id] = 1;
    		
    		//CLOSE TEMPLATE OBJECT AND SAVE IT?
    		
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
		
		//close stream.
		
	}


}
