package com.ociweb.jfast.loader;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.ociweb.jfast.field.DictionaryFactory;
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
    
    
    int intCount;
    int longCount;
    int textCount;
    int byteCount;
    int decimalCount;    
    
    public void startElement(String uri, String localName,
                  String qName, Attributes attributes) throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerUnsignedOptional:
    				TypeMask.IntegerUnsigned;
    		
    		id = Integer.valueOf(attributes.getValue("id"));
    		name = attributes.getValue("name");
    	} else if (qName.equalsIgnoreCase("int32")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerSignedOptional:
    				TypeMask.IntegerSigned;
    		
    		id = Integer.valueOf(attributes.getValue("id"));
    		name = attributes.getValue("name");    	
    	} else if (qName.equalsIgnoreCase("uint64")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongUnsignedOptional:
    				TypeMask.LongUnsigned;
    		
    		id = Integer.valueOf(attributes.getValue("id"));
    		name = attributes.getValue("name");
    	} else if (qName.equalsIgnoreCase("int64")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongSignedOptional:
    				TypeMask.LongSigned;
    		
    		id = Integer.valueOf(attributes.getValue("id"));
    		name = attributes.getValue("name");
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
    		
    		id = Integer.valueOf(attributes.getValue("id"));
    		name = attributes.getValue("name");
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		
    		type = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.DecimalOptional:
    				TypeMask.Decimal;
    		
    		id = Integer.valueOf(attributes.getValue("id"));
    		name = attributes.getValue("name");
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		
    	} else if (qName.equalsIgnoreCase("mantissa")) {
    	
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
    		
    		name = attributes.getValue("name");
    		
    	} else if (qName.equalsIgnoreCase("length")) { 
    		
    	} else if (qName.equalsIgnoreCase("template")) {
    		
    	    templateReset = attributes.getValue("reset");
    	    templateDictionary = attributes.getValue("dictionary");
    	    templateXMLns = attributes.getValue("xmlns");
    	    
    	    //CREATE NEW TEMPLATE OBJECT FOR ADDING STUFF.
    	    
    	    
    	} else if (qName.equalsIgnoreCase("templates")) {
    		
    		templatesXMLns = attributes.getValue("xmlns");
    		
    		//SAVE DATA THAT MAY BE HELPFULL FOR TEMPLATES
    		
    	}
    }

    public void endElement(String uri, String localName, String qName)
                  throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32")) {
    		int token = TokenBuilder.buildToken(type, operator, intCount++);
    		
    	} else if (qName.equalsIgnoreCase("int32")) {
    		int token = TokenBuilder.buildToken(type, operator, intCount++);
    		
    	} else if (qName.equalsIgnoreCase("uint64")) {
    		int token = TokenBuilder.buildToken(type, operator, longCount++);
    		
    	} else if (qName.equalsIgnoreCase("int64")) {
    		int token = TokenBuilder.buildToken(type, operator, longCount++);
    	
    	} else if (qName.equalsIgnoreCase("string")) {
    		int token = TokenBuilder.buildToken(type, operator, textCount++);
    		
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		int token = TokenBuilder.buildToken(type, operator, decimalCount++);
    		
    	} else if (qName.equalsIgnoreCase("exponent")) {

    	} else if (qName.equalsIgnoreCase("mantissa")) {
    	
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		int token = TokenBuilder.buildToken(type, operator, byteCount++);
    		
    	} else if (qName.equalsIgnoreCase("copy")) {
    		
    	} else if (qName.equalsIgnoreCase("constant")) {
    	
    	} else if (qName.equalsIgnoreCase("default")) {
    	
    	} else if (qName.equalsIgnoreCase("delta")) {
    	
    	} else if (qName.equalsIgnoreCase("increment")) {
    	
    	} else if (qName.equalsIgnoreCase("tail")) {
    	
    	} else if (qName.equalsIgnoreCase("group")) {

    	} else if (qName.equalsIgnoreCase("sequence")) {   		
    		
    	} else if (qName.equalsIgnoreCase("template")) {
    		//CLOSE TEMPLATE OBJECT AND SAVE IT?
    		
    	} else if (qName.equalsIgnoreCase("templates")) {
    		//CLEAR TEMPLATES DATA?
    		
    		
    	}

    }


}
