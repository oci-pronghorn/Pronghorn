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

	private final PrimitiveWriter writer;
		
    public TemplateHandler(FASTOutput output) {
    	writer = new PrimitiveWriter(output);
	}

    //Catalog represents all the templates supported 
    int[] catalogTemplateScript = new int[TokenBuilder.MAX_FIELD_ID_VALUE];//Does not need to be this big.
    int catalogTemplateScriptIdx = 0;
    int[][] catalogScripts = new int[TokenBuilder.MAX_FIELD_ID_VALUE][];
    
    //Name space for all the active templates if they do not define their own.
    String templatesXMLns; //TODO: name space processing is not implemented yet.
    
    //Templates never nest and only appear one after the other. Therefore 
    //these fields never need to be in a stack and the values put here by the
    //start will still be there for end.
    int    templateId;
    int 	templateIdBiggest = 0;
    int 	templateIdUnique = 0;
    byte[] templateLookup = new byte[TokenBuilder.MAX_FIELD_ID_VALUE]; //checking for unique templateId 
    String templateName;
    String templateReset;
    String templateDictionary;
    String templateXMLns;
    
    
   // Fields never nest and only appear one after the other.
    int     fieldId;
    int 	fieldIdBiggest = 0;
    int 	fieldIdUnique = 0;
    int[]   tokenLookupFromFieldId = new int[TokenBuilder.MAX_FIELD_ID_VALUE];
    long[]  absentLookupFromFieldId = new long[TokenBuilder.MAX_FIELD_ID_VALUE];
    int     fieldType;
    int 	fieldOperator;
    String  fieldName;
    String  fieldDictionary;
    String  fieldDictionaryKey;
    
    boolean fieldExponentOptional = false;
    int      fieldExponentAbsent;
    boolean fieldMantissaOptional = false;
    long     fieldMantissaAbsent;
    int      fieldPMapInc = 1;//changes to 2 only when inside twin decimal
    
    //Counters for TokenBuilder so each field is given a unique spot in the dictionary.
    int tokenBuilderIntCount;
    int tokenBuilderLongCount;
    int tokenBuilderTextCount;
    int tokenBuilderByteCount;
    int tokenBuilderDecimalCount; 
   
    
    //groups can be nested and need a stack, this includes sequence and template.   

    int[] groupOpenTokenPMapStack = new int[TokenBuilder.MAX_FIELD_ID_VALUE];
    int[] groupOpenTokenStack = new int[TokenBuilder.MAX_FIELD_ID_VALUE];//Need not be this big.
    int   groupTokenStackHead = -1;
        
    
    public void startElement(String uri, String localName,
                  String qName, Attributes attributes) throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32")) {
    		
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerUnsignedOptional:
    				TypeMask.IntegerUnsigned;

    		commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    	} else if (qName.equalsIgnoreCase("int32")) {
    		
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerSignedOptional:
    				TypeMask.IntegerSigned;
    		
    		commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);  	
    	} else if (qName.equalsIgnoreCase("uint64")) {
    		
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongUnsignedOptional:
    				TypeMask.LongUnsigned;
    		
    		commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG);
    	} else if (qName.equalsIgnoreCase("int64")) {
    		
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongSignedOptional:
    				TypeMask.LongSigned;
    		
    		commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG);
    	} else if (qName.equalsIgnoreCase("length")) { 
    		
    		fieldType = TypeMask.GroupLength;//NOTE: length is not optional
    		
    		commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    		
    	} else if (qName.equalsIgnoreCase("string")) {
    		if ("unicode".equals(attributes.getValue("charset"))) {
    			//default is required
    			fieldType = "optional".equals(attributes.getValue("presence")) ?
    					TypeMask.TextUTF8Optional:
    					TypeMask.TextUTF8;	
    		} else {
    			//default is ascii 
    			fieldType = "optional".equals(attributes.getValue("presence")) ?
    					TypeMask.TextASCIIOptional:
    					TypeMask.TextASCII;	
    		}
    		commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		fieldPMapInc=2; //any operators must count as two PMap fields.
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.DecimalOptional:
    				TypeMask.Decimal;
    		
    		commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    		
    		fieldExponentOptional = false;
    		fieldMantissaOptional = false;
    		
    		
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		fieldPMapInc=1;
    		fieldExponentOptional = "optional".equals(attributes.getValue("presence"));
    		
			String absentString = attributes.getValue("nt_absent_const");
			if (null!=absentString && absentString.trim().length()>0) {
				fieldExponentAbsent = Integer.parseInt(absentString.trim());
			} else {
				//default value for absent of this type
				fieldExponentAbsent = Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
			}
    		
    	} else if (qName.equalsIgnoreCase("mantissa")) {
    		fieldPMapInc=1;
    		fieldMantissaOptional = "optional".equals(attributes.getValue("presence"));
    		
			String absentString = attributes.getValue("nt_absent_const");
			if (null!=absentString && absentString.trim().length()>0) {
				fieldMantissaAbsent = Long.parseLong(absentString.trim());
			} else {
				//default value for absent of this type
				fieldMantissaAbsent = Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
			}
    		
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		fieldType = TypeMask.ByteArray;
    		fieldId = Integer.valueOf(attributes.getValue("id"));
    		fieldName = attributes.getValue("name");
    		
    	} else if (qName.equalsIgnoreCase("copy")) { 
    		fieldOperator = OperatorMask.Field_Copy;
    		groupOpenTokenPMapStack[groupTokenStackHead]+=fieldPMapInc;
    		
    	} else if (qName.equalsIgnoreCase("constant")) {
    		fieldOperator = OperatorMask.Field_Constant;
    		if ((fieldType&1)!=0) {
    			groupOpenTokenPMapStack[groupTokenStackHead]+=fieldPMapInc;//optional constant does but required does not.
    		}
    		
    	} else if (qName.equalsIgnoreCase("default")) {
    	    fieldOperator = OperatorMask.Field_Default;
    	    groupOpenTokenPMapStack[groupTokenStackHead]+=fieldPMapInc;
    	    
    	} else if (qName.equalsIgnoreCase("delta")) {
    		fieldOperator = OperatorMask.Field_Delta;
    		//Never uses pmap
    		
    	} else if (qName.equalsIgnoreCase("increment")) {
    	    fieldOperator = OperatorMask.Field_Increment;
    	    groupOpenTokenPMapStack[groupTokenStackHead]+=fieldPMapInc;
    	    
    	} else if (qName.equalsIgnoreCase("tail")) {
    		fieldOperator = OperatorMask.Field_Tail;
    		//Never uses pmap
    		
    	} else if (qName.equalsIgnoreCase("group")) {
    		fieldName = attributes.getValue("name");
    		
    		//Token must hold the max bytes needed for the PMap but this is the start element
    		//and that data is not ready yet. So in the Count field we will put the templateScriptIdx.
    		//upon close of this element the token at that location in the templateScript must have
    		//the Count updated to the right value.
    		int token = TokenBuilder.buildToken(TypeMask.Group,
							    				0, 
							    				catalogTemplateScriptIdx);
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		catalogTemplateScript[catalogTemplateScriptIdx++] = token; 
    		
    	} else if (qName.equalsIgnoreCase("sequence")) {   		
    		
    		fieldName = attributes.getValue("name");
    		
    		//Token must hold the max bytes needed for the PMap but this is the start element
    		//and that data is not ready yet. So in the Count field we will put the templateScriptIdx.
    		//upon close of this element the token at that location in the templateScript must have
    		//the Count updated to the right value.
    		int token = TokenBuilder.buildToken(TypeMask.Group,
							    				OperatorMask.Group_Bit_Seq, 
							    				catalogTemplateScriptIdx);
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		catalogTemplateScript[catalogTemplateScriptIdx++] = token; 
    		

    	} else if (qName.equalsIgnoreCase("template")) {
    		
    		
    		//Token must hold the max bytes needed for the PMap but this is the start element
    		//and that data is not ready yet. So in the Count field we will put the templateScriptIdx.
    		//upon close of this element the token at that location in the templateScript must have
    		//the Count updated to the right value.
    		int token = TokenBuilder.buildToken(TypeMask.Group,
							    				0, 
							    				catalogTemplateScriptIdx);
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		catalogTemplateScript[catalogTemplateScriptIdx++] = token; 
    		
    		//template specific values after this point
    		
    		templateId = Integer.valueOf(attributes.getValue("id"));
    		if (fieldId<0) {
    			throw new SAXException("Template Id must be positive: "+fieldId);
    		} else {
    			templateIdBiggest = Math.max(templateIdBiggest,templateId);
    		}
    		
    		templateName = attributes.getValue("name");
    	    templateReset = attributes.getValue("reset");
    	    templateDictionary = attributes.getValue("dictionary");
    	    templateXMLns = attributes.getValue("xmlns");
    	    catalogTemplateScriptIdx = 0;
    	    
    	    //CREATE NEW TEMPLATE OBJECT FOR ADDING STUFF.
    	    commonIdAttributes(attributes, Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    	    
    	} else if (qName.equalsIgnoreCase("templates")) {
    		
    		templatesXMLns = attributes.getValue("xmlns");
    		
    		//SAVE DATA THAT MAY BE HELPFULL FOR TEMPLATES
    		
    	}
    }

	private void commonIdAttributes(Attributes attributes, long defaultAbsent) throws SAXException {
		fieldId = Integer.valueOf(attributes.getValue("id"));
		if (fieldId<0) {
			throw new SAXException("Field Id must be positive: "+fieldId);
		} else {
			fieldIdBiggest = Math.max(fieldIdBiggest,fieldId);
		}
		fieldName = attributes.getValue("name"); 
		
		//rare: used when we want special dictionary.
		fieldDictionary = attributes.getValue("dictionary");
		//more rare: used when we want to read last value from another field.
		fieldDictionaryKey = attributes.getValue("key");
		
		String absentString = attributes.getValue("nt_absent_const");
		if (null!=absentString && absentString.trim().length()>0) {
			absentLookupFromFieldId[fieldId] = Long.parseLong(absentString.trim());
		} else {
    		//default value for absent of this type
			absentLookupFromFieldId[fieldId] = defaultAbsent;
		}
	}

    public void endElement(String uri, String localName, String qName)
                  throws SAXException {
    	
    	if (qName.equalsIgnoreCase("uint32") || qName.equalsIgnoreCase("int32")) {
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderIntCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    		
    	} else if (qName.equalsIgnoreCase("uint64") || qName.equalsIgnoreCase("int64")) {
       		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderLongCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    	
    	} else if (qName.equalsIgnoreCase("string")) {
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderTextCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    		
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			
    			//TODO: check for expontent and mantissa data for double fields/
    			
    			//Build special fieldOperator. Can clear them now if we like.
    			
    			//if undefined create new token
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderDecimalCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;

    		fieldPMapInc=1;//set back to 1 we are leaving decimal processing
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderByteCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    		    	
    	} else if (qName.equalsIgnoreCase("group") ||
    			    qName.equalsIgnoreCase("sequence") ||
    			    qName.equalsIgnoreCase("template")
    			 ) {
    		
    		int pmapBits = groupOpenTokenPMapStack[groupTokenStackHead];
    		int pmapBytes = (pmapBits+6)/7; //if bits is zero this will be zero.
    		int opMask = OperatorMask.Group_Bit_Close;
    		int openToken = groupOpenTokenStack[groupTokenStackHead];
    		if (pmapBytes>0) {
    			opMask |= OperatorMask.Group_Bit_PMap;
    			openToken |= (OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER);
    		}
    		int scriptOpenGroupIdx = TokenBuilder.extractCount(openToken);
    		//open token has max bytes required for pmap or zero if pmap flag is not set.
    		groupOpenTokenStack[groupTokenStackHead] = (TokenBuilder.MAX_FIELD_MASK&openToken) |
    				                                   (TokenBuilder.MAX_FIELD_ID_VALUE&pmapBytes);
    		
    		int jumpToTop = catalogTemplateScriptIdx-scriptOpenGroupIdx;
    		
    		//closing token has positive jump back to head, in case it is needed 
    		int token = TokenBuilder.buildToken(TypeMask.Group, opMask, jumpToTop);
    		catalogTemplateScript[catalogTemplateScriptIdx++] = token; 
    		
    		groupTokenStackHead--;//pop this group off the stack to work on the previous.

    		if (qName.equalsIgnoreCase("template")) {
        		        		
        		if (0!=templateLookup[templateId]) {
        			throw new SAXException("Duplicate template id: "+templateId);
        		}
        		templateLookup[templateId] = 1;
        		
        		//give this script to the catalog
        		int[] script = new int[catalogTemplateScriptIdx];
        		System.arraycopy(catalogTemplateScript, 0, script, 0, catalogTemplateScriptIdx);
        		catalogScripts[fieldId] = script;
        		templateIdUnique++;     		
        		
        	}
    		
    	} else if (qName.equalsIgnoreCase("templates")) {
    		templatesXMLns = null;
    	}

    }


	private void validate() throws SAXException {
		//if defined use old token but validate it
		int expectedType = TokenBuilder.extractType(tokenLookupFromFieldId[fieldId]);
		if (expectedType != fieldType) {
			throw new SAXException("id: "+fieldId+" can not be defined for both types "+expectedType+" and "+fieldType);
		}
	}
	
	public void postProcessing() {
		
		//write catalog data.
		Catalog.save(writer, fieldIdUnique, fieldIdBiggest, 
				     tokenLookupFromFieldId, absentLookupFromFieldId,
				     templateIdUnique, templateIdBiggest, catalogScripts);
				
		//close stream.
		writer.flush();
		
	}


}
