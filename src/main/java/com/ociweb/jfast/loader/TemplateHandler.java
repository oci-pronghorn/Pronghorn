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
    int catalogLargestTemplatePMap = 0;
    int catalogLargestNonTemplatePMap = 0;
    
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
    String  fieldOperatorValue;
    String  fieldName;
    String  fieldDictionary;
    String  fieldDictionaryKey;
    
    boolean fieldExponentOptional = false;
    int      fieldExponentAbsent;
    int      fieldExponentOperator;
    
    boolean fieldMantissaOptional = false;
    long     fieldMantissaAbsent;
    int      fieldMantissaOperator;
    
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
    		fieldOperator = OperatorMask.Field_None;
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerUnsignedOptional:
    				TypeMask.IntegerUnsigned;

    		commonIdAttributes(attributes, TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    	} else if (qName.equalsIgnoreCase("int32")) {
    		fieldOperator = OperatorMask.Field_None;
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.IntegerSignedOptional:
    				TypeMask.IntegerSigned;
    		
    		commonIdAttributes(attributes, TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);  	
    	} else if (qName.equalsIgnoreCase("uint64")) {
    		fieldOperator = OperatorMask.Field_None;
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongUnsignedOptional:
    				TypeMask.LongUnsigned;
    		
    		commonIdAttributes(attributes, TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG);
    	} else if (qName.equalsIgnoreCase("int64")) {
    		fieldOperator = OperatorMask.Field_None;
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.LongSignedOptional:
    				TypeMask.LongSigned;
    		
    		commonIdAttributes(attributes, TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG);
    	} else if (qName.equalsIgnoreCase("length")) { 
    		fieldOperator = OperatorMask.Field_None;
    		fieldType = TypeMask.GroupLength;//NOTE: length is not optional
    		
    		commonIdAttributes(attributes, TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    		
    	} else if (qName.equalsIgnoreCase("string")) {
    		fieldOperator = OperatorMask.Field_None;
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
    		commonIdAttributes(attributes, TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		fieldOperator = OperatorMask.Field_None; //none is zero and the same for twin and single types
    		fieldPMapInc=2; //any operators must count as two PMap fields.
    		fieldType = "optional".equals(attributes.getValue("presence")) ?
    				TypeMask.DecimalOptional:
    				TypeMask.Decimal;
    		
    		commonIdAttributes(attributes, TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT);
    		
    		fieldExponentOptional = false;
    		fieldMantissaOptional = false;
    		fieldExponentOperator = OperatorMask.Field_None;
    		fieldMantissaOperator = OperatorMask.Field_None;
    		
    		
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		fieldPMapInc=1;
    		fieldExponentOptional = "optional".equals(attributes.getValue("presence"));
    		fieldOperator = OperatorMask.Field_None;
    		
			String absentString = attributes.getValue("nt_absent_const");
			if (null!=absentString && absentString.trim().length()>0) {
				fieldExponentAbsent = Integer.parseInt(absentString.trim());
			} else {
				//default value for absent of this type
				fieldExponentAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
			}
    		
    	} else if (qName.equalsIgnoreCase("mantissa")) {
    		fieldPMapInc=1;
    		fieldMantissaOptional = "optional".equals(attributes.getValue("presence"));
    		fieldOperator = OperatorMask.Field_None;
    		
			String absentString = attributes.getValue("nt_absent_const");
			if (null!=absentString && absentString.trim().length()>0) {
				fieldMantissaAbsent = Long.parseLong(absentString.trim());
			} else {
				//default value for absent of this type
				fieldMantissaAbsent = TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
			}
    		
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		fieldOperator = OperatorMask.Field_None;
    		fieldType = TypeMask.ByteArray;
    		fieldId = Integer.valueOf(attributes.getValue("id"));
    		fieldName = attributes.getValue("name");
    		
    	} else if (qName.equalsIgnoreCase("copy")) { 
    		fieldOperator = OperatorMask.Field_Copy;
    		groupOpenTokenPMapStack[groupTokenStackHead]+=fieldPMapInc;
    		
    	} else if (qName.equalsIgnoreCase("constant")) {
    		fieldOperator = OperatorMask.Field_Constant;
    		fieldOperatorValue = attributes.getValue("value");
    		if ((fieldType&1)!=0) {
    			groupOpenTokenPMapStack[groupTokenStackHead]+=fieldPMapInc;//optional constant does but required does not.
    		}
    		
    	} else if (qName.equalsIgnoreCase("default")) {
    	    fieldOperator = OperatorMask.Field_Default;
    	    fieldOperatorValue = attributes.getValue("value");
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
							    				catalogTemplateScriptIdx+1);//we jump over the length field
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		
    		//sequence token is not added to the script until the Length field is seen
    		//catalogTemplateScript[catalogTemplateScriptIdx++] = groupOpenTokenStack[groupTokenStackHead]; 
    		

    	} else if (qName.equalsIgnoreCase("template")) {
    		catalogTemplateScriptIdx = 0; //for the children but not used by this "group"
    		
    		
    		//Token must hold the max bytes needed for the PMap but this is the start element
    		//and that data is not ready yet. So in the Count field we will put the templateScriptIdx.
    		//upon close of this element the token at that location in the templateScript must have
    		//the Count updated to the right value.
    		int token = TokenBuilder.buildToken(TypeMask.Group,
							    				0, 
							    				0);
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		
    		//messages do not need to be listed because they are the top level group.
    //		catalogTemplateScript[catalogTemplateScriptIdx++] = token; 
    		
    		//template specific values after this point
    		
    		templateId = Integer.valueOf(attributes.getValue("id"));
    		if (fieldId<0) {
    			throw new SAXException("Template Id must be positive: "+fieldId);
    		} else {
    			templateIdBiggest = Math.max(templateIdBiggest,templateId);
    		}
    		validateUniqueTemplateId();
    		
    		templateXMLns = attributes.getValue("xmlns");
    		templateName = attributes.getValue("name");
    	    templateDictionary = attributes.getValue("dictionary");
    	    
    	    //if number is in same dictionary as before it is the same field
    	    //if not it is different and needs a new token.
    	    //dictionary 2 bits are no longer needed?
    	    //function to convert dictionary names to sequential integers, change int in each scope.
    	    //
    	    
    	    

    	    //TODO: full dictionary redesign:
    	    //use different index for each dictionary so no need to swap array.
    	    //copy will work the same, All dictionary work is done here in XML parse.
    	    
    	    
    	    
    	    //Dictionary needs to restrict the fieldIds to this scope.
    	    //TODO: must add dictionary support for this complex example.
    	    //must generate right 2 dight dictionary here for all fieldId usage.
//    		 *    2 bits
//    		 *       00 none/global
//    		 *       01 app type (TODO: int assigned for app types)
//    		 *       10 template (use template id as int?)
//    		 *       11 use internal lookup of custom by field id. 
    	    
    	    if ("Y".equalsIgnoreCase(attributes.getValue("reset"))) {
    	    	//add Dictionary command to reset in the script
    	    	int resetToken = TokenBuilder.buildToken(TypeMask.Dictionary,
	    										OperatorMask.Dictionary_Reset, 
	    										0);
    	    	catalogTemplateScript[catalogTemplateScriptIdx++] = resetToken; 
    	    }
    	    
    	    
    	    
    	} else if (qName.equalsIgnoreCase("templates")) {
    		
    		templatesXMLns = attributes.getValue("xmlns");
    		    		
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
    	/*
    	 * The script will require tokens to be saved in catalog because when dictionaries are used
    	 * we will have multiple tokens per single Id and will not be able to rebuild the token list
    	 * just from id.  But the id is where the value is placed in the output buffers.  So with each
    	 * pass of has next the id may end up with another field.  Save script as sequence of LONGS.
    	 * 
    	 */
    	
    	
    	if (qName.equalsIgnoreCase("uint32") ||
    	    qName.equalsIgnoreCase("int32")) {
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			assert (0==tokenLookupFromFieldId[fieldId]);
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderIntCount++);
       			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		//TODO: duplicate this for the others as a method call once complete.
    		if (fieldOperator==OperatorMask.Field_Constant ||fieldOperator==OperatorMask.Field_Default) {
    			if (null!=fieldOperatorValue && !fieldOperatorValue.isEmpty()) {
    				//TODO: must convert to int and save in the catalog as the default value for this field.
    				
    			}
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    	} else if (qName.equalsIgnoreCase("uint64") ||
    			    qName.equalsIgnoreCase("int64")) {
       		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			assert (0==tokenLookupFromFieldId[fieldId]);
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderLongCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    	
    	} else if (qName.equalsIgnoreCase("string")) {
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			assert (0==tokenLookupFromFieldId[fieldId]);
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderTextCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    		
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			
    			//TODO: decimal parts are not implemented to each have their own optional.
    			
    			if (0!=fieldExponentOperator || 0!= fieldMantissaOperator) {
    				fieldOperator = (fieldExponentOperator<<TokenBuilder.SHIFT_OPER_DECIMAL_EX)|fieldMantissaOperator;
    			} else {
    				fieldOperator |= (fieldOperator<<TokenBuilder.SHIFT_OPER_DECIMAL_EX);
    			}
    			    			
    			//if undefined create new token
    			assert (0==tokenLookupFromFieldId[fieldId]);
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderDecimalCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;

    		fieldPMapInc=1;//set back to 1 we are leaving decimal processing
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		fieldExponentOperator = fieldOperator;
    		
    	} else if (qName.equalsIgnoreCase("mantissa")) {
    		fieldMantissaOperator = fieldOperator;
    		
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			assert (0==tokenLookupFromFieldId[fieldId]);
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderByteCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;
    	} else if (qName.equalsIgnoreCase("template")) {
    		//templates always add 1 more for the templateId in the pmap
    		int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead]+1;
    		
    		//convert pmap bits to FAST 7bit bytes
    		int pmapMaxBytes = (pmapMaxBits+6)/7; 
    		assert (pmapMaxBytes>0) : "Dynamic templates always have a pmap of at least 1 byte";

    		//save biggest found template pmap for use by the catalog
    		catalogLargestTemplatePMap = Math.max(catalogLargestTemplatePMap,pmapMaxBytes);   		
    		
    		//No need to adjust the open token because as a (template/message) it is not in the script.
    		//we do need to decrement the stack counter because it was used for capture of the pmap size
    		groupTokenStackHead--;
    		assert(-1==groupTokenStackHead) : "poped off template so the stack should be empty again.";
    		
    		addCompleteScriptToCatalog();     		
   		
    		
    	} else if (qName.equalsIgnoreCase("length")) {
    		//Length must be the first field inside of the sequence.
    		
    		//TODO: sequence length up front
    		
    		if (0==tokenLookupFromFieldId[fieldId]) {
    			//if undefined create new token
    			tokenLookupFromFieldId[fieldId] = TokenBuilder.buildToken(fieldType, fieldOperator, tokenBuilderIntCount++);
    			fieldIdUnique++;
    		} else {
    			validate();   			
    		}
    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = fieldId;

    		
    		catalogTemplateScript[catalogTemplateScriptIdx++] = groupOpenTokenStack[groupTokenStackHead]; 
    		
    		
    		//NOTE: we want the sequence length to come first then the repeating group pmap therefore
    	    //we are waiting until now to add the open group token.
    		
    		//TODO: urgen finish xx
    		
    	} else if (qName.equalsIgnoreCase("sequence")  ) {
    		
    		int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead];
    		int pmapMaxBytes = (pmapMaxBits+6)/7; //if bits is zero this will be zero.
    		catalogLargestNonTemplatePMap = Math.max(catalogLargestNonTemplatePMap,pmapMaxBytes);
    		
    		
    		int opMask = OperatorMask.Group_Bit_Close;
    		int openToken = groupOpenTokenStack[groupTokenStackHead];
    		if (pmapMaxBytes>0) {
    			opMask |= OperatorMask.Group_Bit_PMap;
    			openToken |= (OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER);
    		}
    		
    		//
    		int openGroupIdx = TokenBuilder.extractCount(openToken);
			int groupSize = catalogTemplateScriptIdx - openGroupIdx;
    		    		    			
			//change open token so it has the total number of script steps inside the group.
			catalogTemplateScript[openGroupIdx] =
			groupOpenTokenStack[groupTokenStackHead] = (TokenBuilder.MAX_FIELD_MASK&openToken) |
														(TokenBuilder.MAX_FIELD_ID_VALUE&groupSize);
			

			//closing token has positive jump back to head, in case it is needed 
			catalogTemplateScript[catalogTemplateScriptIdx++] = 
					            TokenBuilder.buildToken(TypeMask.Group, opMask, groupSize);

    		
    		groupTokenStackHead--;//pop this group off the stack to work on the previous.

    	} else if (qName.equalsIgnoreCase("group") ) {
    		
    		int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead];
    		int pmapMaxBytes = (pmapMaxBits+6)/7; //if bits is zero this will be zero.
    		catalogLargestNonTemplatePMap = Math.max(catalogLargestNonTemplatePMap,pmapMaxBytes);
    		
    		
    		int opMask = OperatorMask.Group_Bit_Close;
    		int openToken = groupOpenTokenStack[groupTokenStackHead];
    		if (pmapMaxBytes>0) {
    			opMask |= OperatorMask.Group_Bit_PMap;
    			openToken |= (OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER);
    		}
    		
    		//
    		int openGroupIdx = TokenBuilder.extractCount(openToken);
			int groupSize = catalogTemplateScriptIdx - openGroupIdx;
    		    		    			
			//change open token so it has the total number of script steps inside the group.
			catalogTemplateScript[openGroupIdx] =
			groupOpenTokenStack[groupTokenStackHead] = (TokenBuilder.MAX_FIELD_MASK&openToken) |
														(TokenBuilder.MAX_FIELD_ID_VALUE&groupSize);
			

			//closing token has positive jump back to head, in case it is needed 
			catalogTemplateScript[catalogTemplateScriptIdx++] = 
					            TokenBuilder.buildToken(TypeMask.Group, opMask, groupSize);

    		
    		groupTokenStackHead--;//pop this group off the stack to work on the previous.

    		
    	} else if (qName.equalsIgnoreCase("templates")) {
    		templatesXMLns = null;
    	}

    }

	private void validateUniqueTemplateId() throws SAXException {
		if (0!=templateLookup[templateId]) {
			throw new SAXException("Duplicate template id: "+templateId);
		}
		templateLookup[templateId] = 1;
	}

	private void addCompleteScriptToCatalog() {
		//give this script to the catalog
		int[] script = new int[catalogTemplateScriptIdx];
		//TODO: delete System.err.println("parsed "+templateId+" with script length "+catalogTemplateScriptIdx);
		System.arraycopy(catalogTemplateScript, 0, script, 0, catalogTemplateScriptIdx);
		catalogScripts[templateId] = script;
		templateIdUnique++;
	}


	private void validate() throws SAXException {
		//if defined use old token but validate it
		int expectedType = TokenBuilder.extractType(tokenLookupFromFieldId[fieldId]);
		if (expectedType != fieldType) {
			throw new SAXException("id: "+fieldId+" can not be defined for both types "+expectedType+" and "+fieldType);
		}
	}
	
	public void postProcessing() {
		int singleTextLength = 128; //TODO: must set somewhere. but not here its not part of template.
				
		DictionaryFactory df = new DictionaryFactory(tokenBuilderIntCount,
				                                     tokenBuilderLongCount, 
				                                     tokenBuilderTextCount, singleTextLength, 
													 tokenBuilderDecimalCount, 
													 tokenBuilderByteCount);
		
		
		//write catalog data.
		TemplateCatalog.save(writer, fieldIdUnique, fieldIdBiggest, 
				     tokenLookupFromFieldId, absentLookupFromFieldId,
				     templateIdUnique, templateIdBiggest, catalogScripts, df, 
				     catalogLargestTemplatePMap, 
				     catalogLargestNonTemplatePMap);
				
		//close stream.
		writer.flush();
				
	}


}
