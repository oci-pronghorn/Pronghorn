//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
import com.ociweb.jfast.primitive.FASTOutput;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class TemplateHandler extends DefaultHandler {

	private static final String SPECIAL_PREFIX = "'>";


	private final PrimitiveWriter writer;


    //Catalog represents all the templates supported 
    int[] catalogScriptTokens = new int[TokenBuilder.MAX_FIELD_ID_VALUE];
    int[] catalogScriptFieldIds = new int[TokenBuilder.MAX_FIELD_ID_VALUE];
           
    int catalogTemplateScriptIdx = 0;
 
    int catalogLargestTemplatePMap = 0;
    int catalogLargestNonTemplatePMap = 0;
	
	DictionaryFactory defaultConstValues = new DictionaryFactory();
    	
	List<List<Integer>> members = new ArrayList<List<Integer>>();
	
	//post processing for catalog
	int[][] tokenIdxMembers;
	int[] tokenIdxMemberHeads;
    
    //compact slower structure to determine dictionaries because data is very sparse and not large
    //FieldId, Dictionary, Token
    //must provide lookup and insert.
    //FieldId  -> *Dictionary
    //FieldId/Dictionary -> Token
    // [fieldId][Dictionary] -> token, as each is used it may grow to fit the count of dictionaries
    /// the second array will contain zeros to allow direct offset to the dictionary.
    
    int[][] dictionaryMap = new int[TokenBuilder.MAX_FIELD_ID_VALUE][];
    
    //TODO: must detect two fieldId defined in different dictionaries when they appear in the same stop node block.
    
    //every dictionary must be converted into an integer so we will use the index in a simple list.
    final List<String> dictionaryNames = new ArrayList<String>(128);
    int activeDictionary = -1;
    final String globalDictionaryName = "global"; 
    
    
    //Name space for all the active templates if they do not define their own.
    String templatesXMLns; //TODO: name space processing is not implemented yet.
    
    //Templates never nest and only appear one after the other. Therefore 
    //these fields never need to be in a stack and the values put here by the
    //start will still be there for end.
    int    templateId;
    int 	templateIdBiggest = 0;
    int 	templateIdUnique = 0;
    //holds offset to template in script 
    int[] templateIdx = new int[TokenBuilder.MAX_FIELD_ID_VALUE]; //checking for unique templateId 
    int[] templateLimit = new int[TokenBuilder.MAX_FIELD_ID_VALUE]; //checking for unique templateId 
    
    String templateName;
    String templateXMLns;
    
    
   // Fields never nest and only appear one after the other.
    int     fieldId;
    int 	fieldIdBiggest = 0;
    int 	fieldTokensUnique = 0;
    
    int     fieldType;
    int 	fieldOperator;
    String  fieldOperatorValue;
    String  fieldName;
    String  fieldDictionary;
    String  fieldDictionaryKey;
    
    boolean fieldExponentOptional = false;
    int      fieldExponentAbsent;
    int      fieldExponentOperator;
    String   fieldExponentOperatorValue;
    
    boolean fieldMantissaOptional = false;
    long     fieldMantissaAbsent;
    int      fieldMantissaOperator;
    String   fieldMantissaOperatorValue;
    
    int      fieldPMapInc = 1;//changes to 2 only when inside twin decimal
    
    //Counters for TokenBuilder so each field is given a unique spot in the dictionary.
    AtomicInteger tokenBuilderIntCount = new AtomicInteger(0);
    AtomicInteger tokenBuilderLongCount = new AtomicInteger(0);
    AtomicInteger tokenBuilderTextCount = new AtomicInteger(0);
    AtomicInteger tokenBuilderByteCount = new AtomicInteger(0);
    AtomicInteger tokenBuilderDecimalCount = new AtomicInteger(0); 
   
    
    //groups can be nested and need a stack, this includes sequence and template.   

    int[] groupOpenTokenPMapStack = new int[TokenBuilder.MAX_FIELD_ID_VALUE];
    int[] groupOpenTokenStack = new int[TokenBuilder.MAX_FIELD_ID_VALUE];//Need not be this big.
    int   groupTokenStackHead = -1;
        
	
    public TemplateHandler(FASTOutput output) {
    	writer = new PrimitiveWriter(output);
    	
    	dictionaryNames.add(globalDictionaryName);
    	activeDictionary = dictionaryNames.indexOf(globalDictionaryName);
    	
	}
    
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
    		fieldOperatorValue = attributes.getValue("value");
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
    		fieldOperatorValue = attributes.getValue("value");
    		//Never uses pmap
    		
    	} else if (qName.equalsIgnoreCase("increment")) {
    	    fieldOperator = OperatorMask.Field_Increment;
    	    groupOpenTokenPMapStack[groupTokenStackHead]+=fieldPMapInc;
    	    fieldOperatorValue = attributes.getValue("value");
    	    
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
							    				catalogTemplateScriptIdx, TokenBuilder.MASK_ABSENT_DEFAULT);
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		
    		catalogScriptTokens[catalogTemplateScriptIdx] = token;
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = 0; //Zero id for group
    		
    	} else if (qName.equalsIgnoreCase("sequence")) {   		
    		
    		fieldName = attributes.getValue("name");
    		
    		//Token must hold the max bytes needed for the PMap but this is the start element
    		//and that data is not ready yet. So in the Count field we will put the templateScriptIdx.
    		//upon close of this element the token at that location in the templateScript must have
    		//the Count updated to the right value.
    		int token = TokenBuilder.buildToken(TypeMask.Group,
							    				OperatorMask.Group_Bit_Seq, 
							    				catalogTemplateScriptIdx+1, TokenBuilder.MASK_ABSENT_DEFAULT);//we jump over the length field
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		
    		//sequence token is not added to the script until the Length field is seen

    	} else if (qName.equalsIgnoreCase("template")) {
    		//must support zero so we add 1 to the index.
    		int templateOffset = catalogTemplateScriptIdx+1; 
    		
    		templateId = Integer.valueOf(attributes.getValue("id"));
    		if (0!=templateIdx[templateId]) {
				throw new SAXException("Duplicate template id: "+templateId);
			}
			templateIdx[templateId] = templateOffset;
    		if (templateId<0) {
    			throw new SAXException("Template Id must be positive: "+templateId);
    		} else {
    			templateIdBiggest = Math.max(templateIdBiggest,templateId);
    		}
    		
    		//Token must hold the max bytes needed for the PMap but this is the start element
    		//and that data is not ready yet. So in the Count field we will put the templateScriptIdx.
    		//upon close of this element the token at that location in the templateScript must have
    		//the Count updated to the right value.
    		int token = TokenBuilder.buildToken(TypeMask.Group,
							    				0, 
							    				0, TokenBuilder.MASK_ABSENT_DEFAULT);
    		
    		//this token will tell how to get back to the index in the script to fix it.
    		//this value will also be needed for the back jump value in the closing task.
    		groupOpenTokenStack[++groupTokenStackHead] = token;
    		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
    		
    		//messages do not need to be listed in catalogTemplateScript because they are the top level group.
    		
    		templateXMLns = attributes.getValue("xmlns");
    		templateName = attributes.getValue("name");
    	    
    		//TODO: must also add dictionary logic to group etc. not just template
    		setActiveDictionary(attributes);
    		    	    
    	    if ("Y".equalsIgnoreCase(attributes.getValue("reset"))) {
    	    	//add Dictionary command to reset in the script
    	    	int resetToken = TokenBuilder.buildToken(TypeMask.Dictionary,
			    									  	 OperatorMask.Dictionary_Reset, 
			    										 activeDictionary, TokenBuilder.MASK_ABSENT_DEFAULT);
    	    	
    	    	catalogScriptTokens[catalogTemplateScriptIdx] = resetToken;
        		catalogScriptFieldIds[catalogTemplateScriptIdx++] = 0;
       	    }
    	    
    	    
    	    
    	} else if (qName.equalsIgnoreCase("templates")) {
    		
    		templatesXMLns = attributes.getValue("xmlns");
    		    		
    	}
    }

	private void setActiveDictionary(Attributes attributes) {
		String dictionaryName = attributes.getValue("dictionary");
		if ("template".equalsIgnoreCase(dictionaryName)) {
			dictionaryName = SPECIAL_PREFIX+templateId;
		} else if ("apptype".equalsIgnoreCase(dictionaryName)) {
			int appType = -1;//TODO: implement application type in XML parse    			
			dictionaryName = SPECIAL_PREFIX+appType;
		}
		int idx = dictionaryNames.indexOf(dictionaryName);
		if (idx<0) {
			dictionaryNames.add(dictionaryName);
			activeDictionary = dictionaryNames.indexOf(dictionaryName);
		} else {
			activeDictionary = idx;
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
    		    		
    		int token = buildToken(tokenBuilderIntCount);
    		    		
    			
    			int optionalOffset = 0;
    			if (fieldOperator==OperatorMask.Field_Default) {
    				if ((fieldType&1)!=0) {
    					//optional default
    					optionalOffset=1;
    				}
    			}
    			
    			//only set if the value was given
    			if (null!=fieldOperatorValue && !fieldOperatorValue.isEmpty()) {
    				int tmp = Integer.parseInt(fieldOperatorValue);
    				defaultConstValues.addInitInteger(token&TokenBuilder.MAX_INSTANCE,
                        						tmp<0?tmp:optionalOffset+tmp);//+1 for optional not applied to negative values.
    			} 
    			//if no default is set the field must be undefined and therefore remains zero
    			
    			fieldOperatorValue=null;

	    	catalogScriptTokens[catalogTemplateScriptIdx] = token;
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;
    		
    	} else if (qName.equalsIgnoreCase("uint64") ||
    			    qName.equalsIgnoreCase("int64")) {
       		
    		int token = buildToken(tokenBuilderLongCount);
    		
	
    			int optionalOffset = 0;
    			if (fieldOperator==OperatorMask.Field_Default) {
    				if ((fieldType&1)!=0) {
    					//optional default
    					optionalOffset=1;
    				}
    			}
    			
    			//only set if the value was given
    			if (null!=fieldOperatorValue && !fieldOperatorValue.isEmpty()) {
    				long tmp = Long.parseLong(fieldOperatorValue);
    				defaultConstValues.addInitLong(token&TokenBuilder.MAX_INSTANCE,
    						tmp<0?tmp:optionalOffset+tmp);//+1 for optional not applied to negative values.
    			} 
    			//if no default is set the field must be undefined and therefore remains zero
    			
    			fieldOperatorValue=null;
    		
	    	catalogScriptTokens[catalogTemplateScriptIdx] = token;
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;
    	
    	} else if (qName.equalsIgnoreCase("string")) {
    		
    		int token = buildToken(tokenBuilderTextCount);

			//only set if the value was given
			if (null!=fieldOperatorValue && !fieldOperatorValue.isEmpty()) {
				defaultConstValues.addInit(token&TokenBuilder.MAX_INSTANCE,fieldOperatorValue.toCharArray());
			} 
			fieldOperatorValue=null;

    		
	    	catalogScriptTokens[catalogTemplateScriptIdx] = token;
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;
    		
    	} else if (qName.equalsIgnoreCase("decimal")) {
    		
    		//decimal specific logic to combine the operators
    		if (0!=fieldExponentOperator || 0!= fieldMantissaOperator) {
    			fieldOperator = (fieldExponentOperator<<TokenBuilder.SHIFT_OPER_DECIMAL_EX)|fieldMantissaOperator;
    		} else {
    			fieldOperator |= (fieldOperator<<TokenBuilder.SHIFT_OPER_DECIMAL_EX);
    		}
    		
    		int token = buildToken(tokenBuilderDecimalCount);
    		

    			int optionalExponentOffset = 0;
    			if (fieldExponentOperator==OperatorMask.Field_Default) {
    				if ((fieldType&1)!=0) {
    					//optional default
    					optionalExponentOffset=1; 
    				}
    			}
    			
    			//only set if the value was given
    			if (null!=fieldExponentOperatorValue && !fieldExponentOperatorValue.isEmpty()) {
    				int tmp = Integer.parseInt(fieldExponentOperatorValue);
    				defaultConstValues.addInitDecimalExponent(token&TokenBuilder.MAX_INSTANCE,
    								tmp<0?tmp:optionalExponentOffset+tmp);//+1 for optional not applied to negative values.
    			}
    			//if no default is set the field must be undefined and therefore remains zero

    			fieldExponentOperatorValue=null;

    			
    			int optionalMantissaOffset = 0;
    			if (fieldMantissaOperator==OperatorMask.Field_Default) {
    				if ((fieldType&1)!=0) {
    					//optional default
    					optionalMantissaOffset=1;
    				}
    			}
    			
    			//only set if the value was given
    			if (null!=fieldMantissaOperatorValue && !fieldMantissaOperatorValue.isEmpty()) {
    				long tmp = Long.parseLong(fieldMantissaOperatorValue);
    				defaultConstValues.addInitDecimalMantissa(token&TokenBuilder.MAX_INSTANCE,
    						tmp<0?tmp:optionalMantissaOffset+tmp);//+1 for optional not applied to negative values.
    			} 
    			//if no default is set the field must be undefined and therefore remains zero
    			
    			fieldMantissaOperatorValue=null;

    		
	    	catalogScriptTokens[catalogTemplateScriptIdx] = token;
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;

    		fieldPMapInc=1;//set back to 1 we are leaving decimal processing
    	} else if (qName.equalsIgnoreCase("exponent")) {
    		fieldExponentOperator = fieldOperator;
    		fieldExponentOperatorValue = fieldOperatorValue;
    		fieldOperatorValue =  null;
    	} else if (qName.equalsIgnoreCase("mantissa")) {
    		fieldMantissaOperator = fieldOperator;
    		fieldMantissaOperatorValue = fieldOperatorValue;
    		fieldOperatorValue =  null;
    	} else if (qName.equalsIgnoreCase("bytevector")) {
    		
    		int token = buildToken(tokenBuilderByteCount);

	    	catalogScriptTokens[catalogTemplateScriptIdx] = token;
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;
    		
    	} else if (qName.equalsIgnoreCase("template")) {
    		
    		templateLimit[templateId] = catalogTemplateScriptIdx;
    		
    		//templates always add 1 more for the templateId in the pmap
    		int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead]+1;
    		//convert pmap bits to FAST 7bit bytes
    		int pmapMaxBytes = (pmapMaxBits+6)/7; 
    		//System.err.println("pmap bits "+pmapMaxBits+" "+pmapMaxBytes);
    		assert (pmapMaxBytes>0) : "Dynamic templates always have a pmap of at least 1 byte";

    		//save biggest found template pmap for use by the catalog
    		catalogLargestTemplatePMap = Math.max(catalogLargestTemplatePMap,pmapMaxBytes);   		
    		
    		//No need to adjust the open token because as a (template/message) it is not in the script.
    		//we do need to decrement the stack counter because it was used for capture of the pmap size
    		groupTokenStackHead--;
    		assert(-1==groupTokenStackHead) : "poped off template so the stack should be empty again.";
    		
    		templateIdUnique++;   		
   		
    		
    	} else if (qName.equalsIgnoreCase("length")) {
    		//Length must be the first field inside of the sequence.
    		
    		int token = buildToken(tokenBuilderIntCount);
    		
    		
    		//NOTE: we want the sequence length to come first then the repeating group pmap therefore
    		//we are waiting until now to add the open group token.
	    	catalogScriptTokens[catalogTemplateScriptIdx] = token;
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;
    		
	    	catalogScriptTokens[catalogTemplateScriptIdx] = groupOpenTokenStack[groupTokenStackHead];
    		catalogScriptFieldIds[catalogTemplateScriptIdx++] = 0;
    		
    		    		
    	} else if (qName.equalsIgnoreCase("sequence")  ) {
    		
    		int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead];
    		int pmapMaxBytes = (pmapMaxBits+6)/7; //if bits is zero this will be zero.
    		//System.err.println("x pmap bits "+pmapMaxBits+" "+pmapMaxBytes);
    		catalogLargestNonTemplatePMap = Math.max(catalogLargestNonTemplatePMap,pmapMaxBytes);
    		
    		
    		int opMask = OperatorMask.Group_Bit_Close|OperatorMask.Group_Bit_Seq;
    		int openToken = groupOpenTokenStack[groupTokenStackHead];
    		if (pmapMaxBytes>0) {
    			opMask |= OperatorMask.Group_Bit_PMap;
    			openToken |= (OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER);
    		}
    		
    		//
    		int openGroupIdx = TokenBuilder.MAX_INSTANCE&openToken;
			int groupSize = catalogTemplateScriptIdx - openGroupIdx;
    		    		    			
			//change open token so it has the total number of script steps inside the group.
	    	catalogScriptTokens[openGroupIdx] = (groupOpenTokenStack[groupTokenStackHead] = (TokenBuilder.MAX_FIELD_MASK&openToken) |
					(TokenBuilder.MAX_FIELD_ID_VALUE&groupSize));
    		catalogScriptFieldIds[openGroupIdx++] = 0;
						
	    	catalogScriptTokens[catalogTemplateScriptIdx] = TokenBuilder.buildToken(TypeMask.Group, opMask, groupSize, TokenBuilder.MASK_ABSENT_DEFAULT);
	    	catalogScriptFieldIds[catalogTemplateScriptIdx++] = 0;
    		
    		groupTokenStackHead--;//pop this group off the stack to work on the previous.

    	} else if (qName.equalsIgnoreCase("group") ) {
    		
    		int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead];
    		int pmapMaxBytes = (pmapMaxBits+6)/7; //if bits is zero this will be zero.
    		//System.err.println("y pmap bits "+pmapMaxBits+" "+pmapMaxBytes);
    		catalogLargestNonTemplatePMap = Math.max(catalogLargestNonTemplatePMap,pmapMaxBytes);
    		
    		
    		int opMask = OperatorMask.Group_Bit_Close;
    		int openToken = groupOpenTokenStack[groupTokenStackHead];
    		if (pmapMaxBytes>0) {
    			opMask |= OperatorMask.Group_Bit_PMap;
    			openToken |= (OperatorMask.Group_Bit_PMap<<TokenBuilder.SHIFT_OPER);
    		}
    		
    		//
    		int openGroupIdx = TokenBuilder.MAX_INSTANCE&openToken;
			int groupSize = catalogTemplateScriptIdx - openGroupIdx;
    		    		    			
			//change open token so it has the total number of script steps inside the group.
	    	catalogScriptTokens[openGroupIdx] = (groupOpenTokenStack[groupTokenStackHead] = (TokenBuilder.MAX_FIELD_MASK&openToken) |
					(TokenBuilder.MAX_FIELD_ID_VALUE&groupSize));
    		catalogScriptFieldIds[openGroupIdx++] = 0;
    		
	    	catalogScriptTokens[catalogTemplateScriptIdx] = TokenBuilder.buildToken(TypeMask.Group, opMask, groupSize, TokenBuilder.MASK_ABSENT_DEFAULT);
	    	catalogScriptFieldIds[catalogTemplateScriptIdx++] = 0;
    		
    		groupTokenStackHead--;//pop this group off the stack to work on the previous.

    		
    	} else if (qName.equalsIgnoreCase("templates")) {
    		templatesXMLns = null;
    	}

    }

    
	private int buildToken(AtomicInteger count) throws SAXException {
		int token;
		int[] dTokens = dictionaryMap[fieldId];
		if (null==dTokens || dTokens.length<=activeDictionary) {
			int[] newDTokens = new int[activeDictionary+1];
			if (null!=dTokens) {
				System.arraycopy(dTokens, 0, newDTokens, 0, dTokens.length);
			}
			int tokCount = count.getAndIncrement();
			newDTokens[activeDictionary] = token = TokenBuilder.buildToken(fieldType, fieldOperator, tokCount, TokenBuilder.MASK_ABSENT_DEFAULT);
			saveMember(activeDictionary,fieldType,tokCount, fieldOperator);
			fieldTokensUnique++;
			dictionaryMap[fieldId]=dTokens=newDTokens;
			
		} else {
			token = dTokens[activeDictionary];
			if (0!=token) {
				if (fieldType!=TokenBuilder.extractType(token) || fieldOperator!=TokenBuilder.extractType(token)) {
					throw new SAXException("Field id can not be redefined within the same dictionary.");
				}
			} else {
				int tokCount = count.getAndIncrement();
				dTokens[activeDictionary] = token = TokenBuilder.buildToken(fieldType, fieldOperator, tokCount, TokenBuilder.MASK_ABSENT_DEFAULT);
				saveMember(activeDictionary,fieldType,tokCount, fieldOperator);
				fieldTokensUnique++;
			}
		}
		
		return token;
	}

	
	private void saveMember(int activeDictionary, int fieldType, int tokCount, int fieldOperator) {
		
		if (TypeMask.GroupLength==fieldType) {
			return;//these are not needed for reset because it is part of the sequence definition.
		}
		
		//these never update the dictionary so they should never do a reset.
		if (OperatorMask.Field_None==fieldOperator ||
			OperatorMask.Field_Constant==fieldOperator ||
			OperatorMask.Field_Default==fieldOperator) {
			//System.err.println("skipped "+TypeMask.toString(fieldType));
			return;
		}
		
		//only need to group by major type.
		int d = activeDictionary<<TokenBuilder.BITS_TYPE;
		
		if (fieldType<0x0C) {
			fieldType = fieldType&0xFC;
		} else {
			fieldType = fieldType&0xFE;
		}
			
		int listId = d|fieldType;
				
		while (members.size()<=listId) {
			members.add(new ArrayList<Integer>());
		}
		
		//these are ever increasing in value, the order makes a difference in performance at run time.
		assert(members.get(listId).size()==0 || members.get(listId).get(members.get(listId).size()-1).intValue()<tokCount);
		members.get(listId).add(tokCount);
	}

	private void buildDictionaryMemberLists() {
		
		//walk the lists of dictionary members and join them into a master list for each dictionary.
		//each section must start with stop bit and type for the following identifiers. All cache friendly forward motion.

		int dictionaryCount = dictionaryNames.size();
		tokenIdxMembers = new int[dictionaryCount][TokenBuilder.MAX_FIELD_ID_VALUE];
		tokenIdxMemberHeads = new int[dictionaryCount];
		
		int j = members.size();
		while (--j>=0) {
			if (!members.get(j).isEmpty()) {
				int d = j>>>TokenBuilder.BITS_TYPE;
		        int t = j&TokenBuilder.MASK_TYPE;
		        int stopInt = 0xFFFF0000|t;
		        tokenIdxMembers[d][tokenIdxMemberHeads[d]++] = stopInt;
		        //System.err.println("stopInt:"+stopInt+" "+Integer.toBinaryString(stopInt)+" "+TypeMask.toString(t));
		        for(Integer i:members.get(j)) {
		        	tokenIdxMembers[d][tokenIdxMemberHeads[d]++] = i;
		        }
			}
		}
		//tokenIdxMembers are ready to be saved but must be trimmed by heads
		
	}

	public void postProcessing() {
		
		buildDictionaryMemberLists();
		
		//the catalog file need not be "Small" but it probably will be.
		//the catalog file must be "Fast" to load without any "Processing" needed by the consumer.
		//this enables fast startup/recovery times that do not produce garbage.

		defaultConstValues.setTypeCounts(tokenBuilderIntCount.intValue(),
                         tokenBuilderLongCount.intValue(), 
                         tokenBuilderTextCount.intValue(), 
						 tokenBuilderDecimalCount.intValue(), 
						 tokenBuilderByteCount.intValue());
				
		//write catalog data.
		TemplateCatalog.save(writer, fieldTokensUnique, fieldIdBiggest, 
						     templateIdUnique, templateIdBiggest,
						     defaultConstValues, catalogLargestTemplatePMap, catalogLargestNonTemplatePMap, 
						     tokenIdxMembers, 
						     tokenIdxMemberHeads,
						     catalogScriptTokens,
						     catalogScriptFieldIds, catalogTemplateScriptIdx,
						     templateIdx, templateLimit);
				
		//close stream.
		writer.flush();
		//System.err.println("wrote:"+writer.totalWritten());
				
	}


}
