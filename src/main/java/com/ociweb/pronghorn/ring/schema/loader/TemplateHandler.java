//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.pronghorn.ring.schema.loader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.token.OperatorMask;
import com.ociweb.pronghorn.ring.token.TokenBuilder;
import com.ociweb.pronghorn.ring.token.TypeMask;
import com.ociweb.pronghorn.ring.util.hash.LongHashTable;

interface SAXEvent
{
    void play(TemplateHandler handler) throws SAXException;
}

class StartElementEvent implements SAXEvent {
    String uri;
    String localName;
    String qName;
    Attributes attributes;

    StartElementEvent(String uri, String localName, String qName, Attributes attributes)
    {
        this.uri = uri;
        this.localName = localName;
        this.qName = qName;
        this.attributes = new AttributesImpl(attributes);
    }

    public void play(TemplateHandler handler) throws SAXException
    {
       handler.startElement(uri,localName,qName,attributes);
    }
}

class EndElementEvent implements SAXEvent {
    String uri;
    String localName;
    String qName;

    EndElementEvent(String uri, String localName, String qName)
    {
        this.uri = uri;
        this.localName = localName;
        this.qName = qName;
    }

    public void play(TemplateHandler handler) throws SAXException
    {
        handler.endElement(uri,localName, qName);
    }
}


public class TemplateHandler extends DefaultHandler {

	//TODO: B, needs FAST knowledge to add this,  add support for not repeating the template id if message of same time are in sequence.

    private static final String SPECIAL_PREFIX = "'>";

    //it could be made bigger but this also consumes more memory so we need a good reason
    //TODO: B, instead of making this larger find a way to make scripts re-use the fragments of other scripts
    private final static int MAX_SCRIPT_LENGTH = 1<<24;//16M
    static {
    	assert(MAX_SCRIPT_LENGTH>TokenBuilder.MAX_FIELD_ID_VALUE) : "script length constant is not large enough";
    }

    // Catalog represents all the templates supported
    public int[] catalogScriptTokens = new int[MAX_SCRIPT_LENGTH];
    public long[] catalogScriptFieldIds = new long[MAX_SCRIPT_LENGTH];
    public String[] catalogScriptFieldNames = new String[MAX_SCRIPT_LENGTH];
    public String[] catalogScriptDictionaryNames = new String[MAX_SCRIPT_LENGTH];

    final List<SAXEvent> templateEvents = new ArrayList<SAXEvent>();
    
    Map<String,List<SAXEvent>> templateMap = new HashMap<String, List<SAXEvent>>();

    public int catalogTemplateScriptIdx = 0;

    public int catalogLargestTemplatePMap = 0;
    public int catalogLargestNonTemplatePMap = 0;

    public DictionaryFactory defaultConstValues = new DictionaryFactory();

    List<List<Integer>> resetList = new ArrayList<List<Integer>>();

    // post processing for catalog
    public int[][] tokenIdxMembers;
    public int[] tokenIdxMemberHeads;

    // compact slower structure to determine dictionaries because data is very
    // sparse and not large
    // FieldId, Dictionary, Token
    // must provide lookup and insert.
    // FieldId -> *Dictionary
    // FieldId/Dictionary -> Token
    // [fieldId][Dictionary] -> token, as each is used it may grow to fit the
    // count of dictionaries
    // / the second array will contain zeros to allow direct offset to the
    // dictionary.

    int[][] dictionaryMap = new int[TokenBuilder.MAX_FIELD_ID_VALUE+1][];

    // TODO: T, must detect two fieldId defined in different dictionaries when
    // they appear in the same stop node block.

    // every dictionary must be converted into an integer so we will use the
    // index in a simple list.
    final List<String> dictionaryNames = new ArrayList<String>(128);
    int activeDictionary = -1;
    final String globalDictionaryName = "global";

    // Name space for all the active templates if they do not define their own.
    String templatesXMLns; // TODO: B, name space processing is not implemented
                           // yet.

    // Templates never nest and only appear one after the other. Therefore
    // these fields never need to be in a stack and the values put here by the
    // start will still be there for end.
    long templateId;
    public long templateIdBiggest = 0;
    public int templateIdUnique = 0;
    // holds offset to template in script

    //can only support 64K unique keys but the actual values can be much larger 32 bit ints
    public LongHashTable templateToOffset = new LongHashTable(17);
    public LongHashTable templateToLimit = new LongHashTable(17);

    String templateName;
    String templateXMLns;

    // Fields never nest and only appear one after the other.
    long fieldId;
    public long fieldIdBiggest = 0;
    int fieldTokensUnique = 0;

    int fieldType;
    int fieldOperator;
    String fieldOperatorValue;
    String fieldName;
    String fieldDictionary;
    String fieldDictionaryKey;

    int fieldExponentAbsent;
    int fieldExponentOperator;
    String fieldExponentOperatorValue;

    long fieldMantissaAbsent;
    int fieldMantissaOperator;
    String fieldMantissaOperatorValue;

    int fieldPMapInc = 1;// changes to 2 only when inside twin decimal

    // Counters for TokenBuilder so each field is given a unique spot in the
    // dictionary.
    AtomicInteger tokenBuilderIntCount = new AtomicInteger(0);
    AtomicInteger tokenBuilderLongCount = new AtomicInteger(0);
    AtomicInteger tokenBuilderByteCount = new AtomicInteger(0);

    // groups can be nested and need a stack, this includes sequence and
    // template.

    //TODO: B, Make these smaller they do not need to be this big
    int[] groupOpenTokenPMapStack = new int[TokenBuilder.MAX_FIELD_ID_VALUE];
    int[] groupOpenTokenStack = new int[TokenBuilder.MAX_FIELD_ID_VALUE];

    int groupTokenStackHead = -1;
    public int maxGroupTokenStackDepth;

	public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;

	// because optional values are sent as +1 when >= 0 it is not possible to
	// send the
	// largest supported positive value, as a result this is the ideal default
	// because it can not possibly collide with any real values
	public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;


    public TemplateHandler() {
       
        this.dictionaryNames.add(globalDictionaryName);
        this.activeDictionary = dictionaryNames.indexOf(globalDictionaryName);

    }

    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {

        if (qName.equalsIgnoreCase("template")) {
            // must support zero so we add 1 to the index.
            int templateOffset = catalogTemplateScriptIdx + 1;
            fieldName = attributes.getValue("name");
            String templateIdString = attributes.getValue("id");
            if (templateIdString != null ) {
                //numeric value of template id can be in hex and start with 0x or can be decimal.
                templateId = templateIdString.startsWith("0x") ? 
                        Long.parseLong(templateIdString.substring(2), 16) :    
                        Long.parseLong(templateIdString);

                if (!LongHashTable.setItem(templateToOffset, templateId, templateOffset)) {
                	throw new SAXException("Error in XML file, Duplicate template id: " + templateId+"(0x"+Long.toHexString(templateId)+")");
                }

                if (templateId < 0) {
                    throw new SAXException("Template Id must be positive: " + templateId);
                } else {
                    templateIdBiggest = Math.max(templateIdBiggest, templateId);
                }

                // Token must hold the max bytes needed for the PMap but this is the
                // start element
                // and that data is not ready yet. So in the Count field we will put
                // the templateScriptIdx.
                // upon close of this element the token at that location in the
                // templateScript must have
                // the Count updated to the right value.
                boolean hasTemplateId = true;//TODO: B FAST, where do we get this from?  THIS is ONLY set on the group open the close does not need it.
                int token = TokenBuilder.buildToken(TypeMask.Group, hasTemplateId ? OperatorMask.Group_Bit_Templ : 0, catalogTemplateScriptIdx);

                // this token will tell how to get back to the index in the script
                // to fix it.
                // this value will also be needed for the back jump value in the
                // closing task.
                groupOpenTokenStack[++groupTokenStackHead] = token;
                maxGroupTokenStackDepth = Math.max(maxGroupTokenStackDepth, groupTokenStackHead);
                groupOpenTokenPMapStack[groupTokenStackHead] = 0;

                catalogScriptTokens[    catalogTemplateScriptIdx] = token;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;

                String dictionaryName = setActiveDictionary(attributes);
                catalogScriptDictionaryNames[catalogTemplateScriptIdx] = dictionaryName;

                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[  catalogTemplateScriptIdx++] = templateId;



                // messages do not need to be listed in catalogTemplateScript
                // because they are the top level group.

                templateXMLns = attributes.getValue("xmlns");
                templateName = attributes.getValue("name");


                if ("Y".equalsIgnoreCase(attributes.getValue("reset"))) {
                    // add Dictionary command to reset in the script
                    int resetToken = TokenBuilder.buildToken(TypeMask.Dictionary, OperatorMask.Dictionary_Reset,
                            activeDictionary);

                    catalogScriptTokens[catalogTemplateScriptIdx] = resetToken;
                    catalogScriptFieldNames[catalogTemplateScriptIdx] = templateName;
                    catalogScriptFieldIds[catalogTemplateScriptIdx++] = templateId;
                }
            }
            else {
                templateId = -1;
                templateName = attributes.getValue("name");
            }

            templateEvents.clear();

        } else if (qName.equalsIgnoreCase("templates")) {
            setActiveDictionary(attributes);
            templatesXMLns = attributes.getValue("xmlns");

        } else if (qName.equalsIgnoreCase("templateRef")) {
        	//a templateRef is NOT a group but it is LIKE one as far as building its script is concerned

            // int token = TokenBuilder.buildToken(TypeMask.TemplateRef, 0, catalogTemplateScriptIdx);
            //             basicGroupStart(attributes, token);
            //
            //             catalogScriptTokens[    catalogTemplateScriptIdx] = token;
            //             catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
            //             fieldName=null;//ensure it is only used once
            //             catalogScriptFieldIds[  catalogTemplateScriptIdx++] = 0; // Zero id

            String refName = attributes.getValue("name");
            if (refName == null) {
                throw new SAXException("dynamic templateRef is not supported");
            }

            List<SAXEvent> events = templateMap.get(refName);
            if (events == null) {
                throw new SAXException("templateRef to unknown template " + refName);
            }

            for (SAXEvent ev : events) {
                ev.play(this);
            }
        }
        else {

            templateEvents.add(new StartElementEvent(uri,localName,qName,attributes));

            if (templateId < 0)
                return;


            if (qName.equalsIgnoreCase("uint32")) {
                fieldType = "optional".equals(attributes.getValue("presence")) ? TypeMask.IntegerUnsignedOptional : TypeMask.IntegerUnsigned;
                commonIdAttributes(attributes);
            } else if (qName.equalsIgnoreCase("int32")) {
                fieldType = "optional".equals(attributes.getValue("presence")) ? TypeMask.IntegerSignedOptional : TypeMask.IntegerSigned;
                commonIdAttributes(attributes);
            } else if (qName.equalsIgnoreCase("uint64")) {
                fieldType = "optional".equals(attributes.getValue("presence")) ? TypeMask.LongUnsignedOptional : TypeMask.LongUnsigned;
                commonIdAttributes(attributes);
            } else if (qName.equalsIgnoreCase("int64")) {
                fieldType = "optional".equals(attributes.getValue("presence")) ? TypeMask.LongSignedOptional : TypeMask.LongSigned;
                commonIdAttributes(attributes);
            } else if (qName.equalsIgnoreCase("length")) {
                fieldType = TypeMask.GroupLength;// NOTE: length is not optional
                commonIdAttributes(attributes);
            } else if (qName.equalsIgnoreCase("string")) {
                if ("unicode".equals(attributes.getValue("charset"))) {
                    // default is required
                    fieldType = "optional".equals(attributes.getValue("presence")) ? TypeMask.TextUTF8Optional
                            : TypeMask.TextUTF8;
                } else {
                    // default is ascii
                    fieldType = "optional".equals(attributes.getValue("presence")) ? TypeMask.TextASCIIOptional
                            : TypeMask.TextASCII;
                }
                commonIdAttributes(attributes);
            } else if (qName.equalsIgnoreCase("decimal")) {
                fieldPMapInc = 2; // any operators must count as two PMap fields.
                fieldType = "optional".equals(attributes.getValue("presence")) ? TypeMask.DecimalOptional  : TypeMask.Decimal;
                commonIdAttributes(attributes);

                fieldExponentOperator = OperatorMask.Field_None;
                fieldMantissaOperator = OperatorMask.Field_None;

            } else if (qName.equalsIgnoreCase("exponent")) {
                fieldPMapInc = 1;
                if ("optional".equals(attributes.getValue("presence"))) {
                    fieldType = TypeMask.DecimalOptional;
                }
                fieldOperator = OperatorMask.Field_None;

                String absentString = attributes.getValue("nt_absent_const");
                if (null != absentString && absentString.trim().length() > 0) {
                    fieldExponentAbsent = Integer.parseInt(absentString.trim());
                } else {
                    // default value for absent of this type
                    fieldExponentAbsent = DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
                }

            } else if (qName.equalsIgnoreCase("mantissa")) {
                fieldPMapInc = 1;
                fieldOperator = OperatorMask.Field_None;

                String absentString = attributes.getValue("nt_absent_const");
                if (null != absentString && absentString.trim().length() > 0) {
                    fieldMantissaAbsent = Long.parseLong(absentString.trim());
                } else {
                    // default value for absent of this type
                    fieldMantissaAbsent = DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
                }

            } else if (qName.equalsIgnoreCase("bytevector")) {
                fieldOperator = OperatorMask.Field_None;
                fieldType = TypeMask.ByteArray;
                fieldId = Integer.parseInt(attributes.getValue("id"));
                fieldName = attributes.getValue("name");

            } else if (qName.equalsIgnoreCase("copy")) {
                setActiveDictionary(attributes);
                fieldOperator = OperatorMask.Field_Copy;
                fieldOperatorValue = attributes.getValue("value");
                groupOpenTokenPMapStack[groupTokenStackHead] += fieldPMapInc;

            } else if (qName.equalsIgnoreCase("constant")) {
                fieldOperator = OperatorMask.Field_Constant;
                fieldOperatorValue = attributes.getValue("value");
                if ((fieldType & 1) != 0) {
                    groupOpenTokenPMapStack[groupTokenStackHead] += fieldPMapInc;
                }
            } else if (qName.equalsIgnoreCase("default")) {
                fieldOperator = OperatorMask.Field_Default;
                fieldOperatorValue = attributes.getValue("value");
                groupOpenTokenPMapStack[groupTokenStackHead] += fieldPMapInc;

            } else if (qName.equalsIgnoreCase("delta")) {
                setActiveDictionary(attributes);
                fieldOperator = OperatorMask.Field_Delta;
                fieldOperatorValue = attributes.getValue("value");
                // Never uses pmap

            } else if (qName.equalsIgnoreCase("increment")) {
                setActiveDictionary(attributes);
                fieldOperator = OperatorMask.Field_Increment;
                groupOpenTokenPMapStack[groupTokenStackHead] += fieldPMapInc;
                fieldOperatorValue = attributes.getValue("value");

            } else if (qName.equalsIgnoreCase("tail")) {
                setActiveDictionary(attributes);
                fieldOperator = OperatorMask.Field_Tail;
                // Never uses pmap

            } else if (qName.equalsIgnoreCase("group")) {
            	int token = TokenBuilder.buildToken(TypeMask.Group, 0, catalogTemplateScriptIdx);

                basicGroupStart(attributes, token);

                catalogScriptTokens[    catalogTemplateScriptIdx] = token;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[  catalogTemplateScriptIdx++] = 0; // Zero id for group

            } else if (qName.equalsIgnoreCase("sequence")) {
            	int token = TokenBuilder.buildToken(TypeMask.Group, OperatorMask.Group_Bit_Seq, catalogTemplateScriptIdx + 1);

                basicGroupStart(attributes, token);
                //add 1 because we moved the length back up to the previous location
                catalogScriptFieldNames[catalogTemplateScriptIdx+1] = fieldName;
                // sequence token is not added to the script until the Length field
                // is seen

            }
        }
    }

	private void basicGroupStart(Attributes attributes, int token) {
		fieldName = attributes.getValue("name");
		setActiveDictionary(attributes);
		// Token must hold the max bytes needed for the PMap but this is the
		// start element
		// and that data is not ready yet. So in the Count field we will put
		// the templateScriptIdx.
		// upon close of this element the token at that location in the
		// templateScript must have
		// the Count updated to the right value.

		// this token will tell how to get back to the index in the script
		// to fix it.
		// this value will also be needed for the back jump value in the
		// closing task.
		groupOpenTokenStack[++groupTokenStackHead] = token;
		maxGroupTokenStackDepth = Math.max(maxGroupTokenStackDepth, groupTokenStackHead);
		groupOpenTokenPMapStack[groupTokenStackHead] = 0;
	}

    //template, templates, sequence, group, ops - copy,inc,delta,tail all set dictionary.
    //TODO: B, must pop and return the previous dictionary at end of scope.
    private String setActiveDictionary(Attributes attributes) {
        String dictionaryName = attributes.getValue("dictionary");
        if (null==dictionaryName) {
            //Do not change activeDictionary if dictionary attribute does not appear.
            return activeDictionary>=0? dictionaryNames.get(activeDictionary) : globalDictionaryName;
        }
        if ("template".equalsIgnoreCase(dictionaryName)) {
            dictionaryName = SPECIAL_PREFIX + templateId;
        } else if ("apptype".equalsIgnoreCase(dictionaryName)) {
            int appType = -1;// TODO: B, implement application type in XML parse
            dictionaryName = SPECIAL_PREFIX + appType;
        }
        int idx = dictionaryNames.indexOf(dictionaryName);
        if (idx < 0) {
            dictionaryNames.add(dictionaryName);
            activeDictionary = dictionaryNames.indexOf(dictionaryName);
        } else {
            activeDictionary = idx;
        }
        return dictionaryName;
    }

    private void commonIdAttributes(Attributes attributes) throws SAXException {
    	fieldOperator = OperatorMask.Field_None;
    	String strValue = attributes.getValue("id");
    	if (null==strValue) {
    	    throw new SAXException("id is a required attribute on all fields in the template with ID "+templateId+"(0x"+Long.toHexString(templateId)+")");
    	}
        fieldId = strValue.startsWith("0x") ? Long.parseUnsignedLong(strValue.substring(2), 16) : Long.parseLong(strValue);
        if (fieldId < 0) {
            throw new SAXException("Field Id must be positive: " + fieldId);
        } else {
            fieldIdBiggest = Math.max(fieldIdBiggest, fieldId);
        }
        fieldName = attributes.getValue("name");

        // rare: used when we want special dictionary.
        fieldDictionary = attributes.getValue("dictionary");
        // more rare: used when we want to read last value from another field.
        fieldDictionaryKey = attributes.getValue("key");

    }

    public void endElement(String uri, String localName, String qName) throws SAXException {
        /*
         * The script will require tokens to be saved in catalog because when
         * dictionaries are used we will have multiple tokens per single Id and
         * will not be able to rebuild the token list just from id. But the id
         * is where the value is placed in the output buffers. So with each pass
         * of has next the id may end up with another field. Save script as
         * sequence of LONGS.
         */

         if (qName.equalsIgnoreCase("template")) {

             templateMap.put(templateName, templateEvents);

             if (templateId < 0)
                 return;

             // System.err.println("templateId=" + templateId);
             // System.err.println("templateName=" + templateName);

        	if (!LongHashTable.setItem(templateToLimit,templateId, catalogTemplateScriptIdx)) {
        		throw new RuntimeException("internal parse error");
        	}

            // templates always add 1 more for the templateId in the pmap
            int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead] + 1;
            // convert pmap bits to FAST 7bit bytes
            int pmapMaxBytes = (pmapMaxBits + 6) / 7;
            // System.err.println("pmap bits "+pmapMaxBits+" "+pmapMaxBytes);
            assert (pmapMaxBytes > 0) : "Dynamic templates always have a pmap of at least 1 byte";

            // save biggest found template pmap for use by the catalog
            catalogLargestTemplatePMap = Math.max(catalogLargestTemplatePMap, pmapMaxBytes);

            int opMask = OperatorMask.Group_Bit_Close;
            int openToken = groupOpenTokenStack[groupTokenStackHead];
            //we added 1 bit for the templateId and we do not want it to count so the accumulated
            //bits must be greater than one before marking this group as requiring a PMap.
            if (pmapMaxBits > 1) {
                opMask |= OperatorMask.Group_Bit_PMap;
                openToken |= (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER);
            }

            //
            int openGroupIdx = TokenBuilder.MAX_INSTANCE & openToken;
            int groupSize = catalogTemplateScriptIdx - openGroupIdx;
            // change open token so it has the total number of script steps
            // inside the group.
            catalogScriptTokens[openGroupIdx] = (groupOpenTokenStack[groupTokenStackHead] =
                                                (TokenBuilder.MAX_FIELD_MASK & openToken) | (TokenBuilder.MAX_FIELD_ID_VALUE & groupSize));
            openGroupIdx++; //fieldId already populated do not modify

            //add closing group to script
            catalogScriptTokens[catalogTemplateScriptIdx] = TokenBuilder.buildToken(TypeMask.Group, opMask, groupSize);
            catalogTemplateScriptIdx++; //fieldId already populated do not modify


            // we do need to decrement the stack counter because it was used for
            // capture of the pmap size
            groupTokenStackHead--;
            assert (-1 == groupTokenStackHead) : "poped off template so the stack should be empty again.";

            templateIdUnique++;

        }else if (qName.equalsIgnoreCase("templates")) {
            templatesXMLns = null;
        } else if (qName.equalsIgnoreCase("templateRef")) {
        	//tempalateRef is NOT a group but it is closed with the same flag
            // int opMask = OperatorMask.Group_Bit_Close;
            //             basicGroupClose(opMask);
        }
        else {
            templateEvents.add(new EndElementEvent(uri,localName,qName));
            if (templateId < 0)
                return;

            if (qName.equalsIgnoreCase("uint32") || qName.equalsIgnoreCase("int32")) {

                int token = buildToken(tokenBuilderIntCount);

                int optionalOffset = 0;
                if (fieldOperator == OperatorMask.Field_Default) {
                    if ((fieldType & 1) != 0) {
                        // optional default
                        optionalOffset = 1;
                    }
                }

                // only set if the value was given
                if (null != fieldOperatorValue && !fieldOperatorValue.isEmpty()) {
                    int tmp = Integer.parseInt(fieldOperatorValue);
                    defaultConstValues.addInitInteger(token & TokenBuilder.MAX_INSTANCE, tmp < 0 ? tmp : optionalOffset
                            + tmp);// +1 for optional not applied to negative
                                   // values.
                }
                // if no default is set the field must be undefined and therefore
                // remains zero which is the signed

                fieldOperatorValue = null;

                catalogScriptTokens[catalogTemplateScriptIdx] = token;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;

            } else if (qName.equalsIgnoreCase("uint64") || qName.equalsIgnoreCase("int64")) {

                int token = buildToken(tokenBuilderLongCount);

                int optionalOffset = 0;
                if (fieldOperator == OperatorMask.Field_Default) {
                    if ((fieldType & 1) != 0) {
                        // optional default
                        optionalOffset = 1;
                    }
                }

                // only set if the value was given
                if (null != fieldOperatorValue && !fieldOperatorValue.isEmpty()) {
                    long tmp = Long.parseLong(fieldOperatorValue);
                    defaultConstValues.addInitLong(token & TokenBuilder.MAX_INSTANCE, tmp < 0 ? tmp : optionalOffset + tmp);// +1
                                                                                                                            // for
                                                                                                                            // optional
                                                                                                                            // not
                                                                                                                            // applied
                                                                                                                            // to
                                                                                                                            // negative
                                                                                                                            // values.
                }
                // if no default is set the field must be undefined and therefore
                // remains zero

                fieldOperatorValue = null;

                catalogScriptTokens[catalogTemplateScriptIdx] = token;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;

            } else if (qName.equalsIgnoreCase("string")) {

                int token = buildToken(tokenBuilderByteCount);
                int idx = token & TokenBuilder.MAX_INSTANCE;

                // only set if the value was given
                if (null != fieldOperatorValue && !fieldOperatorValue.isEmpty()) {
                    defaultConstValues.addInit(idx, fieldOperatorValue.getBytes());
                } else {
                	//TODO:M, for someone who knows how FAST works,  this needs a unit test to ensure this null is represented as null all the way to the dictionary
                    defaultConstValues.addInit(idx, null);
                }
                fieldOperatorValue = null;

                catalogScriptTokens[catalogTemplateScriptIdx] = token;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;

            } else if (qName.equalsIgnoreCase("decimal")) {

                fieldOperator = fieldExponentOperator;
                int tokenExponent = buildToken(tokenBuilderIntCount);

                //Mantissa is NEVER optional because the optional logic is done by exponent.
                //Masking off the optional bit
                fieldType = 0xFFFFFFFE&fieldType;

                fieldOperator = fieldMantissaOperator;
                int tokenMantisssa = buildToken(tokenBuilderLongCount);

                int optionalExponentOffset = 0;
                if (fieldExponentOperator == OperatorMask.Field_Default) {
                    if ((fieldType & 1) != 0) {
                        // optional default
                        optionalExponentOffset = 1;
                    }
                }

                // only set if the value was given
                if (null != fieldExponentOperatorValue && !fieldExponentOperatorValue.isEmpty()) {
                    int tmp = Integer.parseInt(fieldExponentOperatorValue);
                    defaultConstValues.addInitInteger(tokenExponent & TokenBuilder.MAX_INSTANCE, tmp < 0 ? tmp
                            : optionalExponentOffset + tmp);// +1 for optional not
                                                            // applied to negative
                                                            // values.
                }
                // if no default is set the field must be undefined and therefore
                // remains zero

                fieldExponentOperatorValue = null;

                int optionalMantissaOffset = 0;
                if (fieldMantissaOperator == OperatorMask.Field_Default) {
                    if ((fieldType & 1) != 0) {
                        // optional default
                        optionalMantissaOffset = 1;
                    }
                }

                // only set if the value was given
                if (null != fieldMantissaOperatorValue && !fieldMantissaOperatorValue.isEmpty()) {
                    long tmp = Long.parseLong(fieldMantissaOperatorValue);
                    defaultConstValues.addInitLong(tokenMantisssa & TokenBuilder.MAX_INSTANCE, tmp < 0 ? tmp
                            : optionalMantissaOffset + tmp);// +1 for optional not
                                                            // applied to negative
                                                            // values.
                }
                // if no default is set the field must be undefined and therefore
                // remains zero

                fieldMantissaOperatorValue = null;

                catalogScriptTokens[catalogTemplateScriptIdx] = tokenExponent;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;

                catalogScriptTokens[catalogTemplateScriptIdx] = tokenMantisssa;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[catalogTemplateScriptIdx++] = fieldId;

                fieldPMapInc = 1;// set back to 1 we are leaving decimal processing
            } else if (qName.equalsIgnoreCase("exponent")) {
                fieldExponentOperator = fieldOperator;
                fieldExponentOperatorValue = fieldOperatorValue;
                fieldOperatorValue = null;
            } else if (qName.equalsIgnoreCase("mantissa")) {
                fieldMantissaOperator = fieldOperator;
                fieldMantissaOperatorValue = fieldOperatorValue;
                fieldOperatorValue = null;
            } else if (qName.equalsIgnoreCase("bytevector")) {

                int token = buildToken(tokenBuilderByteCount);

                catalogScriptTokens[    catalogTemplateScriptIdx] = token;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[  catalogTemplateScriptIdx++] = fieldId;

            } else if (qName.equalsIgnoreCase("length")) {
                // Length must be the first field inside of the sequence.

                int token = buildToken(tokenBuilderIntCount);

                // NOTE: we want the sequence length to come first then the
                // repeating group pmap therefore
                // we are waiting until now to add the open group token.
                catalogScriptTokens[    catalogTemplateScriptIdx] = token;
                catalogScriptFieldNames[catalogTemplateScriptIdx] = fieldName;
                fieldName=null;//ensure it is only used once
                catalogScriptFieldIds[  catalogTemplateScriptIdx++] = fieldId;

                catalogScriptTokens[catalogTemplateScriptIdx] = groupOpenTokenStack[groupTokenStackHead];
                catalogScriptFieldIds[catalogTemplateScriptIdx++] = 0;

            } else if (qName.equalsIgnoreCase("sequence")) {
            	int opMask = OperatorMask.Group_Bit_Close | OperatorMask.Group_Bit_Seq;
                basicGroupClose(opMask);

            } else if (qName.equalsIgnoreCase("group")) {
            	int opMask = OperatorMask.Group_Bit_Close;
                basicGroupClose(opMask);

            }
        }

    }

	private void basicGroupClose(int opMask) {
		int pmapMaxBits = groupOpenTokenPMapStack[groupTokenStackHead];
		int pmapMaxBytes = (pmapMaxBits + 6) / 7; // if bits is zero this
		                                          // will be zero.
		// System.err.println("y pmap bits "+pmapMaxBits+" "+pmapMaxBytes);
		catalogLargestNonTemplatePMap = Math.max(catalogLargestNonTemplatePMap, pmapMaxBytes);

		int openToken = groupOpenTokenStack[groupTokenStackHead];
		if (pmapMaxBytes > 0) {
		    opMask |= OperatorMask.Group_Bit_PMap;
		    openToken |= (OperatorMask.Group_Bit_PMap << TokenBuilder.SHIFT_OPER);
		}

		//
		int openGroupIdx = TokenBuilder.MAX_INSTANCE & openToken;
		int groupSize = catalogTemplateScriptIdx - openGroupIdx;

		// change open token so it has the total number of script steps
		// inside the group.
		catalogScriptTokens[openGroupIdx] = (groupOpenTokenStack[groupTokenStackHead] =
		                                    (TokenBuilder.MAX_FIELD_MASK & openToken) | (TokenBuilder.MAX_FIELD_ID_VALUE & groupSize));
		catalogScriptFieldIds[openGroupIdx++] = 0;

		//add closing group to script
		catalogScriptTokens[catalogTemplateScriptIdx] = TokenBuilder.buildToken(TypeMask.Group, opMask, groupSize);

		catalogScriptFieldIds[catalogTemplateScriptIdx++] = 0;

		groupTokenStackHead--;// pop this group off the stack to work on the
		                      // previous.
	}

    private int buildToken(AtomicInteger count) throws SAXException {
                
        int intFieldId = (int)fieldId; //TODO: Fix design limitation, Use new LongHashMap to map into ordal int of this matrix,  dictionaryMap is an array and limited to the lenght of an int not log
        if (((long)intFieldId) != fieldId) {
            throw new UnsupportedOperationException("Can not loop up, the field ID is too large.");
        }
        int token;
        int[] dTokens = dictionaryMap[intFieldId];
        if (null == dTokens || dTokens.length <= activeDictionary) {
            int[] newDTokens = new int[activeDictionary + 1];
            if (null != dTokens) {
                System.arraycopy(dTokens, 0, newDTokens, 0, dTokens.length);
            }
            int tokCount = count.getAndIncrement();

            //must do decimal resets as either int or long
            int saveAsType = (fieldType!=TypeMask.Decimal &&
                    fieldType!=TypeMask.DecimalOptional) ? fieldType :
                        (count==tokenBuilderLongCount ? TypeMask.LongSigned : TypeMask.IntegerSigned  );

            //Only USE Decimal for the exponent field and USE Long for the Mantissa field
            int tokenType = TypeMask.LongSigned==saveAsType? saveAsType : fieldType;
            newDTokens[activeDictionary] = token = TokenBuilder.buildToken(tokenType, fieldOperator, tokCount);

            saveResetListMembers(activeDictionary, saveAsType, tokCount, fieldOperator);
            fieldTokensUnique++;

            dictionaryMap[intFieldId] = dTokens = newDTokens;

        } else {
            token = dTokens[activeDictionary];
            if (0 != token && fieldType!= TypeMask.Decimal && fieldType!=TypeMask.DecimalOptional) { ///TODO: B, hack for now but need to clean up for decimals.
                if (fieldType != TokenBuilder.extractType(token) || fieldOperator != TokenBuilder.extractOper(token)) {
                    throw new SAXException("Field id "+intFieldId+"  0x"+Integer.toHexString(intFieldId)+" can not be redefined within the same dictionary. "+
                                            fieldType+" vs "+TokenBuilder.extractType(token)+"  "+
                                            fieldOperator+" vs "+TokenBuilder.extractOper(token)+" name:"+fieldName
                            );
                }
            } else {
                int tokCount = count.getAndIncrement();

                //must do decimal resets as either int or long
                int saveAsType = (fieldType!=TypeMask.Decimal&&fieldType!=TypeMask.DecimalOptional) ? fieldType :
                    (count==tokenBuilderLongCount ? TypeMask.LongSigned : TypeMask.IntegerSigned  );

                //Only USE Decimal for the exponent field and USE Long for the Mantissa field
                int tokenType = TypeMask.LongSigned==saveAsType? saveAsType : fieldType;

                dTokens[activeDictionary] = token = TokenBuilder.buildToken(tokenType, fieldOperator, tokCount);


                saveResetListMembers(activeDictionary, saveAsType, tokCount, fieldOperator);
                fieldTokensUnique++;
            }
        }

        return token;
    }

    private void saveResetListMembers(int activeDictionary, int fieldType, int tokCount, int fieldOperator) {

        if (TypeMask.GroupLength == fieldType) {
            return;// these are not needed for reset because it is part of the
                   // sequence definition.
        }

        // these never update the dictionary so they should never do a reset.
        if (OperatorMask.Field_None == fieldOperator ||
            OperatorMask.Field_Constant == fieldOperator ||
            OperatorMask.Field_Default == fieldOperator) {
            // System.err.println("skipped "+TypeMask.toString(fieldType));
            return;
        }

        // only need to group by major type.
        int d = activeDictionary << TokenBuilder.BITS_TYPE;

        if (fieldType < 0x0C) {
            fieldType = fieldType & 0xFC;
        } else {
            fieldType = fieldType & 0xFE;
        }

        int listId = d | fieldType;

        while (resetList.size() <= listId) {
            resetList.add(new ArrayList<Integer>());
        }

        // these are ever increasing in value, the order makes a difference in
        // performance at run time.
        assert (resetList.get(listId).size() == 0 || resetList.get(listId).get(resetList.get(listId).size() - 1).intValue() < tokCount);
        resetList.get(listId).add(tokCount);
    }

    private void buildDictionaryMemberLists() {

        // walk the lists of dictionary members and join them into a master list
        // for each dictionary.
        // each section must start with stop bit and type for the following
        // identifiers. All cache friendly forward motion.

        int dictionaryCount = dictionaryNames.size();

        tokenIdxMembers = new int[dictionaryCount][];

        tokenIdxMemberHeads = new int[dictionaryCount];

        int j = resetList.size();
        while (--j >= 0) {
            if (!resetList.get(j).isEmpty()) {
            	final int d = j >>> TokenBuilder.BITS_TYPE;

        		//only allocate exactly what is needed, when it is needed for the type needed
                if (null == tokenIdxMembers[d]) {
                	tokenIdxMembers[d] = new int[lengthOfArrayForThisType(d)];
                }

                int t = j & TokenBuilder.MASK_TYPE;
                int stopInt = 0xFFFF0000 | t;
                tokenIdxMembers[d][tokenIdxMemberHeads[d]++] = stopInt;
                // System.err.println("stopInt:"+stopInt+" "+Integer.toBinaryString(stopInt)+" "+TypeMask.toString(t));
                for (Integer i : resetList.get(j)) {
                    tokenIdxMembers[d][tokenIdxMemberHeads[d]++] = i.intValue();
                }


            }
        }
        // tokenIdxMembers are ready to be saved but must be trimmed by heads

    }


	private int lengthOfArrayForThisType(int target) {
		int maxTokens = 0;
        int j = resetList.size();
        while (--j >= 0) {
        	final int d = j >>> TokenBuilder.BITS_TYPE;
            if (d == target) {
	        	List<Integer> list = resetList.get(j);
	        	if (!list.isEmpty()) {
	        		maxTokens = maxTokens + list.size() + 1;
	        	}
            }
        }
		return maxTokens;
	}

    public static void postProcessDictionary(TemplateHandler handler, int byteGap, int maxByteLength) {
		handler.buildDictionaryMemberLists();

        // the catalog file need not be "Small" but it probably will be.
        // the catalog file must be "Fast" to load without any "Processing"
        // needed by the consumer.
        // this enables fast startup/recovery times that do not produce garbage.

        handler.defaultConstValues.setTypeCounts(   handler.tokenBuilderIntCount.intValue(),
									        		handler.tokenBuilderLongCount.intValue(),
									        		handler.tokenBuilderByteCount.intValue(),
			                               byteGap,
			                               maxByteLength);
	}
    
    public static FieldReferenceOffsetManager from(TemplateHandler handler, short preambleBytes) {   
        return from(handler, preambleBytes,"Catalog");
    }
    
    public static FieldReferenceOffsetManager from(TemplateHandler handler, short preambleBytes, String name) {                
    	return  new FieldReferenceOffsetManager(
    			  Arrays.copyOfRange(handler.catalogScriptTokens,0,handler.catalogTemplateScriptIdx), 
       		      preambleBytes, 
       		      Arrays.copyOfRange(handler.catalogScriptFieldNames,0,handler.catalogTemplateScriptIdx),
       		      Arrays.copyOfRange(handler.catalogScriptFieldIds,0,handler.catalogTemplateScriptIdx),
       		      Arrays.copyOfRange(handler.catalogScriptDictionaryNames,0,handler.catalogTemplateScriptIdx),
       		      name);
    }
    
    public static FieldReferenceOffsetManager loadFrom(String source) throws ParserConfigurationException, SAXException, IOException {
    	return loadFrom(source,(short)0);
    }
    
	public static FieldReferenceOffsetManager loadFrom(String source, short preamble) throws ParserConfigurationException, SAXException, IOException {

		InputStream sourceInputStream = TemplateHandler.class.getResourceAsStream(source);

		File folder = null;
        if (null == sourceInputStream) {
        	folder = new File(source);
        	if (folder.exists() && !folder.isDirectory()) {
        		sourceInputStream = new FileInputStream(source);
        	}
        }
        
        TemplateHandler handler = new TemplateHandler();
        
        SAXParserFactory spfac = SAXParserFactory.newInstance();
        SAXParser sp = spfac.newSAXParser();
        if (null != sourceInputStream) {
        	sp.parse(sourceInputStream, handler);
        } else {
        	for (File f : folder.listFiles()) {
        		if (f.isFile()) {
        			sp.parse(f, handler);
        		}
        	}
        }
        
        return TemplateHandler.from(handler,preamble, simpleName(source));
	}

    public static String simpleName(String source) {
        return source.substring(Math.max(Math.max(0, 1+source.lastIndexOf('/')), Math.max(0, 1+source.lastIndexOf('\\'))));
    }

    public static FieldReferenceOffsetManager loadFrom(InputStream sourceInputStream) throws ParserConfigurationException, SAXException, IOException {
        
        TemplateHandler handler = new TemplateHandler();
        
        SAXParserFactory spfac = SAXParserFactory.newInstance();
        SAXParser sp = spfac.newSAXParser();
        sp.parse(sourceInputStream, handler);
        
        return TemplateHandler.from(handler,(short)0);
    }

    public static void buildFROMConstructionSource(StringBuilder target, FieldReferenceOffsetManager expectedFrom, String varName, String fromName) {
        //write out the expected source.
        target.append("public final static FieldReferenceOffsetManager ");
        target.append(varName).append(" = new ").append(FieldReferenceOffsetManager.class.getSimpleName()).append("(\n");

        target.append("    new int[]{");
            for(int token:expectedFrom.tokens) {
                target.append("0x").append(Integer.toHexString(token)).append(',');
            }
        target.setLength(target.length()-1);
        target.append("},\n    ");
        
        target.append("(short)").append(0).append(",\n");// expectedFrom.preambleBytes;//TODO: swap in
        
        target.append("    new String[]{");
        for(String tmp:expectedFrom.fieldNameScript) {
            if (null==tmp) {
                target.append("null,");
            } else {
                target.append('"').append(tmp).append("\",");
            }
        }
        target.setLength(target.length()-1);
        target.append("},\n");

        target.append("    new long[]").append(Arrays.toString(expectedFrom.fieldIdScript).replaceAll("\\[","\\{").replaceAll("\\]","\\}")).append(",\n");
        
        target.append("    new String[]{");
        for(String tmp:expectedFrom.dictionaryNameScript) {
            if (null==tmp) {
                target.append("null,");
            } else {
                target.append('"').append(tmp).append("\",");
            }
        }
        target.setLength(target.length()-1);
        target.append("},\n");
        
        target.append("    \""+fromName+"\");");
    }
    

}
