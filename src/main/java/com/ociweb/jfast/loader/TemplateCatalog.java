//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class TemplateCatalog {

	//because optional values are sent as +1 when >= 0 it is not possible to send the
	//largest supported positive value, as a result this is the ideal default because it
	//can not possibly collide with any real values
	public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;
	public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;
	private static final long NO_ID = 0xFFFFFFFF00000000l;
	
	final DictionaryFactory dictionaryFactory;
	final int maxTemplatePMapSize;
	final int maxNonTemplatePMapSize;
	final int maxPMapDepth;
	final int maxFieldId;
	
	final int[] templateStartIdx; //TODO: X, these two arrays can be shortened!
	final int[] templateLimitIdx;
	
	final int[] scriptTokens;
	final int[] scriptFieldIds;
	final int templatesInCatalog;
	
	final int[][] dictionaryMembers;
	
	//Runtime specific message prefix, only used for some transmission technologies
	byte preambleSize=0; //default is none
	int maxTextLength = 16;//default
	int maxByteVectorLength = 16;//default
	int textLengthGap = 8;//default
	int byteVectorGap = 8;//default
	
	public static final int END_OF_SEQ_ENTRY = 0x01;
	public static final int END_OF_MESSAGE = 0x02;
	
	public int getTemplateStartIdx(int templateId) {
		return templateStartIdx[templateId];
	}
	public int getTemplateLimitIdx(int templateId) {
		return templateLimitIdx[templateId];
	}
	
	public TemplateCatalog(PrimitiveReader reader) {
				
		int templatePow = reader.readIntegerUnsigned();
		assert(templatePow<32) : "Corrupt catalog file";
		templateStartIdx = new int[1<<templatePow];
		templateLimitIdx = new int[1<<templatePow];		
		
		int fullScriptLength = reader.readIntegerUnsigned();
		scriptTokens = new int[fullScriptLength];
		scriptFieldIds = new int[fullScriptLength];
		templatesInCatalog = reader.readIntegerUnsigned();
		
		loadTemplateScripts(reader);
		
		int dictionaryCount = reader.readIntegerUnsigned();
		dictionaryMembers = new int[dictionaryCount][];
		
		loadDictionaryMembers(reader);
		
		maxFieldId = reader.readIntegerUnsigned();
		//it is assumed that template PMaps are smaller or larger than the other PMaps so these are kept separate
		maxTemplatePMapSize = reader.readIntegerUnsigned();
		maxNonTemplatePMapSize = reader.readIntegerUnsigned();
		maxPMapDepth = reader.readIntegerUnsigned();
		
	//	System.err.println("PMaps sizes templates:"+maxTemplatePMapSize+" nonTemplates:"+maxNonTemplatePMapSize+" both should be very small for best peformance.");

		
		dictionaryFactory = new DictionaryFactory(reader);
		
	}




	public void setMessagePreambleSize(byte size) {
		this.preambleSize = size;
	}
	
	//Assumes that the tokens are already loaded and ready for use.
	private void loadTemplateScripts(PrimitiveReader reader) {
				
		int i = templatesInCatalog;
		while (--i>=0) {
			//look up for script index given the templateId
			int templateId = reader.readIntegerUnsigned();
			templateStartIdx[templateId] = reader.readIntegerUnsigned();
			templateLimitIdx[templateId] = reader.readIntegerUnsigned();
			//System.err.println("templateId "+templateId);			
		}
		//System.err.println("total:"+templatesInCatalog);

		i = scriptTokens.length;
		while (--i>=0) {
			scriptTokens[i]=reader.readIntegerSigned();
			scriptFieldIds[i]=reader.readIntegerUnsigned(); 
		}
		
	//	System.err.println("script tokens/fields "+scriptTokens.length);//46
	//	System.err.println("templateId idx start/stop count "+this.templateStartIdx.length);//128
		

	}
	

//	//		stream    message* | block*
//	//		block     BlockSize message+
//	//		message   segment
//	//		segment   PresenceMap TemplateIdentifier? (field | segment)*
//	//*		field     integer | string | delta | ScaledNumber | ByteVector
//	//*		integer   UnsignedInteger | SignedInteger
//	//*		string    ASCIIString | UnicodeString
//	//*		delta     IntegerDelta | ScaledNumberDelta | ASCIIStringDelta |ByteVectorDelta
	
	public static void save(PrimitiveWriter writer, 
			                  int uniqueIds, int biggestId, 
			                  int uniqueTemplateIds, int biggestTemplateId,
			                  DictionaryFactory df, int maxTemplatePMap, 
			                  int maxNonTemplatePMap, int[][] tokenIdxMembers, 
			                  int[] tokenIdxMemberHeads,
			                  int[] catalogScriptTokens,
			                  int[] catalogScriptFieldIds,
			                  int scriptLength,
			                  int[] templateIdx, 
			                  int[] templateLimit,
			                  int maxPMapDepth) {
		
		
		saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, 
				            catalogScriptTokens,
				            catalogScriptFieldIds, scriptLength,
				            templateIdx, templateLimit);			
		
		saveDictionaryMembers(writer, tokenIdxMembers, tokenIdxMemberHeads);
				
		writer.writeIntegerUnsigned(biggestId);
	//	System.err.println("save pmap sizes "+maxTemplatePMap+" "+maxNonTemplatePMap);
		writer.writeIntegerUnsigned(maxTemplatePMap);
		writer.writeIntegerUnsigned(maxNonTemplatePMap);
		writer.writeIntegerUnsigned(maxPMapDepth);

		df.save(writer);
		
		
		
	}

	
	private static void saveDictionaryMembers(PrimitiveWriter writer, int[][] tokenIdxMembers, int[] tokenIdxMemberHeads) {
		//save count of dictionaries
		int dictionaryCount = tokenIdxMembers.length;
		writer.writeIntegerUnsigned(dictionaryCount);
		//
		int d = dictionaryCount;
		while (--d>=0) {
			int[] members = tokenIdxMembers[d];
			int h = tokenIdxMemberHeads[d];
			writer.writeIntegerUnsigned(h);//length of reset script (eg member list)
			while (--h>=0) {
				writer.writeIntegerSigned(members[h]);
			}			
		}
	}

	
	private void loadDictionaryMembers(PrimitiveReader reader) {
		// //target  int[][]  dictionaryMembers
		int dictionaryCount = dictionaryMembers.length;
		int d = dictionaryCount;
		while (--d>=0) {
			int h = reader.readIntegerUnsigned();//length of reset script (eg member list)
			int[] members = new int[h];
			while (--h>=0) {
				members[h] = reader.readIntegerSigned();
			}	
			dictionaryMembers[d] = members;
		}
	}
	

    private int[] resetList(int dictionary) {
    	return dictionaryMembers[dictionary];
    }


	/**
	 * 
	 * Save template scripts to the catalog file.
	 * The Script is made up of the field id(s) or Tokens.
	 * Each field value needs to know the id so it is stored by id.
	 * All other types (group tasks,dictionary tasks) just need to
	 * be executed so they are stored as tokens only.  These special
	 * tasks frequently multiple tokens to a single id which requires
	 * that the token is used in all cases.  An example is the 
	 * Open and Close tokens for a given group.
	 *  
	 * 
	 * @param writer
	 * @param uniqueTemplateIds
	 * @param biggestTemplateId
	 * @param scripts
	 */
	private static void saveTemplateScripts(PrimitiveWriter writer, 
			                                  int uniqueTemplateIds, int biggestTemplateId,
			    			                  int[] catalogScriptTokens,
			    			                  int[] catalogScriptFieldIds,
			    			                  int scriptLength,
			    			                  int[] templateStartIdx, 
			    			                  int[] templateLimitIdx) {
		//what size array will we need for template lookup. this must be a power of two
		//therefore we will only store the exponent given a base of two.
		//this is not so much for making the file smaller but rather to do the computation
		//now instead of at runtime when latency is an issue.
		int pow = 0;  
		int tmp = biggestTemplateId;
		while (tmp!=0) {
			pow++;
			tmp = tmp>>1;
		}
		assert(pow<32);
		writer.writeIntegerUnsigned(pow);//will be < 32
		writer.writeIntegerUnsigned(scriptLength);
				
		//total number of templates are are defining here in the catalog
		writer.writeIntegerUnsigned(uniqueTemplateIds);	
		//write each template index
		int i = templateStartIdx.length;
		while (--i>=0) {
			if (0!=templateStartIdx[i]) {
				writer.writeIntegerUnsigned(i);
				writer.writeIntegerUnsigned(templateStartIdx[i]-1); //return the index to its original value (-1)
				writer.writeIntegerUnsigned(templateLimitIdx[i]);
			}			
		}
		
		//write the scripts
		i = scriptLength;
		while (--i>=0) {
			writer.writeIntegerSigned(catalogScriptTokens[i]);
			writer.writeIntegerUnsigned(catalogScriptFieldIds[i]); //not sure about how helpfull this structure is.
		}
				
		
	}
	
	public DictionaryFactory dictionaryFactory() {
		return dictionaryFactory;
	}

	public byte getMessagePreambleSize() {
		return preambleSize;
	}

	public int maxTemplatePMapSize() {
		return maxTemplatePMapSize;
	}


	public int maxFieldId() {
		return maxFieldId;
	}

	public int[][] dictionaryMembers() {
		return dictionaryMembers;
	}




	public int getMaxTextLength() {
		return maxTextLength;
	}

	public void setMaxTextLength(int maxTextLength, int gap) {
		this.maxTextLength = maxTextLength;
		this.textLengthGap = gap;
	}

	public int getMaxByteVectorLength() {
		return maxByteVectorLength;
	}

	public void setMaxByteVectorLength(int maxByteVectorLength, int gap) {
		this.maxByteVectorLength = maxByteVectorLength;
		this.byteVectorGap = gap;
	}


	public int templatesCount() {
		return templatesInCatalog;
	}

	public int[] fullScript() {
		return scriptTokens;
	}

	public int getByteVectorGap() {
		return byteVectorGap;
	}
	
	public int getTextGap() {
		return textLengthGap;
	}
	public int maxNonTemplatePMapSize() {
		return maxNonTemplatePMapSize;
	}
	
	public int maxPMapDepth() { //TODO: X, Move back into PM, not sure?
		//adds 2 between each template for max depth of usage, needed to allocate space
		return (2+((Math.max(maxTemplatePMapSize,maxNonTemplatePMapSize)+2)*maxPMapDepth));
	}
	
	public int getMaxGroupDepth() {
		return maxPMapDepth;
	}
	
}
