//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class TemplateCatalog {

	//because optional values are sent as +1 when >= 0 it is not possible to send the
	//largest supported positive value, as a result this is the ideal default because it
	//can not possibly collide with any real values
	public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;
	public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;
	private static final long NO_ID = 0xFFFFFFFF00000000l;
	
	final long[][] scriptsCatalog; //top 32 bits id, lower 32 bits token
	final DictionaryFactory dictionaryFactory;
	final int maxTemplatePMapSize;
	final int maxNonTemplatePMapSize;
	final int maxFieldId;
	
	final int[][] dictionaryMembers;
	
	//Runtime specific message prefix, only used for some transmission technologies
	byte prefixSize=0; //default is none
	int maxTextLength = 16;//default
	int maxByteVectorLength = 16;
	
	
	public TemplateCatalog(PrimitiveReader reader) {
				
		int templatePow = reader.readIntegerUnsigned();
		assert(templatePow<32) : "Corrupt catalog file";
		scriptsCatalog = new long[1<<templatePow][];
		
		loadTemplateScripts(reader);
		
		int dictionaryCount = reader.readIntegerUnsigned();
		dictionaryMembers = new int[dictionaryCount][];
		
		loadDictionaryMembers(reader);
		
		maxFieldId = reader.readIntegerUnsigned();
		//it is assumed that template PMaps are smaller or larger than the other PMaps so these are kept separate
		maxTemplatePMapSize = reader.readIntegerUnsigned();
		maxNonTemplatePMapSize = reader.readIntegerUnsigned();
		
	//	System.err.println("PMaps sizes templates:"+maxTemplatePMapSize+" nonTemplates:"+maxNonTemplatePMapSize+" both should be very small for best peformance.");

		
		dictionaryFactory = new DictionaryFactory(reader);
		
	}




	public void setMessagePrefix(byte prefixSize) {
		this.prefixSize = prefixSize;
	}
	
	//Assumes that the tokens are already loaded and ready for use.
	private void loadTemplateScripts(PrimitiveReader reader) {
		
		int templatesInCatalog = reader.readIntegerUnsigned();
		
		int tic = templatesInCatalog;
		while (--tic>=0) {
			int templateId = reader.readIntegerUnsigned();
			int s = reader.readIntegerUnsigned();
			long[] script = new long[s]; //top 32 are id, low 32 are token
			while (--s>=0) { //TODO: need to add full field names to be looked up upon error etc.
				script[s] = reader.readLongUnsigned();
				
				//int fieldId = (int)(script[s]>>>32);
				//int token = (int)(script[s]&0xFFFFFFFF);
				//System.err.println("Load:"+Long.toHexString(script[s])+" "+fieldId+" "+TokenBuilder.tokenToString(token));

				
			}
			

			//save the script into the catalog
			scriptsCatalog[templateId] = script;
			
			//one bit in pmap for tempalteid?
			//seqNO channel  pmap  templateId message
			// 4     1        n       x        y
			
			//first bit template
			//pmap               template    34        52     131
			//64                    2         1      58782   string
			
			//TODO: build a helper for printing scripts with human readable commands
			//System.err.println("load new template:"+templateId+" len "+script.length+"  "+Arrays.toString(script));
			
	//		stream    message* | block*
	//		block     BlockSize message+
	//		message   segment
	//		segment   PresenceMap TemplateIdentifier? (field | segment)*
	//*		field     integer | string | delta | ScaledNumber | ByteVector
	//*		integer   UnsignedInteger | SignedInteger
	//*		string    ASCIIString | UnicodeString
	//*		delta     IntegerDelta | ScaledNumberDelta | ASCIIStringDelta |ByteVectorDelta
						
			
		}
	}
	

	
	public static void save(PrimitiveWriter writer, 
			                  int uniqueIds, int biggestId, 
			                  int uniqueTemplateIds, int biggestTemplateId,
			                  long[][] scripts, DictionaryFactory df, 
			                  int maxTemplatePMap, int maxNonTemplatePMap, 
			                  int[][] tokenIdxMembers, int[] tokenIdxMemberHeads) {
		
		//TODO: Remove absent value this will be set client side as needed and can be different.
		
		saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, scripts);			
		
		saveDictionaryMembers(writer, tokenIdxMembers, tokenIdxMemberHeads);
				
		writer.writeIntegerUnsigned(biggestId);
	//	System.err.println("save pmap sizes "+maxTemplatePMap+" "+maxNonTemplatePMap);
		writer.writeIntegerUnsigned(maxTemplatePMap);
		writer.writeIntegerUnsigned(maxNonTemplatePMap);

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
	private static void saveTemplateScripts(PrimitiveWriter writer, int uniqueTemplateIds, int biggestTemplateId,
			long[][] scripts) {
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
				
		//total number of templates are are defining here in the catalog
		writer.writeIntegerUnsigned(uniqueTemplateIds);		
		//now write each template
		int j = scripts.length;
		int x = 0;
		while (--j>=0) {
			long[] script = scripts[j];
			if (null!=script) {
				x++;
				writer.writeIntegerUnsigned(j);
				int i = script.length;
				writer.writeIntegerUnsigned(i);//length of script written first
				
				//System.err.println(j+" has script length of "+i);
				
				while (--i>=0) {
					//int fieldId = (int)(script[i]>>>32);
					//int token = (int)(script[i]&0xFFFFFFFF);
					//.err.println("Write:"+Long.toHexString(script[i])+" "+fieldId+" "+TokenBuilder.tokenToString(token));
					writer.writeLongUnsigned(script[i]);
				}
			}
		}
	}
	
	public long[] templateScript(int templateId) {
		return scriptsCatalog[templateId];
	}

	public int templatesCount() {
		int tmp = 0;
		int x=scriptsCatalog.length;
		while (--x>=0) {
			if (null!=scriptsCatalog[x]) {
				tmp++;
			}
		}
		return tmp;
	}
	
	public DictionaryFactory dictionaryFactory() {
		return dictionaryFactory;
	}

	public byte getMessagePrefixSize() {
		return prefixSize;
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




	public void setMaxTextLength(int maxTextLength) {
		this.maxTextLength = maxTextLength;
	}




	public int getMaxByteVectorLength() {
		return maxByteVectorLength;
	}




	public void setMaxByteVectorLength(int maxByteVectorLength) {
		this.maxByteVectorLength = maxByteVectorLength;
	}

	
	
}
