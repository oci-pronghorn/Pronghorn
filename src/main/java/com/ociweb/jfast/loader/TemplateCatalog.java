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
	
	final long[] absent;
	final long[][] scriptsCatalog; //top 32 bits id, lower 32 bits token
	final DictionaryFactory dictionaryFactory;
	final int maxTemplatePMapSize;
	final int maxNonTemplatePMapSize;
	
	
	//Runtime specific message prefix, only used for some transmission technologies
	int prefixSize=0; //default is none
	
	public TemplateCatalog(PrimitiveReader reader) {
				
		int tokenPow = reader.readIntegerUnsigned();
		assert(tokenPow<32) : "Corrupt catalog file";
		int maxTokens = 1<<tokenPow;
		
		absent = new long[maxTokens];
		
		int templatePow = reader.readIntegerUnsigned();
		assert(templatePow<32) : "Corrupt catalog file";
		scriptsCatalog = new long[1<<templatePow][];
		
		loadTemplateScripts(reader);
		
		//it is assumed that template PMaps are smaller or larger than the other PMaps so these are kept separate
		maxTemplatePMapSize = reader.readIntegerUnsigned();
		maxNonTemplatePMapSize = reader.readIntegerUnsigned();
		
	//	System.err.println("PMaps sizes templates:"+maxTemplatePMapSize+" nonTemplates:"+maxNonTemplatePMapSize+" both should be very small for best peformance.");

		
		dictionaryFactory = new DictionaryFactory(reader);
		
	}

	
	public void setMessagePrefix(int prefixSize) {
		this.prefixSize = prefixSize;
	}
	
	//Assumes that the tokens are already loaded and ready for use.
	private void loadTemplateScripts(PrimitiveReader reader) {
		
		int templatesInCatalog = reader.readIntegerUnsigned();
		
		int tic = templatesInCatalog;
		while (--tic>=0) {
			int templateId = reader.readIntegerUnsigned();
			int templateScriptLength = reader.readIntegerUnsigned();
			int s = templateScriptLength;
			long[] script = new long[s]; //top 32 are id, low 32 are token
			while (--s>=0) { //TODO: need to add full field names to be looked up upon error etc.
				int tmp = reader.readIntegerSigned();
				if (tmp<0) {
					//tmp is token
					script[s] = NO_ID|tmp;
				} else {
					//tmp is id
					int token = 0;//tokenLookup[tmp];
					long x = tmp;
					script[s] = (x<<32) | (0xFFFFFFFFl&token);					
				}
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
			                  long[] absentValue, int uniqueTemplateIds,
			                  int biggestTemplateId, long[][] scripts, 
			                  DictionaryFactory df, int maxTemplatePMap, 
			                  int maxNonTemplatePMap) {
		
		//TODO: Remove absent value this will be set client side as needed and can be different.
		
		saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, scripts);				
				
	//	System.err.println("save pmap sizes "+maxTemplatePMap+" "+maxNonTemplatePMap);
		writer.writeIntegerUnsigned(maxTemplatePMap);
		writer.writeIntegerUnsigned(maxNonTemplatePMap);

		df.save(writer);
		
		
		
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
//		int templateId = scripts.length;
//		while (--templateId>=0) {
//			int[] script = scripts[templateId];
//			if (null!=script) {
//				writer.writeIntegerUnsigned(templateId);
//				int i = script.length;
//				writer.writeIntegerUnsigned(i);//length of script written first
//				//TODO: delete System.err.println(templateId+" has script length of "+i);
//				while (--i>=0) {
//					writer.writeIntegerSigned(script[i]);
//				}
//			}
//		}
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

	public int getMessagePrefixSize() {
		return prefixSize;
	}

	public int maxTemplatePMapSize() {
		return maxTemplatePMapSize;
	}


	public int maxFieldId() {
		// TODO Auto-generated method stub
		return 0;
	}

	
	
}
