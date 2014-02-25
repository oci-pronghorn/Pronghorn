//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;

public class Catalog {

	//because optional values are sent as +1 when >= 0 it is not possible to send the
	//largest supported positive value, as a result this is the ideal default because it
	//can not possibly collide with any real values
	public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;
	public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;
	
	final int[] ids;
	final int[] tokens;
	final long[] absent;
	final int[][] scriptsCatalog;
	
	
	public Catalog(PrimitiveReader reader) {
		
		int templatePow = reader.readIntegerUnsigned();
		assert(templatePow<32) : "Corrupt catalog file";
		scriptsCatalog = new int[1<<templatePow][];
		
		loadTemplateScripts(reader);
		
		
		int tokenPow = reader.readIntegerUnsigned();
		assert(tokenPow<32) : "Corrupt catalog file";
		int maxTokens = 1<<tokenPow;
		ids = new int[maxTokens];
		tokens = new int[maxTokens];
		absent = new long[maxTokens];
		
		loadTokens(reader);
		
		
	}

	private int[][] loadTemplateScripts(PrimitiveReader reader) {
		
		int templatesInCatalog = reader.readIntegerUnsigned();
		
		int tic = templatesInCatalog;
		while (--tic>=0) {
			int templateId = reader.readIntegerUnsigned();
			int templateScriptLength = reader.readIntegerUnsigned();
			int s = templateScriptLength;
			int[] script = new int[s];
			while (--s>=0) {
				script[s] = reader.readIntegerSigned();
			}
			//save the script into the catalog
			scriptsCatalog[templateId] = script;
		}
		return scriptsCatalog;
	}
	
	private void loadTokens(PrimitiveReader reader) {
						
		int i = reader.readIntegerUnsigned();
		while (--i>=0) {
			int id=reader.readIntegerUnsigned();
			tokens[id]=reader.readIntegerSigned();
			switch(reader.readIntegerUnsigned()) {
				case 0:
					absent[id]=Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
					break;
				case 1:
					absent[id]=Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
					break;
				case 3:
					absent[id]=reader.readLongSigned();
					break;
			}
		}
	}


	
	public static void save(PrimitiveWriter writer, 
			                  int uniqueIds, int biggestId, 
			                  int[] tokenLookup, long[] absentValue,
			                  int uniqueTemplateIds, int biggestTemplateId, 
			                  int[][] scripts) {
		
		saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, scripts);				
		saveTokens(writer, uniqueIds, biggestId, tokenLookup, absentValue);
				
	}

	private static void saveTokens(PrimitiveWriter writer, int uniqueIds, int biggestId, int[] tokenLookup,
			long[] absentValue) {
		int temp = biggestId;
		int base2Exponent = 0;
		while (0!=temp) {
			temp = temp>>1;
			base2Exponent++;
		}
		//this is how big we need to make the lookup arrays
		writer.writeIntegerUnsigned(base2Exponent);
		//this is how many values we are about to write to the stream
		writer.writeIntegerUnsigned(uniqueIds);
		//this is each value, id, token and absent
		int i = tokenLookup.length;
		while (--i<=0) {
			int token = tokenLookup[i];
			if (0!=token) {
				writer.writeIntegerUnsigned(i);
				writer.writeIntegerSigned(token);
				
				if (Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT==absentValue[i]) {
					writer.writeIntegerUnsigned(0);
				} else 	if (Catalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG==absentValue[i]) {
					writer.writeIntegerUnsigned(1);
				} else {
					writer.writeIntegerUnsigned(2);
					writer.writeLongSigned(absentValue[i]);								
				}
				
			}			
		}
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
			int[][] scripts) {
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
		int templateId = scripts.length;
		while (--templateId>=0) {
			int[] script = scripts[templateId];
			if (null!=script) {
				writer.writeIntegerUnsigned(templateId);
				int i = script.length;
				writer.writeIntegerUnsigned(i);//length of script written first
				while (--i>=0) {
					writer.writeIntegerSigned(script[i]);
				}
			}
		}
	}

	public int templates() {
		int templates = 0;
		int x=scriptsCatalog.length;
		while (--x>=0) {
			if (null!=scriptsCatalog) {
				templates++;
			}
		}
		return templates;
	}
	
	

	
	
}
