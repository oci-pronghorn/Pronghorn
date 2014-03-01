//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.loader;

import java.util.Arrays;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.primitive.PrimitiveReader;
import com.ociweb.jfast.primitive.PrimitiveWriter;
import com.ociweb.jfast.primitive.adapter.FASTInputStream;
import com.ociweb.jfast.stream.FASTDynamicReader;
import com.ociweb.jfast.stream.FASTReaderDispatch;

public class TemplateCatalog {

	//because optional values are sent as +1 when >= 0 it is not possible to send the
	//largest supported positive value, as a result this is the ideal default because it
	//can not possibly collide with any real values
	public static final int DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT = Integer.MAX_VALUE;
	public static final long DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG = Long.MAX_VALUE;
	private static final long NO_ID = 0xFFFFFFFF00000000l;
	
	final int[] tokenLookup;
	final long[] absent;
	final long[][] scriptsCatalog; //top 32 bits id, lower 32 bits token
	
	//Runtime specific message prefix, only used for some transmission technologies
	int prefixId=-1;  
	int prefixSize=0; //default is none
	
	public TemplateCatalog(PrimitiveReader reader) {
				
		int tokenPow = reader.readIntegerUnsigned();
		assert(tokenPow<32) : "Corrupt catalog file";
		int maxTokens = 1<<tokenPow;
		
		tokenLookup = new int[maxTokens];
		absent = new long[maxTokens];
		
		loadTokens(reader);

		
		int templatePow = reader.readIntegerUnsigned();
		assert(templatePow<32) : "Corrupt catalog file";
		scriptsCatalog = new long[1<<templatePow][];
		
		loadTemplateScripts(reader);
		
				
	}

	public int[] tokenLookup() {
		return tokenLookup;
	}

	
	public void setMessagePrefix(int prefixId, int prefixSize) {
		this.prefixId = prefixId;
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
					int token = tokenLookup[tmp];
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
	
	private void loadTokens(PrimitiveReader reader) {
						
		int i = reader.readIntegerUnsigned();
		while (--i>=0) {
			int id=reader.readIntegerUnsigned();
						
			tokenLookup[id]=reader.readIntegerSigned();
			
			//System.err.println("LOAD:"+id+"  token:"+TokenBuilder.tokenToString(tokens[id])+" _ "+Integer.toHexString(tokens[id]));
			
			switch(reader.readIntegerUnsigned()) {
				case 0:
					absent[id]=TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT;
					break;
				case 1:
					absent[id]=TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG;
					break;
				case 2:
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
		
		saveTokens(writer, uniqueIds, biggestId, tokenLookup, absentValue);
		saveTemplateScripts(writer, uniqueTemplateIds, biggestTemplateId, scripts);				
				
//		int integerCount=0;
//		int longCount=0; 
//		int charCount=0; 
//        int singleCharLength=0; 
//        int decimalCount=0; 
//        int bytesCount=0; 
//		
//		DictionaryFactory df = new DictionaryFactory(integerCount, longCount, charCount, 
//													singleCharLength, decimalCount, bytesCount,
//													tokenLookup);
//		df.save(writer);
		
		
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
		while (--i>=0) {
			int token = tokenLookup[i];
			assert(TokenBuilder.tokenToString(token).indexOf("unknown")==-1): "Bad token "+TokenBuilder.tokenToString(token);
			if (token<0) {
//			System.err.println("SAVE:"+i+"  token:"+TokenBuilder.tokenToString(token)+" _ "+Integer.toHexString(token));

				writer.writeIntegerUnsigned(i);
				writer.writeIntegerSigned(token);
				
				if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_INT==absentValue[i]) {
					writer.writeIntegerUnsigned(0);
				} else 	if (TemplateCatalog.DEFAULT_CLIENT_SIDE_ABSENT_VALUE_LONG==absentValue[i]) {
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


	
	

	
	
}
