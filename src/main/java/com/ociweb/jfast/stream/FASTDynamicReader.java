//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.loader.DictionaryFactory;
import com.ociweb.jfast.loader.TemplateCatalog;
import com.ociweb.jfast.primitive.PrimitiveReader;

/*
 * Implementations of read can use this object
 * to pull the most recent parsed values of any available fields.
 * Even those in outer groups may be read however values appearing in the template
 * after the <groupId> will not have been read yet and are not available.
 * If those values are needed wait until this method is called with the
 * desired surrounding <groupId>.
 * 
 * Supports dynamic modification of the templates including:  
 * 		Field compression/operation type changes.
 * 	    Field order changes within a group.
 *      Mandatory/Optional field designation.
 *      Pulling up fields from group to the surrounding group.
 *      Pushing down fields from group to the internal group.
 * 
 * In some cases after modification the data will no longer be available
 * and unexpected results can occur.  Caution must be used whenever pulling up
 * or pushing down fields as it probably changes the meaning of the data. 
 * 
 */
public class FASTDynamicReader implements FASTDataProvider {

	private final FASTReaderDispatch readerDispatch;
	private final TemplateCatalog catalog;
	
	long[] activeScript;
	int activeScriptIdx;
	
	//read groups field ids and build repeating lists of tokens.
	
	//only look up the most recent value read and return it to the caller.
	public FASTDynamicReader(PrimitiveReader reader, TemplateCatalog catalog) {
		this.catalog = catalog;
		
		this.activeScript = catalog.templateScript(0); //TODO: what is the first template?
		this.activeScriptIdx = 0;
		
				
		//TODO: need these values from catalog?
		int integerCount=0;
		int longCount=0; 
		int charCount=0; 
        int singleCharLength=0; 
        int decimalCount=0; 
        int bytesCount=0; 
        
		DictionaryFactory dcr = new DictionaryFactory(integerCount, longCount, charCount, 
                								singleCharLength, decimalCount, bytesCount,
                								catalog.tokenLookup());
		
		readerDispatch = new FASTReaderDispatch(reader, dcr, catalog.templatesCount());
		
		
		
	}
	
    
	
	
	
	/**
	 * Read up to the end of the next sequence or message (eg. a repeating group)
	 * 
	 * Rules for making client compatible changes to templates.
	 * - Field can be demoted to more general common value before the group.
	 * - Field can be promoted to more specific value inside sequence
	 * - Field order inside group can change but can not cross sequence boundary.
	 * - Group boundaries can be added or removed.
	 * 
	 * Note nested sequence will stop once for each of the sequences therefore at the
	 * bottom hasMore may not have any new data but is only done as a notification that
	 * the loop has completed.
	 * 
	 * @return
	 */
	
	public int hasMore() {
		
		int fieldId = -1; //undefined for groups and commands.
		
		//when do we get templateId.
	//	catalog.templateScript(templateId);
		//must take this array and turn into two arrays of values.
		
	//	readerDispatch.dispatchReadByToken(fieldId, token);
		
		return -1;//return field id of the group just read
	}
	
//	public int hasField() {
//		
//		return -1;//return field id of field just read.
//	}

	public int readInt(int id) {
		return readerDispatch.lastInt(id);
	}

	public long readLong(int id) {
		return readerDispatch.lastLong(id);
	}

	public int readBytes(int id) {
		return readerDispatch.lastInt(id);
	}

	public int readDecimalExponent(int id) {
		return readerDispatch.lastInt(id);
	}

	public long readDecimalMantissa(int id) {
		return readerDispatch.lastLong(id);
	}
	
	public int readText(int id) {
		return readerDispatch.lastInt(id);
	}


}
