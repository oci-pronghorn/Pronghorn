//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.util.Arrays;

import com.ociweb.jfast.error.FASTException;
import com.ociweb.jfast.field.OperatorMask;
import com.ociweb.jfast.field.TokenBuilder;
import com.ociweb.jfast.field.TypeMask;
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
	
	private int activeScriptTemplateMask;
	private long[] activeScript;
	private int activeScriptLength;
	private int activeScriptCursor;
	
	private final byte[] prefixData;
	private final byte prefixDataLength;
	
	private long messageCount = 0;
	
	//read groups field ids and build repeating lists of tokens.
	
	//only look up the most recent value read and return it to the caller.
	public FASTDynamicReader(PrimitiveReader reader, TemplateCatalog catalog) {
		this.catalog = catalog;
		this.prefixDataLength=catalog.getMessagePrefixSize();
		this.prefixData = new byte[prefixDataLength];
		this.activeScriptTemplateMask = -1; //no selected script				
		this.readerDispatch = new FASTReaderDispatch(reader, 
				                                catalog.dictionaryFactory(), 
				                                catalog.templatesCount(), 
				                                3, catalog.maxFieldId(),
				                                catalog.dictionaryMembers());
			
	}
	
	public long messageCount() {
		return messageCount;
	}
	
    public void reset() {
    	messageCount = 0;
    	this.activeScriptTemplateMask = -1; //no selected script
    	this.activeScriptCursor = -1;
    	this.readerDispatch.reset();
    }
	
    public String toBinary(byte[] input) {
    	StringBuilder builder = new StringBuilder();
    	for(byte b:input) {
    		builder.append(Integer.toBinaryString(0xFF&b)).append(",");
    	}
    	return builder.toString();
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

    
	public int hasMore(long[] target) {
		
		//System.err.println("hasMore call");
		int t = 0;
		
		do {
			if (activeScriptTemplateMask<0) {
				//start new script or detect that the end of the data has been reached
				if (readerDispatch.isEOF()) {
					return 0;
				}
				
				//get next token id then immediately start processing the script
				parseNextTokenId();
				
			} 
			
			//continue existing script
			long val = activeScript[activeScriptCursor];
			int token = (int)(val&0xFFFFFFFF);
			
			//jump to top if at end of sequence with count remaining
			if (readerDispatch.dispatchReadByToken((int)(val>>>32), token, target, t++)) {
					//jump back to top of this sequence in the script.
					//return this cursor position as the unique id for this sequence.
					return activeScriptTemplateMask|(activeScriptCursor -= (TokenBuilder.MAX_INSTANCE&token));
			}
							
			
		} while (++activeScriptCursor<activeScriptLength);
		
		//reached the end of the script so close and prep for the next one

		int result = activeScriptTemplateMask;
		activeScriptTemplateMask = -1;//find next template
		readerDispatch.closeMessage();
		return result;//returns the template mask for the end of this message
	
		
	}

	private void parseNextTokenId() {
		///read prefix bytes if any (only used by some implementations)
		if (prefixDataLength!=0) {
			readerDispatch.dispatchReadPrefix(prefixData);
			//System.err.println("read prefix:"+toBinary(prefixData));
		};
		///////////////////
		
		//open message (special type of group)			
		int templateId = readerDispatch.openMessage(catalog.maxTemplatePMapSize());
		if (templateId>=0) {
			activeScriptTemplateMask = templateId<<TokenBuilder.MAX_FIELD_ID_BITS; //for id returned to caller
			messageCount++;
		}
					
		//set the script and cursor for this template			
		activeScript = catalog.templateScript(templateId);
		activeScriptLength = activeScript.length;
		activeScriptCursor = 0;
	}
	
	public byte[] prefix() {
		return prefixData;
	}
	
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
