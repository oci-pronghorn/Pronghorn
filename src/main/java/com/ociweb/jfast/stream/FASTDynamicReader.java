//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import java.util.Arrays;

import com.ociweb.jfast.error.FASTException;
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
	
	int activeScriptTemplateId;
	long[] activeScript;
	int activeScriptCursor;
	
	byte[] prefixData;
	
	//read groups field ids and build repeating lists of tokens.
	
	//only look up the most recent value read and return it to the caller.
	public FASTDynamicReader(PrimitiveReader reader, TemplateCatalog catalog) {
		this.catalog = catalog;
		this.prefixData = new byte[catalog.getMessagePrefixSize()];
		this.activeScriptTemplateId = -1; //no selected script				
		this.readerDispatch = new FASTReaderDispatch(reader, 
				                                catalog.dictionaryFactory(), 
				                                catalog.templatesCount(), 
				                                3, catalog.maxFieldId());
			
	}
	
	int count = 0;
    public void reset() {
    	count = 0;
    	this.activeScriptTemplateId = -1; //no selected script
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
	
	public int hasMore() {
		
		//System.err.println("has more call");
		
		do {
			if (activeScriptTemplateId<0) {
				//start new script or detect that the end of the data has been reached
				if (readerDispatch.isEOF()) {
					return 0;
				}
				
				///read prefix bytes if any (only used by some implementations)
				if (prefixData.length>0) {
					readerDispatch.dispatchReadPrefix(prefixData);
					//System.err.println("read prefix:"+toBinary(prefixData));
				};
				///////////////////
				
				//open message (special type of group)			
				int templateId = readerDispatch.openMessage(catalog.maxTemplatePMapSize());
				if (templateId>=0) {
					activeScriptTemplateId = templateId;
					count++;
				
				} else {
					//System.err.println("messagesRead:"+count);
					//TODO: hack to stop test at this point.***********************************
					return 0;
					
//					throw new FASTException("Unimplemented case when template is not found in message header");
				}
							
				//set the script and cursor for this template			
				activeScript = catalog.templateScript(activeScriptTemplateId);
				activeScriptCursor = 0;
				
			} else {
				//continue existing script
				long val = activeScript[activeScriptCursor++];
				int fieldId = (int)(val>>>32);
				int token = (int)(val&0xFFFFFFFF);
				
				//TODO: if fieldId is zero then it should be a group!!!!
	//			System.err.println(activeScriptTemplateId+" active cursor:"+activeScriptCursor+" id:"+fieldId+
	//					           " "+TokenBuilder.tokenToString(token));
				
				//group templates open close //no need to build token because it is in catalog
				//group sequence open close  //steps in script inside group.
				//group group open close     //steps in script inside group.
				
				
				//TODO: dispatch two if token was a sequence? needed to fetch the length?
				//TODO: but length could be anywhere in the data so what now??
				
				
				if (!readerDispatch.dispatchReadByToken(fieldId, token)) {
					//hit end of sequence because false is returned (TODO: refactor to not include message)
					
					if (!readerDispatch.completeSequence(token)) {
						//return back to top of this sequence.
						activeScriptCursor -= (TokenBuilder.MAX_INSTANCE&token)+1;
					}
					
					int level = 1; //TODO: change to ordinal position in template script.
					return (activeScriptTemplateId<<10)|level; //TODO: hack test for now need to formalize shift
	
				}
				
//				//System.err.println("type:"+TokenBuilder.extractType(token)+" "+TypeMask.toString(TokenBuilder.extractType(token)));
//				if ((TokenBuilder.extractType(token)|1)==TypeMask.TextASCIIOptional) {
//					System.err.println("text<"+readerDispatch.textHeap().get(readText(fieldId), new StringBuilder())+">");
//				}
//				if ((TokenBuilder.extractType(token)|1)==TypeMask.IntegerUnsignedOptional) {
//					System.err.println("int<"+readInt(fieldId)+">");
//				}
//				if ((TokenBuilder.extractType(token)|1)==TypeMask.LongUnsignedOptional) {
//					System.err.println("long<"+readLong(fieldId)+">");
//				}
//				if (TokenBuilder.extractType(token)==TypeMask.GroupLength) {
//					System.err.println("seqLen<"+readInt(fieldId)+">");
//				}
				
				
				//reached the end of the script so close and prep for the next one
				if (activeScriptCursor>=activeScript.length) {
				
					readerDispatch.closeMessage();
					int result = activeScriptTemplateId<<10;
					activeScriptTemplateId = -1;//find next template
					
					//TODO need to loop script back.
					return result;//TODO: not sure what to return but must let caller process data so far
				}			
			}
		} while (true);
		
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
