//Copyright 2013, Nathan Tippy
//See LICENSE file for BSD license details.
//Send support requests to http://www.ociweb.com/contact
package com.ociweb.jfast.stream;

import com.ociweb.jfast.field.TokenBuilder;
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
	
	private int activeScriptCursor;
	private int activeScriptLimit;
	
	private final int[] fullScript;
	
	private final byte[] preambleData;
	private final byte preambleDataLength;
	
	private long messageCount = 0;
	
	//the smaller the better to make it fit inside the cache.
	private final FASTRingBuffer ringBuffer;
	
	//read groups field ids and build repeating lists of tokens.
	
	//only look up the most recent value read and return it to the caller.
	public FASTDynamicReader(PrimitiveReader reader, TemplateCatalog catalog, FASTRingBuffer ringBuffer, FASTReaderDispatch dispatch) {
		this.catalog = catalog;
		this.preambleDataLength=catalog.getMessagePrefixSize();
		this.preambleData = new byte[preambleDataLength];
		this.activeScriptTemplateMask = -1; //no selected script				
		this.readerDispatch = dispatch;
		this.fullScript = catalog.fullScript();
		this.ringBuffer = ringBuffer;
		 
	}
	
	public long messageCount() {
		return messageCount;
	}
	
    public void reset(boolean clearData) {
    	this.messageCount = 0;
    	this.activeScriptTemplateMask = -1; //no selected script
    	this.activeScriptCursor = -1;
    	if (clearData) {
    		this.readerDispatch.reset();
    	}
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
    int neededSpaceOrTemplate = -1;
    int lastCapacity = 0;
    
	public int hasMore() {
		

		if (neededSpaceOrTemplate<0) { 
			//start new script or detect that the end of the data has been reached

			//checking EOF first before checking for blocked queue
			if (readerDispatch.isEOF()) {
				return 0;
			}				
			
			//must have room to store the new template
			
			if (lastCapacity<1) {
				lastCapacity = ringBuffer.availableCapacity();
				if (lastCapacity<1) {
					return 0x80000000;
				}
			}
			lastCapacity-=1;
			
			//get next token id then immediately start processing the script
			int templateId = parseNextTokenId();
			
			ringBuffer.append(templateId);//write template id at the beginning of this message
							
			//set the cursor start and stop for this template				
			activeScriptCursor = catalog.getTemplateStartIdx(templateId); 
			activeScriptLimit = catalog.getTemplateLimitIdx(templateId);
							
	    	//Worst case scenario is that this is full of decimals which each need 3.
	    	//but for easy math we will use 4, will require a little more empty space in buffer		    	
			//however we will not need a lookup table 
			neededSpaceOrTemplate = (activeScriptLimit-activeScriptCursor)<<2;
			assert(neededSpaceOrTemplate>0) : "Script must have positive value";// zero is used for unknown template
		} 
		
		if (lastCapacity<neededSpaceOrTemplate) {
			lastCapacity = ringBuffer.availableCapacity();
			if (lastCapacity<neededSpaceOrTemplate) {
				return 0x80000000;
			}
		}
		lastCapacity-=neededSpaceOrTemplate;
				
		
		do {
			if (readerDispatch.dispatchReadByToken(fullScript[activeScriptCursor], ringBuffer)) {
				    ringBuffer.moveForward();
				    if (readerDispatch.isSkippedSequence()) {
				    	//jumping over sequence
				    	System.err.println("has now been tested, please delete");
						activeScriptCursor += (TokenBuilder.MAX_INSTANCE&fullScript[++activeScriptCursor]);
				    	if (activeScriptCursor==activeScriptLimit) {
				    		neededSpaceOrTemplate = -1;
				    		readerDispatch.closeMessage();
				    		return 2;//finished reading full message but we have no sequence
				    	}
				    } else {
					    if (!readerDispatch.isFinishedSequence()) {
					    	int seqScriptLength = TokenBuilder.MAX_INSTANCE&fullScript[activeScriptCursor];
					    	//Worst case scenario is that this is full of decimals which each need 3.
					    	//but for easy math we will use 4, will require a little more empty space in buffer
					    	//however we will not need a lookup table 
					    	neededSpaceOrTemplate = seqScriptLength<<2;
					    	
					    	//jump back to top of this sequence in the script.
							activeScriptCursor -= seqScriptLength;
					    } else {
					    	//finished sequence, no need to jump
					    	if (++activeScriptCursor==activeScriptLimit) {
					    		neededSpaceOrTemplate = -1;
					    		readerDispatch.closeMessage();
					    		return 3;//finished reading full message and the sequence
					    	}
					    }
					    return 1;//has sequence group to read
				    }
			}
		} while (++activeScriptCursor<activeScriptLimit);

		
		//reached the end of the script so close and prep for the next one
		neededSpaceOrTemplate = -1;
		readerDispatch.closeMessage();
		ringBuffer.moveForward();
		return 2;//finished reading full message
	}

	private int parseNextTokenId() {
		///read prefix bytes if any (only used by some implementations)
		if (preambleDataLength!=0) {
			readerDispatch.dispatchReadPrefix(preambleData);
		};
		///////////////////
		
		//open message (special type of group)			
		int templateId = readerDispatch.openMessage(catalog.maxTemplatePMapSize());
		if (templateId>=0) {
			activeScriptTemplateMask = templateId<<TokenBuilder.MAX_FIELD_ID_BITS; //for id returned to caller
			messageCount++;
		}
					
		return templateId;

	}
	
	public byte[] prefix() {
		return preambleData;
	}
	
	public FASTRingBuffer ringBuffer() {
		return ringBuffer;
	}


}
