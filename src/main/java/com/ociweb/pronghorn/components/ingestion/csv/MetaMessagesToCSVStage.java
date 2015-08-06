package com.ociweb.pronghorn.components.ingestion.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MetaMessagesToCSVStage extends PronghornStage {

	private final RingBuffer inputRing;
	private final RingBuffer outputRing;
	private static final Logger log = LoggerFactory.getLogger(MetaMessagesToCSVStage.class);
	
	private int activeFieldIdx = 0;
	private int activeByteBase = 0;
	
	/*
	 * Consumes Meta messages and produces new XML templates catalog upon receiving the flush message
	 */
	public MetaMessagesToCSVStage(GraphManager gm, RingBuffer inputRing, RingBuffer outputRing) {
		super(gm,inputRing,outputRing);
		this.inputRing = inputRing;
		this.outputRing = outputRing;
				
		if (RingBuffer.from(inputRing) != MetaMessageDefs.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the MetaFieldFROM catalog of messages for input.");
		}
		
		if (RingBuffer.from(outputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
				
	}
	
	//NOTE: important debug technique to stop old data from getting over written, TODO: how can we formalize this debug technique to make it easer.
	//RingWalker.setReleaseBatchSize(inputRing, 100000 );
	
	@Override
	public void run() {
		//TODO: AAA, upgrade to non blocking
		
			collectUntilEndOfStream(this);  
		
	}

	public static void collectUntilEndOfStream(MetaMessagesToCSVStage stage) {
		

			while (RingReader.tryReadFragment(stage.inputRing)) {
	        	assert(RingReader.isNewMessage(stage.inputRing)) : "There are no multi fragment message found in the MetaFields";
	        	
	        	int msgLoc = RingReader.getMsgIdx(stage.inputRing);
	        	
	        	assert(msgLoc>=0) : "bad value of "+msgLoc+" inputRing "+stage.inputRing;
	        			
	        	
	        	String name = MetaMessageDefs.FROM.fieldNameScript[msgLoc];
	        	int templateId = (int)MetaMessageDefs.FROM.fieldIdScript[msgLoc];	        	
	        	int type = 0x3F & (templateId>>1); //also contains name because we masked with 111111	        	
	        	
	        	switch (type) {

	        		case 0: //UInt32	   
		        		{
		        			sometimesComma(stage);
		        			//NOTE: we use long here to ensure this value remains unsigned
		        			long uint = RingReader.readInt(stage.inputRing, MetaMessageDefs.UINT32_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;	
		        			
							RingBuffer.validateVarLength(outputRing, 21); //RingBuffer.bytesWorkingHeadPosition(ring)
		        			
							int max = 21 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertLongToASCII(outputRing, 0xFFFFFFFFl & uint, max);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing)));
		        		}	
						break;
	        		case 64: //UInt32 Named	   
		        		{
		        			sometimesComma(stage);
		        			long uint = RingReader.readInt(stage.inputRing, MetaMessageDefs.NAMEDUINT32_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
		        			RingBuffer.validateVarLength(outputRing, 21);
							int max = 21 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertLongToASCII(outputRing, 0xFFFFFFFFl&uint, max);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing)));
		        		}	
						break;
						
	        		case 1: //Int32	      
		        		{
		        			sometimesComma(stage);
							int sint = RingReader.readInt(stage.inputRing, MetaMessageDefs.INT32_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
							RingBuffer.validateVarLength(outputRing, 12);
							int max = 12 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertIntToASCII(outputRing, sint, max);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing)));
		        		}
		        		break;
	        		case 65: //Int32 Named      
		        		{
		        			sometimesComma(stage);
							int sint = RingReader.readInt(stage.inputRing, MetaMessageDefs.NAMEDINT32_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
							RingBuffer.validateVarLength(outputRing, 12);
							int max = 12 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertIntToASCII(outputRing, sint, max);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing)));
		        		}
		        		break;
		        		
	        		case 2: //UInt64
		        		{
		        			sometimesComma(stage);
							long ulong = RingReader.readLong(stage.inputRing, MetaMessageDefs.UINT64_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
							RingBuffer.validateVarLength(outputRing, 21);
							int max = 21 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertLongToASCII(outputRing, ulong, max); //TODO: find a way to deal with top signed bit on long if it happens
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing))); 
		        		}
		        		break;
	        		case 66: //UInt64 Named
		        		{
		        			sometimesComma(stage);
							long ulong = RingReader.readLong(stage.inputRing, MetaMessageDefs.NAMEDUINT64_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
							RingBuffer.validateVarLength(outputRing, 21);
							int max = 21 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertLongToASCII(outputRing, ulong, max);//TODO: find a way to deal with top signed bit on long if it happens
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing))); 
							
		        		}
		        		break;
	        				        		
	        		case 3: //Int64
		        		{
		        			sometimesComma(stage);
							long slong = RingReader.readLong(stage.inputRing, MetaMessageDefs.INT64_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
							RingBuffer.validateVarLength(outputRing, 21);
							int max = 21 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertLongToASCII(outputRing, slong, max);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing)));
							
		        		}
	        			break;
	        		case 67: //Int64 Named
		        		{
		        			sometimesComma(stage);
							long slong = RingReader.readLong(stage.inputRing, MetaMessageDefs.NAMEDINT64_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
							RingBuffer.validateVarLength(outputRing, 21);
							int max = 21 + RingBuffer.bytesWorkingHeadPosition(outputRing);
							int len = RingBuffer.leftConvertLongToASCII(outputRing, slong, max);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(len + RingBuffer.bytesWorkingHeadPosition(outputRing)));
		        		}
		        		break;      			
	        			
	        		case 4: //ASCII
		        		{
		        			sometimesComma(stage);
		        			int readBytesLength = RingReader.readBytesLength(stage.inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
		        			int readBytesPos    = RingReader.readBytesPosition(stage.inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
		        			byte[] backing      = RingReader.readBytesBackingArray(stage.inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
		        			
							RingBuffer.copyBytesFromToRing(backing,readBytesPos,stage.inputRing.byteMask,outputRing.unstructuredLayoutRingBuffer,RingBuffer.bytesWorkingHeadPosition(outputRing),outputRing.byteMask, readBytesLength);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(RingBuffer.bytesWorkingHeadPosition(outputRing) + readBytesLength));
    			
		        		}
		        		break;
	        		case 68: //ASCII Named
		        		{
		        			sometimesComma(stage);
		        			int readBytesLength = RingReader.readBytesLength(stage.inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
		        			int readBytesPos    = RingReader.readBytesPosition(stage.inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
		        			byte[] backing      = RingReader.readBytesBackingArray(stage.inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
							RingBuffer outputRing = stage.outputRing;
									        			
							RingBuffer.copyBytesFromToRing(backing,readBytesPos,stage.inputRing.byteMask,outputRing.unstructuredLayoutRingBuffer,RingBuffer.bytesWorkingHeadPosition(outputRing),outputRing.byteMask, readBytesLength);
							RingBuffer.setBytesWorkingHead(outputRing, RingBuffer.BYTES_WRAP_MASK&(RingBuffer.bytesWorkingHeadPosition(outputRing) + readBytesLength));

		        		}
	        			break;       			
	        			
	        		case 6: //Decimal
		        		{
		        			sometimesComma(stage);
		        			int readDecimalExponent = RingReader.readDecimalExponent(stage.inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
		        			long readDecimalMantissa = RingReader.readDecimalMantissa(stage.inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
		        			
		        			RingBuffer.addDecimalAsASCII(readDecimalExponent, readDecimalMantissa, stage.outputRing);

		        		}
	        	        break;
	        		case 70: //Decimal Named
		        		{
		        			sometimesComma(stage);
		        			int readDecimalExponent = RingReader.readDecimalExponent(stage.inputRing, MetaMessageDefs.NAMEDDECIMAL_VALUE_LOC);
		        			long readDecimalMantissa = RingReader.readDecimalMantissa(stage.inputRing, MetaMessageDefs.NAMEDDECIMAL_VALUE_LOC);

		        			RingBuffer.addDecimalAsASCII(readDecimalExponent, readDecimalMantissa, stage.outputRing);
		        			
		        		}
	        	        break;     	       
	        	        	        	       
	        	        
	        		case 16: //beginMessage 
	        			
	        			RingBuffer.blockWriteMessage(stage.outputRing, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM);        			
	        			stage.activeFieldIdx = 0;
	        			stage.activeByteBase = RingBuffer.bytesWorkingHeadPosition(stage.outputRing);
	        			break;
	        		case 80: //beginMessage Named
	        			RingBuffer.blockWriteMessage(stage.outputRing, FieldReferenceOffsetManager.LOC_CHUNKED_STREAM);	  //?? begin message named?
	        			stage.activeFieldIdx = 0;
	        			stage.activeByteBase = RingBuffer.bytesWorkingHeadPosition(stage.outputRing);
	        			break;
	        		case 17: //endMessage  		
	        			
	        			RingBuffer.copyASCIIToBytes("\n",stage.outputRing);	 //not very efficient may be better to make a single char writer method	         			
	        			
	        			//Total length for the full row row!!
	        			RingBuffer.validateVarLength(stage.outputRing, RingBuffer.bytesWorkingHeadPosition(stage.outputRing)-stage.activeByteBase);
	        			RingBuffer.addBytePosAndLen(stage.outputRing, stage.activeByteBase, RingBuffer.bytesWorkingHeadPosition(stage.outputRing)-stage.activeByteBase);
	        		        			
	        			RingBuffer.publishWrites(stage.outputRing);
	        				        			
	        			break;
	        		case 18: //Null
	        			sometimesComma(stage);
	        			break;
	        		case 82: //Null Named
	        			sometimesComma(stage);
	        			break;		        			
	        			
	        		case 31: //flush
	        		    
	        			RingBuffer.publishEOF(stage.outputRing); //TODO: B, up down stream stage still needs this, need to remove.
	        			RingBuffer.setReleaseBatchSize(stage.inputRing, 0);
	        			RingReader.releaseReadLock(stage.inputRing);

	        			stage.requestShutdown();
	        			return;
	        			
	        	    default:
	        	    	throw new UnsupportedOperationException("Missing case for:"+templateId+" "+name);
	        	
	        	}
	        	
	        	log.trace("Name:{} {} {}",name,msgLoc,templateId);
	        	
		 } 

	}


	private static void sometimesComma(MetaMessagesToCSVStage stage) {
		if (0 != stage.activeFieldIdx++) {
			RingBuffer.copyASCIIToBytes(",",stage.outputRing);  //not very efficient may be better to make a single char writer method
		}
	
	}



}
