package com.ociweb.pronghorn.components.ingestion.csv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.components.ingestion.metaMessageUtil.MetaMessageDefs;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

/**
 * _no-docs_
 * Consumes meta messages and produces new XML templates catalog upon receiving a flush message.
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class MetaMessagesToCSVStage extends PronghornStage {

	private final Pipe inputRing;
	private final Pipe outputRing;
	private static final Logger log = LoggerFactory.getLogger(MetaMessagesToCSVStage.class);
	
	private int activeFieldIdx = 0;
	private int activeByteBase = 0;

	/**
	 *
	 * @param gm graph this stage will be part of
	 * @param inputRing _in_ Pipe containing meta messages
	 * @param outputRing _out_ Pipe containing newly produced XML templates
	 */
	public MetaMessagesToCSVStage(GraphManager gm, Pipe inputRing, Pipe outputRing) {
		super(gm,inputRing,outputRing);
		this.inputRing = inputRing;
		this.outputRing = outputRing;
				
		if (Pipe.from(inputRing) != MetaMessageDefs.FROM) {
			throw new UnsupportedOperationException("This class can only be used with the MetaFieldFROM catalog of messages for input.");
		}
		
		if (Pipe.from(outputRing) != RawDataSchema.FROM) {
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
		

			while (PipeReader.tryReadFragment(stage.inputRing)) {
	        	assert(PipeReader.isNewMessage(stage.inputRing)) : "There are no multi fragment message found in the MetaFields";
	        	
	        	int msgLoc = PipeReader.getMsgIdx(stage.inputRing);
	        	
	        	assert(msgLoc>=0) : "bad value of "+msgLoc+" inputRing "+stage.inputRing;
	        			
	        	
	        	String name = MetaMessageDefs.FROM.fieldNameScript[msgLoc];
	        	int templateId = (int)MetaMessageDefs.FROM.fieldIdScript[msgLoc];	        	
	        	int type = 0x3F & (templateId>>1); //also contains name because we masked with 111111	        	
	        	
	        //	System.err.println("type "+type);
	        	
	        	switch (type) {

	        		case 0: //UInt32	   
		        		{
		        			sometimesComma(stage);
		        			//NOTE: we use long here to ensure this value remains unsigned
		        			long uint = PipeReader.readInt(stage.inputRing, MetaMessageDefs.UINT32_VALUE_LOC);
							Pipe outputRing = stage.outputRing;	
		        			
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, uint);
							DataOutputBlobWriter.closeLowLevelField(out);		        		}	
						break;
	        		case 64: //UInt32 Named	   
		        		{
		        			sometimesComma(stage);
		        			long uint = PipeReader.readInt(stage.inputRing, MetaMessageDefs.NAMEDUINT32_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
							
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, uint);
							DataOutputBlobWriter.closeLowLevelField(out);		        		}	
						break;
						
	        		case 1: //Int32	      
		        		{
		        		
		        			sometimesComma(stage);
							int sint = PipeReader.readInt(stage.inputRing, MetaMessageDefs.INT32_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
							
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, sint);
							DataOutputBlobWriter.closeLowLevelField(out);
			
		        		}
		        		break;
	        		case 65: //Int32 Named      
		        		{
		        			sometimesComma(stage);
							int sint = PipeReader.readInt(stage.inputRing, MetaMessageDefs.NAMEDINT32_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
							
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, sint);
							DataOutputBlobWriter.closeLowLevelField(out);
			      		}
		        		break;
		        		
	        		case 2: //UInt64
		        		{
		        			sometimesComma(stage);
							long ulong = PipeReader.readLong(stage.inputRing, MetaMessageDefs.UINT64_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
							
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, ulong);
							DataOutputBlobWriter.closeLowLevelField(out);
						}
		        		break;
	        		case 66: //UInt64 Named
		        		{
		        			sometimesComma(stage);
							long ulong = PipeReader.readLong(stage.inputRing, MetaMessageDefs.NAMEDUINT64_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, ulong);
							DataOutputBlobWriter.closeLowLevelField(out);
		        		}
		        		break;
	        				        		
	        		case 3: //Int64
		        		{
		        			sometimesComma(stage);
							long slong = PipeReader.readLong(stage.inputRing, MetaMessageDefs.INT64_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, slong);
							DataOutputBlobWriter.closeLowLevelField(out);
		        		}
	        			break;
	        		case 67: //Int64 Named
		        		{
		        			sometimesComma(stage);
							long slong = PipeReader.readLong(stage.inputRing, MetaMessageDefs.NAMEDINT64_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
							DataOutputBlobWriter out = Pipe.openOutputStream(outputRing);
							Appendables.appendValue(out, slong);
							DataOutputBlobWriter.closeLowLevelField(out);
		        		}
		        		break;      			
	        			
	        		case 4: //ASCII
		        		{
		        			sometimesComma(stage);
		        			int readBytesLength = PipeReader.readBytesLength(stage.inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
		        			int readBytesPos    = PipeReader.readBytesPosition(stage.inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
		        			byte[] backing      = PipeReader.readBytesBackingArray(stage.inputRing, MetaMessageDefs.ASCII_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
		        			
						//	System.err.println("data:"+new String(backing, readBytesPos, readBytesLength));
							
							Pipe.copyBytesFromToRing(backing,readBytesPos,stage.inputRing.blobMask,outputRing.blobRing,Pipe.getWorkingBlobHeadPosition((Pipe<?>) outputRing),outputRing.blobMask, readBytesLength);
							Pipe.setBlobWorkingHead(outputRing, Pipe.BYTES_WRAP_MASK&(Pipe.getWorkingBlobHeadPosition((Pipe<?>) outputRing) + readBytesLength));
    			
							
							
		        		}
		        		break;
	        		case 68: //ASCII Named
		        		{
		        			sometimesComma(stage);
		        			int readBytesLength = PipeReader.readBytesLength(stage.inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
		        			int readBytesPos    = PipeReader.readBytesPosition(stage.inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
		        			byte[] backing      = PipeReader.readBytesBackingArray(stage.inputRing, MetaMessageDefs.NAMEDASCII_VALUE_LOC);
							Pipe outputRing = stage.outputRing;
									        			
							Pipe.copyBytesFromToRing(backing,readBytesPos,stage.inputRing.blobMask,outputRing.blobRing,Pipe.getWorkingBlobHeadPosition((Pipe<?>) outputRing),outputRing.blobMask, readBytesLength);
							Pipe.setBlobWorkingHead(outputRing, Pipe.BYTES_WRAP_MASK&(Pipe.getWorkingBlobHeadPosition((Pipe<?>) outputRing) + readBytesLength));

		        		}
	        			break;       			
	        			
	        		case 6: //Decimal
		        		{
		        			sometimesComma(stage);
		        			int readDecimalExponent = PipeReader.readDecimalExponent(stage.inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
		        			long readDecimalMantissa = PipeReader.readDecimalMantissa(stage.inputRing, MetaMessageDefs.DECIMAL_VALUE_LOC);
		        			
		        			Pipe.addDecimalAsASCII(readDecimalExponent, readDecimalMantissa, stage.outputRing);

		        		}
	        	        break;
	        		case 70: //Decimal Named
		        		{
		        			sometimesComma(stage);
		        			int readDecimalExponent = PipeReader.readDecimalExponent(stage.inputRing, MetaMessageDefs.NAMEDDECIMAL_VALUE_LOC);
		        			long readDecimalMantissa = PipeReader.readDecimalMantissa(stage.inputRing, MetaMessageDefs.NAMEDDECIMAL_VALUE_LOC);

		        			Pipe.addDecimalAsASCII(readDecimalExponent, readDecimalMantissa, stage.outputRing);
		        			
		        		}
	        	        break;     	       
	        	        	        	       
	        	        
	        		case 16:
					Pipe pipe = stage.outputRing;
					int msgIdx = RawDataSchema.MSG_CHUNKEDSTREAM_1; //beginMessage 
	        			
	        			//before write make sure the tail is moved ahead so we have room to write
					while (!Pipe.hasRoomForWrite(pipe, Pipe.from(pipe).fragDataSize[msgIdx])) {
					    Pipe.spinWork(pipe);
					}
					Pipe.addMsgIdx(pipe, msgIdx);        			
	        			stage.activeFieldIdx = 0;
	        			stage.activeByteBase = Pipe.getWorkingBlobHeadPosition((Pipe<?>) stage.outputRing);
	        			break;
	        		case 80:
					Pipe pipe1 = stage.outputRing;
					int msgIdx1 = RawDataSchema.MSG_CHUNKEDSTREAM_1; //beginMessage Named
	        			//before write make sure the tail is moved ahead so we have room to write
					while (!Pipe.hasRoomForWrite(pipe1, Pipe.from(pipe1).fragDataSize[msgIdx1])) {
					    Pipe.spinWork(pipe1);
					}
					Pipe.addMsgIdx(pipe1, msgIdx1);	  //?? begin message named?
	        			stage.activeFieldIdx = 0;
	        			stage.activeByteBase = Pipe.getWorkingBlobHeadPosition((Pipe<?>) stage.outputRing);
	        			break;
	        		case 17: //endMessage  		
	        			
	        			Pipe.copyASCIIToBytes("\n",stage.outputRing);	 //not very efficient may be better to make a single char writer method	         			
	        			
	        			//Total length for the full row row!!
	        			Pipe.validateVarLength(stage.outputRing, Pipe.getWorkingBlobHeadPosition((Pipe<?>) stage.outputRing)-stage.activeByteBase);
	        			Pipe.addBytePosAndLen(stage.outputRing, stage.activeByteBase, Pipe.getWorkingBlobHeadPosition((Pipe<?>) stage.outputRing)-stage.activeByteBase);
	        		        			
	        			Pipe.publishWrites(stage.outputRing);
	        				        			
	        			break;
	        		case 18: //Null
	        			sometimesComma(stage);
	        			break;
	        		case 82: //Null Named
	        			sometimesComma(stage);
	        			break;		        			
	        			
	        		case 31: //flush
	        		    
	        			Pipe.publishEOF(stage.outputRing); //TODO: B, up down stream stage still needs this, need to remove.

	        			stage.requestShutdown();
	        			return;
	        			
	        	    default:
	        	    	throw new UnsupportedOperationException("Missing case for:"+templateId+" "+name);
	        	
	        	}
	        	
	        	log.trace("Name:{} {} {}",name,msgLoc,templateId);

	        	Pipe.setReleaseBatchSize(stage.inputRing, 0);
	        	PipeReader.releaseReadLock(stage.inputRing);
		 } 

	}


	private static void sometimesComma(MetaMessagesToCSVStage stage) {
		if (0 != stage.activeFieldIdx++) {
			Pipe.copyASCIIToBytes(",",stage.outputRing);  //not very efficient may be better to make a single char writer method
		}
	
	}



}
