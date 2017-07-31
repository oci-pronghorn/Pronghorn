package com.ociweb.pronghorn.network.mqtt;

import static com.ociweb.pronghorn.pipe.Pipe.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.network.schema.MQTTIdRangeControllerSchema;
import com.ociweb.pronghorn.network.schema.MQTTIdRangeSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Generate ranges of packet Ids for use in distributed communication. Once communications
 * are complete the Id is returned so it can be used again later for a different transaction.
 * 
 * Ids are short values. They are stored in a single int with the low position holding the start
 * and the high value holding the exclusive stop.
 * 
 * @author Nathan Tippy
 *
 */
public class IdGenStage extends PronghornStage {

	private static final int MAX_VALUES = 1<<15;// should be this 65536 but there is a (short) somewhere !!

	private static final Logger logger = LoggerFactory.getLogger(IdGenStage.class);
	
	public static final int MAX_BLOCK_SIZE = 1<<13; //8K
	private static final int MAX_CONSUMED_BLOCKS = 1<<10; //1K
	private static final int MAX_CONSUMED_BLOCKS_LIMIT = MAX_CONSUMED_BLOCKS-1;
	private static final int STOP_CODE = 0xFFFFFFFF;
	
	public static final int IS_REMOTE_BIT = 1<<17;//needs to be above the low 16
	
	private int[] consumedRanges;
	private int totalRanges = 0;
	private boolean isActive = false;//do not begin until we get the signal
	
	private final Pipe<MQTTIdRangeControllerSchema> control;
	private final Pipe<MQTTIdRangeSchema>[] inputs;
	private final Pipe<MQTTIdRangeSchema>[] outputs;	
	private final int sizeOfFragment;
	private static final int theOneMsg = 0;// there is only 1 message supported by this stage
		
	public IdGenStage(GraphManager graphManager, Pipe<MQTTIdRangeSchema> input, Pipe<MQTTIdRangeControllerSchema> control, Pipe<MQTTIdRangeSchema> output) {
		super(graphManager, join(input, control), output);
		this.inputs = new Pipe[]{input};
		this.outputs = new Pipe[]{output};
		this.control = control;
		assert(Pipe.from(input).equals(Pipe.from(output))) : "Both must have same message types ";	
		this.sizeOfFragment = Pipe.from(input).fragDataSize[theOneMsg];
				
		//must be set so this stage will get shut down and ignore the fact that is has un-consumed messages coming in 
        GraphManager.addNota(graphManager,GraphManager.PRODUCER, GraphManager.PRODUCER, this);
                
	}
	
	public IdGenStage(GraphManager graphManager, Pipe<MQTTIdRangeSchema>[] inputs, Pipe<MQTTIdRangeControllerSchema> control, Pipe<MQTTIdRangeSchema>[] outputs) {
		super(graphManager, join(inputs,control), outputs);
		this.inputs = inputs;
		this.outputs = outputs;
		this.control = control;
		//TODO: add assert to confirm that all the inputs and outputs have the same eq from.
		this.sizeOfFragment = Pipe.from(inputs[0]).fragDataSize[theOneMsg];
	}

	@Override
	public void startup() {
		consumedRanges = new int[MAX_CONSUMED_BLOCKS];
		totalRanges = 0;
		isActive = false;
	}

	@Override
	public void run() {
		
		while (PipeReader.tryReadFragment(control)) {
		    int msgIdx = PipeReader.getMsgIdx(control);
		    switch(msgIdx) {
		        case MQTTIdRangeControllerSchema.MSG_CLEARALL_2:
		        	isActive = false;
		        	totalRanges = 0; //wipe out the consumed ranges.
				break;
		        case MQTTIdRangeControllerSchema.MSG_IDRANGE_1:
		        	int fieldRange = PipeReader.readInt(control,MQTTIdRangeControllerSchema.MSG_IDRANGE_1_FIELD_RANGE_100);
		        	
		        	if (totalRanges==0) {
		        		//simple case
		        		consumedRanges[0] = fieldRange;
		        		totalRanges++;		        		
		        	} else {
		        		
		        		int idx = findIndex(consumedRanges, 0, totalRanges, fieldRange);
		        		//this row is >= the beginning of the target.
		        		
		        		int rangeEnd = rangeEnd(fieldRange);
		        		
		        		if (rangeEnd == rangeBegin(consumedRanges[idx])) {
		        			//grow the front case
		        			consumedRanges[idx] = buildRange(rangeBegin(fieldRange), rangeEnd(consumedRanges[idx]));
		        		} else {
		        			if (idx>0 && rangeEnd(consumedRanges[idx-1])==rangeBegin(fieldRange) ) {
		        				//grow the last end case
		        				consumedRanges[idx-1] = buildRange(rangeBegin(consumedRanges[idx]),rangeEnd);		        				
		        			} else {
		        				//insert between case
		        				if (totalRanges+1>MAX_CONSUMED_BLOCKS) {
		        					throw new RuntimeException("saved consumed ranges are more numerious than the previous limit? Was the code changed while data was saved to disk?");	  					
		        				}
		        				
		        				int insertAt = idx;
		        				System.arraycopy(consumedRanges, insertAt, consumedRanges, insertAt+1, totalRanges - insertAt);
		        				consumedRanges[idx] = fieldRange;
		        			}
		        		}
		        	}
		    
		        break;
		        case MQTTIdRangeControllerSchema.MSG_READY_3:
		        	isActive = true;
				break;
		        case -1:
		           requestShutdown();
		        break;
		    }
		    PipeReader.releaseReadLock(control);
		}
		
		
		if (isActive) {
			generateIds();
		}
	
	}

	private void generateIds() {
		//pull in all the released ranges first so we have them to give back out again.
		int i = inputs.length;
		while (--i>=0) {
			Pipe inputRing = inputs[i];
			while (hasContentToRead(inputRing, sizeOfFragment) && 
				   totalRanges<MAX_CONSUMED_BLOCKS_LIMIT) // only release blocks if we have room, just in case one splits, back-pressure 
			{
				int msgIdx = Pipe.takeMsgIdx(inputRing);
				assert(theOneMsg == msgIdx);
				
				releaseRange(Pipe.takeInt(inputRing));
				
				Pipe.releaseReadLock(inputRing);
				Pipe.confirmLowLevelRead(inputRing, sizeOfFragment);
			}
			
			if (totalRanges == MAX_CONSUMED_BLOCKS_LIMIT) {
				logger.warn("A large number {} of PacketIds have not been returned. Must wait for return values.", totalRanges);
			}
		}
		
		//push out all the new ranges for use as long as we have values and the queues have room
		int j = outputs.length;
		while (--j>=0) {
			Pipe outputRing = outputs[j];
			while (hasRoomForWrite(outputRing, sizeOfFragment) ) {
			    				
				int range = reserveNewRange();
				if (STOP_CODE == range || 0==range) {
					return;//no more values to give out
				} 
				
				boolean showNewBlocks = false;
				if (showNewBlocks) {
					logger.info("IdGen new range reserved {}->{} for {}",rangeBegin(range),rangeEnd(range),Integer.toHexString(range));
				}
				
				addMsgIdx(outputRing, theOneMsg);	
				Pipe.addIntValue(range, outputRing);
				publishWrites(outputRing);	
				Pipe.confirmLowLevelWrite(outputRing, sizeOfFragment);
			}
		}
	}

	public static int rangeEnd(int range) {
		return (0xFFFF&(range>>16));
	}
	
    public static int rangeBegin(int range) {
		return (0xFFFF&range);
	}

	public static int buildRange(final int rangeStart, final int rangeEnd) {
		return (0xFFFF&rangeStart) | (rangeEnd<<16);
	}

	public void releaseRange(final int range) {
		
		int insertAt = findIndex(consumedRanges, 0, totalRanges, range);		
				
		if (range != consumedRanges[insertAt]) {
			final int releaseEnd = rangeEnd(range);
			final int releaseBegin = rangeBegin(range);
			if (releaseBegin>=releaseEnd) {
				//this is an invalid message and treated as a no-op
				//this must always be done to support generative testing
				//TODO: B, if desired this can be logged
				logger.debug("skipped release for {}->{}",releaseBegin,releaseEnd);
				return;
			}
			
			//boolean showOutstandingRanges = true;
//			if (totalRanges>8) {
//				logger.warn("many issued packet Ids remain not returned");
//				showRanges();
//				//Note the requested range may already be partially released, caution
//				logger.info("now release range {}->{}  ranges {} ",releaseBegin,releaseEnd, totalRanges);
//			}
			
		//	try {
				
			// if begin is < last cosumedRange end then its in the middle of that row need special logic
			if (insertAt>0) {
				int lastRangeEnd = rangeEnd(consumedRanges[insertAt-1]);
				int lastRangeBegin = rangeBegin(consumedRanges[insertAt-1]);
				
				if (releaseBegin <  lastRangeEnd  ) {
													
					if (releaseEnd < lastRangeEnd) {
						
						//if (showOutstandingRanges) { System.err.println("a");};
						
						//cuts range in two parts, will need to add row to table
						consumedRanges[insertAt-1] = buildRange(lastRangeBegin,releaseBegin);//    //(0xFFFF&consumedRanges[insertAt-1]) | ((0xFFFF&releaseBegin)<<16);
						System.arraycopy(consumedRanges, insertAt, consumedRanges, insertAt+1, totalRanges - insertAt);	
						consumedRanges[insertAt] = buildRange(releaseEnd,lastRangeEnd);// (0xFFFF&(releaseEnd)) | (lastRangeEnd<<16);
						totalRanges++;
						assert(validateInternalTable()) : "Added at "+insertAt + " from release "+releaseBegin+"->"+releaseEnd;
						return; // do not continue

					} else {
						//if (showOutstandingRanges) { System.err.println("b");};
						
						consumedRanges[insertAt-1] = buildRange(lastRangeBegin, releaseBegin);
						//end is >= to last end so this is a tail trim to the existing row							
						if (releaseEnd == lastRangeEnd) {
							assert(validateInternalTable());
							return; //no need to continue
						}
					}					
				}
			}
			
			///
			
			//count up how many full rows must be cleared, result is in rows
			int rows = 0;
			if (insertAt<totalRanges) {
				//if (showOutstandingRanges) { System.err.println("c");};
				
				int rowBegin =0;
				do {
					rowBegin = 0xFFFF&consumedRanges[++rows+insertAt];				
				} while(releaseEnd>rowBegin && (rows+insertAt<totalRanges));
			}
			
			//this last row may be split and needs adjustment			
			int lastRow = rows+insertAt-1;		
			if (totalRanges>0) {
				int lastEnd = rangeEnd(consumedRanges[lastRow]);//(0xFFFF&(consumedRanges[lastRow]>>16));
				if (releaseEnd<lastEnd) {
					//if (showOutstandingRanges) { System.err.println("d");};
					if (releaseEnd>rangeBegin(consumedRanges[lastRow])) {
						//this is a partial row so modify rather than clear
						consumedRanges[lastRow] = (0xFFFF&releaseEnd) | (lastEnd<<16);						
					}
					rows--;//this row is done
				} else {
					//if (showOutstandingRanges) { System.err.println("e");};
					
					//releaseEnd is > this row end
					//confirm its < next row begin
					assert(lastRow+1>=totalRanges || releaseEnd<=(consumedRanges[lastRow+1]&0xFFFF));
										
				}
			}
			
			//all full rows are deleted
			if (rows>0) {
				//if (showOutstandingRanges) { System.err.println("f");};
				
				assert(isInRange(insertAt, rows, releaseBegin, releaseEnd));
				int toCopy = -rows + totalRanges - insertAt;					
				System.arraycopy(consumedRanges, insertAt+rows, consumedRanges, insertAt, toCopy);			
				totalRanges-=rows;
			}

//			} finally {
//				if (showOutstandingRanges) {
//					showRanges();
//				}
//			}
			
			
		} else {
			
			logger.debug("IdGen release range exact match {}->{}",rangeBegin(range),rangeEnd(range));
			//exact match so just delete this row
			int toCopy = -1 + totalRanges - insertAt;
			if (toCopy>0) {
				System.arraycopy(consumedRanges, insertAt+1, consumedRanges, insertAt, toCopy);		
			}
			totalRanges--;
		}
		assert(validateInternalTable());
	}
 	
	
	
	private boolean isInRange(int insertAt, int rows, int releaseBegin, int releaseEnd) {
		int i = rows;
		while (--i>=0) {
			int range = consumedRanges[insertAt+i];
			int begin = rangeBegin(range);
			int end =  rangeEnd(range);
			if (begin<releaseBegin || end<releaseBegin) {
				return false;
			}
			if (end>releaseEnd || begin> releaseEnd) {
				return false;
			}
		}
		return true;
	}

	private static int findIndex(int[] consumedRanges, int from, int to, int target) {
		int i = from;
		int targetStart = rangeBegin(target);
		//while target start is before each range start keep walking
		while (i<to &&  rangeBegin(consumedRanges[i]) < targetStart) {
			i++;
		}
		return i;
	}

	private void showRanges() {
		int i = 0;
		//while target start is before each range start keep walking
		while (i<totalRanges) {
			
			int range = consumedRanges[i];			
			logger.info("consumed range: {}->{}",rangeBegin(range),rangeEnd(range));
									
			i++;
		}
	}
	
	/**
	 * Find the biggest range then reserves and returns a block no bigger than MAX_BLOCK_SIZE
	 * 
	 * @return
	 */
	private int reserveNewRange() {
		//if there are no selected ranges then just add the first one
		if (0 == totalRanges) {
			totalRanges = 1;
			return consumedRanges[0]= 0 | ((MAX_BLOCK_SIZE)<<16);
		}
		
		//if have existing ranges so append onto the end of one of the existing ones.
		
		//all ranges are kept in order 		
		int max = 0;
		int maxStart = 0;
		int idx = 0;
		
		//walk list of all reserved ranges and find the biggest found after each
		int lastBegin = MAX_VALUES-2; //need room to use FULL MASK as the exclude end
		int i = totalRanges;
		while (--i>=0) {
			int j = consumedRanges[i];
			int begin = rangeBegin(j);
			int end   = (j>>16)&0xFFFF;
			
			int len = lastBegin-end;
			
			if (len>max) {
				max = len;
				maxStart = end;
				idx=i+1;
				if (max >= MAX_BLOCK_SIZE) {
     				return reserveRange(maxStart, idx, MAX_BLOCK_SIZE);
				}
			}			
			lastBegin = begin;
		}
		
		//check that we have not run out of values
		if (0==max) {//return stop code
			return STOP_CODE;//try again after some ranges are released
		}		
		
		//we now know where the largest range is and where it starts.		
		return reserveRange(maxStart, idx, Math.min(max, MAX_BLOCK_SIZE));
	}

	private void dumpInternalTable() {
		int j = totalRanges;
		while (--j>=0) {
			System.err.println(j+" reserved "+rangeBegin(consumedRanges[j])+"->"+rangeEnd(consumedRanges[j]));
		}
	}

	private int reserveRange(final int rangeStart, int idx, final int rangeCount) {
		assert(idx>0);
		//appends on to previous internal row
		
		int rangeEnd = rangeStart+rangeCount;
	    if (idx<totalRanges && rangeBegin(consumedRanges[idx])==rangeEnd) {
	    	//combine these two because reservation connects them together	    	
	    	rangeEnd = (consumedRanges[idx]>>16)&0xFFFF;
	    	System.arraycopy(consumedRanges, idx+1, consumedRanges, idx, totalRanges-idx);
	    	totalRanges--;
	    }
		
		consumedRanges[idx-1]= buildRange(consumedRanges[idx-1],rangeEnd);
		
		//send back value that is just the new range
		
		int newReservation = buildRange(rangeStart, rangeStart+rangeCount);
	
		logger.debug("IdGen reserve range {}->{}",rangeStart,rangeEnd);
				
		assert(validateInternalTable());
		return newReservation;
		
	}


	private boolean validateInternalTable() {
		int last = MAX_VALUES;
		int j = totalRanges;
		while (--j>=0) {
			int start = rangeBegin(consumedRanges[j]);
			if (start>last) {
				dumpInternalTable();				
				return false;
			} else {
				last = start;
			}			
		}
		return true;
	}	

}
