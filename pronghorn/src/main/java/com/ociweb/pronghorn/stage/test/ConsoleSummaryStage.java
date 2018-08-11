package com.ociweb.pronghorn.stage.test;

import java.io.IOException;
import java.util.Arrays;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.MessageSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

/**
 * For some Schema T keeps running totals of each message type.
 * Periodically reports the Number of each message type to target appendable.
 * Default target is system.out.
 * @param <T>
 *
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ConsoleSummaryStage<T extends MessageSchema<T>> extends PronghornStage {

	private final Pipe<T> input;
	private final StringBuilder console = new StringBuilder(512);
	
	private final long[] totalCounts;
	private final long[] counts;
	private long totalBytes;
	
	private final long stepTime = 2000;//2 sec
	private long nextOutTime = System.currentTimeMillis()+stepTime;
	private long startTime;
	private Appendable target;
	
	//TODO: AA, need validation stage to confirm values are in range and text is not too long

	/**
	 *
	 * @param gm
	 * @param input _in_ Any schema input pipe that should be summarized.
	 */
	public ConsoleSummaryStage(GraphManager gm, Pipe<T> input) {
	    this(gm, input, System.out);
	}

	public ConsoleSummaryStage(GraphManager gm, Pipe<T> input, Appendable target) {
		super(gm, input, NONE);
		this.input = input;

		FieldReferenceOffsetManager from = Pipe.from(input);		
		this.totalCounts = new long[from.tokensLen];
		this.counts = new long[from.tokensLen];
		this.target = target;
		
		GraphManager.addNota(gm, GraphManager.DOT_BACKGROUND, "cornsilk2", this);
        
	}


	public static <T extends MessageSchema<T>> ConsoleSummaryStage<T> newInstance(GraphManager gm, Pipe<T> input) {
		return new ConsoleSummaryStage(gm, input);		
	}
	
	@Override
	public void startup() {
	    startTime = System.currentTimeMillis();
	}
	
	@Override
	public void shutdown() {
		try {
            processCounts("Final:",counts,totalCounts);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
		long duration = System.currentTimeMillis()-startTime;
		processTotal("Totals:", totalCounts, Pipe.from(input), duration);
	}

	@Override
	public void run() {
	    try {
    		boolean foundData = dataToRead(counts);
    		long now = System.currentTimeMillis();
    		if (foundData || now>nextOutTime) {
    			nextOutTime = now+stepTime;			
                    if (!processCounts("Running:",counts,totalCounts)) {
                    	return;
                    } else {
                    	//System.out.println("should have data but process counts reports none");
                    }
    		} else {
    			if (!foundData) {
    			//	System.out.println("no data to report found, counts length "+counts.length+"  "+Arrays.toString(counts));
    			}
    		}

	    } catch (IOException e) {
	        throw new RuntimeException(e);            
	   }

	}
	
	public long totalBytes() {
	    return totalBytes;
	}
	
	public long totalMessages() {
		long sum = 0;
		int i = totalCounts.length;
		while (--i>=0) {
			sum += totalCounts[i];
		}
		return sum;
	}

	private boolean processCounts(String label, long[] counts,	long[] totalCounts) throws IOException {
		
		console.setLength(0);
		return processCountsLoop(label, counts, totalCounts, 0, 0, 0, counts.length);
	}

    private boolean processCountsLoop(String label, long[] counts, long[] totalCounts, int i, long newMessages, long totalMessages, int limit) throws IOException {
        assert(limit>=1) : "If we are here there should be some counts to report.";
    	
    	while (i<limit) {
			newMessages += counts[i];
			writeToConsole(counts, totalCounts, i);
			totalMessages += totalCounts[i];
			i++;
		}
		return cleanupReport(label, newMessages, totalMessages);
    }

    private boolean cleanupReport(String label, long newMessages, long totalMessages) throws IOException {
        if (newMessages>0) {
			Appendables.appendValue(console.append(" total:"), totalMessages);
			
			target.append(label);			
			target.append(console).append("\n");
			
		}
		return newMessages>0;
    }

    private void writeToConsole(long[] counts, long[] totalCounts, int i) throws IOException {
        if (counts[i]>0) {
        	totalCounts[i] += counts[i];
        	Appendables.appendValue(Appendables.appendValue(console.append('['), i).append(']'), counts[i]).append(' ');
        	counts[i]=0;
        }
    }

	private boolean processTotal(String label, long[] totalCounts, FieldReferenceOffsetManager from, long duration) {
		try {
    		console.setLength(0);
    		int i = 0;
    		long totalMsg = 0;
    		while (i<totalCounts.length) {
    			totalMsg += totalCounts[i];
    			if (totalCounts[i]>0) {
    				console.append('[').append(i).append(']');
    				Appendables.appendValue(console, totalCounts[i]);
    				if (null!=from.fieldNameScript) {
    					if (null!=from.fieldNameScript[i]) {
    						console.append(" Name:").append(from.fieldNameScript[i]);
    					}
    					console.append(" Id:");
    					Appendables.appendValue(console, from.fieldIdScript[i]);					
    				}
    				console.append("\n");
    			}
    			i++;
    		}
    		
    		target.append(label).append("\n");
    		target.append(console).append("\n");
    		
    		Appendables.appendValue(target,"Total Messages:",totalMsg,"\n");    		
    		Appendables.appendValue(target,"Total Bytes:",totalBytes," (slab and blob)\n");
    		Appendables.appendValue(target,"Total Duration:",duration," ms\n");       
    		    		
    		if (0!=totalMsg) {
        		Appendables.appendValue(target,"Avg msg size:",totalBytes/totalMsg,"\n");
        		long msgPerMs = totalMsg/duration;
        		Appendables.appendValue(target, "MsgPerMs:",msgPerMs);
    		}
    		
    		long bitsPerMs = (8*totalBytes)/(duration*1000);
    		Appendables.appendValue(target, " MBitsPerSec:",bitsPerMs,"\n");
    		
    		return totalMsg>0;
		} catch (IOException e) {
		    throw new RuntimeException(e);
		}
	}
	
	private boolean dataToRead(long[] counts) {
		
		int msgIdx = 0;
		boolean data = false;
		
		while (PipeReader.tryReadFragment(input)) {
			if (PipeReader.isNewMessage(input)) {
				msgIdx = PipeReader.getMsgIdx(input);
				if (msgIdx<0) {
				    PipeReader.releaseReadLock(input);
				    requestShutdown();
					break;
				} else {
					counts[msgIdx]++;
					data = true;
				}
			}
			totalBytes += (PipeReader.sizeOfFragment(input)*4) + PipeReader.bytesConsumedByFragment(input);
		
			//logger.info("reading new input total bytes :"+totalBytes);
			
			PipeReader.releaseReadLock(input);
			

		}	
		return data;
	}

}
