package com.ociweb.pronghorn.stage;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class ConsoleStage extends PronghornStage {

	private final RingBuffer inputRing;
	private final StringBuilder console = new StringBuilder();
	
	private final long[] totalCounts;
	private final long[] counts;
	
	private long stepTime = 2000;//2 sec
	private long nextOutTime = System.currentTimeMillis()+stepTime;
		
	public ConsoleStage(GraphManager gm, RingBuffer inputRing) {
		this(gm,inputRing,-1);
	}
	
	public ConsoleStage(GraphManager gm, RingBuffer inputRing, int pillId) {
		super(gm, inputRing, NONE);
		this.inputRing = inputRing;

		FieldReferenceOffsetManager from = RingBuffer.from(inputRing);		
		totalCounts = new long[from.tokensLen];
		counts = new long[from.tokensLen];
	}

	@Override
	public void shutdown() {
		processCounts("Final:",counts,totalCounts);
		processTotal("Totals:",totalCounts, RingBuffer.from(inputRing));
	}

	@Override
	public void run() {
		boolean foundData = dataToRead(counts);
		long now = System.currentTimeMillis();
		if (foundData || now>nextOutTime) {
			nextOutTime = now+stepTime;			
			if (!processCounts("Running:",counts,totalCounts)) {
				return;
			}
		}


	}
	
	public long totalMessages() {
		long sum = 0;
		int i = totalCounts.length;
		while (--i>=0) {
			sum += totalCounts[i];
		}
		return sum;
	}

	private boolean processCounts(String label, long[] counts,	long[] totalCounts) {
		
		console.setLength(0);
		int i = 0;
		long newMessages = 0;
		long totalMessages = 0;
		while (i<counts.length) {
			newMessages += counts[i];
			if (counts[i]>0) {
				totalCounts[i] += counts[i];
				console.append('[').append(i).append(']').append(counts[i]).append(" ");
				counts[i]=0;
			}
			totalMessages += totalCounts[i];
			i++;
		}
		if (newMessages>0) {
			console.append(" total:").append(totalMessages);
			System.out.println(label+console);
		}
		return newMessages>0;
	}

	private boolean processTotal(String label, long[] totalCounts, FieldReferenceOffsetManager from) {
		
		console.setLength(0);
		int i = 0;
		long totalMsg = 0;
		while (i<totalCounts.length) {
			totalMsg += totalCounts[i];
			if (totalCounts[i]>0) {
				console.append('[').append(i).append(']').append(totalCounts[i]);
				if (null!=from.fieldNameScript) {
					if (null!=from.fieldNameScript[i]) {
						console.append(" Name:").append(from.fieldNameScript[i]);
					}
					console.append(" Id:").append(from.fieldIdScript[i]);					
				}
				console.append("\n ");
			}
			i++;
		}
		System.out.println(label);
		System.out.println(console);
		System.out.println("Total:"+totalMsg);
		return totalMsg>0;
	}
	
	private boolean dataToRead(long[] counts) {
		
		int msgIdx = 0;
		boolean data = false;
		
		while (RingReader.tryReadFragment(inputRing)) {
			if (RingReader.isNewMessage(inputRing)) {
				msgIdx = RingReader.getMsgIdx(inputRing);
				if (msgIdx<0) {
					break;
				} else {
					counts[msgIdx]++;
					data = true;
				}
			}
		}		
		return data;
	}
}
