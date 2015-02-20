package com.ociweb.pronghorn.ring.stage;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;

public class ConsoleStage implements Runnable {

	private final RingBuffer inputRing;
	private final StringBuilder console = new StringBuilder();
	private final int posionPillMessageId;
	
	
	public ConsoleStage(RingBuffer inputRing) {
		this.inputRing = inputRing;
		posionPillMessageId = -1;
	}
	
	public ConsoleStage(RingBuffer inputRing, int pillId) {
		this.inputRing = inputRing;
		posionPillMessageId = pillId;
	}

	@Override
	public void run() {
			
		FieldReferenceOffsetManager from = RingBuffer.from(inputRing);
		RingReader.setReleaseBatchSize(inputRing, 4);
		
		long[] totalCounts = new long[from.tokensLen];
		long[] counts = new long[from.tokensLen];
		
		try {
			while (dataToRead(counts)) {
				if (!processCounts("Running:",counts,totalCounts)) {
					//no new data so slow down.
					Thread.sleep(200);
				}
				Thread.yield();
			}
			processCounts("Final:",counts,totalCounts);
			processTotal("Totals:",totalCounts, from);
			
		} catch (Throwable t) {
			t.printStackTrace();
			RingBuffer.shutdown(inputRing);
		}
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
		
		while (RingReader.tryReadFragment(inputRing)) {
			if (RingReader.isNewMessage(inputRing)) {
				msgIdx = RingReader.getMsgIdx(inputRing);
				if (msgIdx<0) {
					break;
				} else {
					counts[msgIdx]++;
				}
			}
		}
		return msgIdx!=posionPillMessageId;
	}
}
