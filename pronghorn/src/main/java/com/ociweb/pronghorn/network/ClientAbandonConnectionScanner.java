package com.ociweb.pronghorn.network;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.stage.scheduling.ElapsedTimeRecorder;
import com.ociweb.pronghorn.util.ArrayGrow;
import com.ociweb.pronghorn.util.ServerObjectHolderVisitor;
import com.ociweb.pronghorn.util.ma.RunningStdDev;

public class ClientAbandonConnectionScanner extends ServerObjectHolderVisitor<ClientConnection> {

	private final static Logger logger = LoggerFactory.getLogger(ClientAbandonConnectionScanner.class);

	
	public static int stdDevsToAbandon = 5; //default which will close connections taking longer than expected
	public static long absoluteNSToAbandon = 300_000_000_000L;//default of 5 minutes, not normally used since standard dev is faster.
	public static long absoluteNSToKeep =        200_000_000L; //default of 200ms for any acceptable wait.
	
	private long maxOutstandingCallTime;
	private ClientConnection candidate;
	
	private int absoluteCounts = 0;
	private ClientConnection[] absoluteAbandons = new ClientConnection[4];

	private static final long CLOSED_LINGER_NS = 200_000L; //200 mirco seconds
	
	private RunningStdDev stdDev = new RunningStdDev();

	private final ClientCoordinator coordinator;
	
	public ClientAbandonConnectionScanner(ClientCoordinator coordinator) {
		this.coordinator = coordinator;
	}
	
	public void reset() {
		maxOutstandingCallTime = -1;
		candidate = null;
		absoluteCounts = 0;
		stdDev.clear();
		Arrays.fill(absoluteAbandons, null);
	}
		
	@Override
	public void visit(ClientConnection t) {
		
		long scanTime = System.nanoTime();
		long callTime = t.outstandingCallTime(scanTime);
		
		//skip those already notified.
		if (!t.isClientClosedNotificationSent()) {			
			if (t.isDisconnecting || !t.isValid) {
				if (callTime>CLOSED_LINGER_NS) {
					//DO NOT IMMEDIATLY SEND THESE OR WE MAY END UP SENDING THE SAME ONE MULTIPLE TIMES.
					absoluteAbandons = ArrayGrow.setIntoArray(absoluteAbandons, t, absoluteCounts++);
				}
			} else {
				
						
				long timeout = t.getTimeoutNS();
				if (timeout>0) {//overrides general behavior if set
					if (callTime>timeout) {
						absoluteAbandons = ArrayGrow.setIntoArray(absoluteAbandons, t, absoluteCounts++);
					}
				} else {
					if (callTime>absoluteNSToAbandon) {
						absoluteAbandons = ArrayGrow.setIntoArray(absoluteAbandons, t, absoluteCounts++);
					} else {
					
						//TODO: can, find the lest recently used connection and close it as well ??
			
						//if no explicit limits are set wait until we have 100 data samples before limiting
						if (ElapsedTimeRecorder.totalCount(t.histogram())>100) {
							//find the std dev of the 98% of all network calls
							RunningStdDev.sample(stdDev, ElapsedTimeRecorder.elapsedAtPercentile(t.histogram(), .98));
							
							//find the single longest outstanding call
							if (callTime > maxOutstandingCallTime) {
								maxOutstandingCallTime = callTime;
								candidate = t;
							}
						}
					}
				}	
			}
		}
	}

	public ClientConnection[] timedOutConnections() {
		return absoluteAbandons;		
	}
	
	public ClientConnection leadingCandidate() {

		if (null!=candidate && (RunningStdDev.sampleCount(stdDev)>1)) {			

			long limit = (long)((stdDevsToAbandon*RunningStdDev.stdDeviation(stdDev))+RunningStdDev.mean(stdDev));
						
//			boolean debug = false;
//			if (debug) {
//			Appendables.appendNearestTimeUnit(System.out.append("Candidate: "), maxOutstandingCallTime).append("\n");
//			Appendables.appendNearestTimeUnit(System.out.append("StdDev Limit: "), limit).append("\n");
//			Appendables.appendNearestTimeUnit(System.out.append("StdDev: "), (long)RunningStdDev.stdDeviation(stdDev) ).append("\n");
//			}
			
			if (maxOutstandingCallTime > absoluteNSToKeep) { //must be greater than the keep value or it is not a candidate.			
				if (maxOutstandingCallTime > Math.min(limit, absoluteNSToAbandon)) {
					
					//logger.info("\n{} waiting connection to {} has been assumed abandoned and is the leading candidate to be closed.",Appendables.appendNearestTimeUnit(new StringBuilder(), maxOutstandingCallTime),candidate);

					//this is the worst offender at this time
					return candidate;
				}
			}
		}
		return null;
	}

}
