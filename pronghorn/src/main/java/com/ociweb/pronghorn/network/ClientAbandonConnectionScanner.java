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
	public static long absoluteNSToAbandon = 600_000_000_000L;//default of 10 minutes, not normally used since standard dev is faster.
	public static long absoluteNSToKeep =        200_000_000; //default of 200ms for any acceptable wait.
	
	private long scanTime;
	private long maxOutstandingCallTime;
	private ClientConnection candidate;
	
	private int absoluteTimeoutCounts = 0;
	private ClientConnection[] absoluteTimeouts = new ClientConnection[4];
	
	private RunningStdDev stdDev = new RunningStdDev();

	private final ClientCoordinator coordinator;
	
	public ClientAbandonConnectionScanner(ClientCoordinator coordinator) {
		this.coordinator = coordinator;
	}
	
	public void reset() {
		scanTime = System.nanoTime();
		maxOutstandingCallTime = -1;
		candidate = null;
		absoluteTimeoutCounts = 0;
		stdDev.clear();
		Arrays.fill(absoluteTimeouts, null);
	}
		
	@Override
	public void visit(ClientConnection t) {
		long callTime = t.outstandingCallTime(scanTime);
				
		long timeout = t.getTimeoutNS();
		if (timeout>0) {//overrides general behavior if set
			if (callTime>timeout) {
				absoluteTimeouts = ArrayGrow.setIntoArray(absoluteTimeouts, t, absoluteTimeoutCounts++);
			}
		} else {
			//find the single longest outstanding call
			if (callTime > maxOutstandingCallTime) {
				maxOutstandingCallTime = callTime;
				candidate = t;
			}
			
			//TODO: can, find the lest recently used connection and close it as well ??
			
			if (ElapsedTimeRecorder.totalCount(t.histogram())>1) {
				//find the std dev of the 98% of all network calls
				RunningStdDev.sample(stdDev, ElapsedTimeRecorder.elapsedAtPercentile(t.histogram(), .98));
			}
		}		
	}

	public ClientConnection[] timedOutConnections() {
		return absoluteTimeouts;		
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
			
			if (maxOutstandingCallTime > Math.min(Math.max(limit,absoluteNSToKeep),absoluteNSToAbandon)) {
				//logger.info("\n{} waiting connection to {} has been assumed abandoned and is the leading candidate to be closed.",Appendables.appendNearestTimeUnit(workspace, maxOutstandingCallTime),candidate);
				
				//this is the worst offender at this time
				return candidate;
			}
		}
		return null;
	}

}
