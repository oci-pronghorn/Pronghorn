package com.ociweb.pronghorn.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.stage.scheduling.ElapsedTimeRecorder;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ServerObjectHolderVisitor;
import com.ociweb.pronghorn.util.ma.RunningStdDev;

public class ClientCoordinatorAbandonScanner extends ServerObjectHolderVisitor<ClientConnection> {

	private final static Logger logger = LoggerFactory.getLogger(ClientCoordinatorAbandonScanner.class);
	
	private long scanTime;
	private long maxOutstandingCallTime;
	private ClientConnection candidate;
	private RunningStdDev stdDev = new RunningStdDev();
	
	
	public void reset() {
		scanTime = System.nanoTime();
		maxOutstandingCallTime = -1;
		candidate = null;
		stdDev.clear();
	}
		
	@Override
	public void visit(ClientConnection t) {
		
		//find the single longest outstanding call
		long callTime = t.outstandingCallTime(scanTime);

		if (callTime > maxOutstandingCallTime) {
			maxOutstandingCallTime = callTime;
			candidate = t;
		}
		
		//TODO: can, find the lest recently used connection and close it as well.
		
		if (ElapsedTimeRecorder.totalCount(t.histogram())>1) {
			//find the std dev of the 98% of all network calls
			RunningStdDev.sample(stdDev, ElapsedTimeRecorder.elapsedAtPercentile(t.histogram(), .98));
		}
	}
	
	StringBuilder workspace = new StringBuilder();
	
	public ClientConnection leadingCandidate() {
		
		///turn back on after we see that the server is stable
		//server is not clearing calls 
		
		if (null!=candidate && (RunningStdDev.sampleCount(stdDev)>1)) {			
			int stdDevs = 4;
			long limit = (long)((stdDevs*RunningStdDev.stdDeviation(stdDev))+RunningStdDev.mean(stdDev));
			if (maxOutstandingCallTime > limit) {
				workspace.setLength(0);
				logger.info("{} waiting connection to {} has been assumed abandonded and now marked as closed.",Appendables.appendNearestTimeUnit(workspace, maxOutstandingCallTime),candidate);
				
				//this is the worst offender at this time
				return candidate;
			}
		}
		return null;
	}

}
