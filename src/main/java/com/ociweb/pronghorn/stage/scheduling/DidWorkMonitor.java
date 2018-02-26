package com.ociweb.pronghorn.stage.scheduling;

import com.ociweb.pronghorn.pipe.PipePublishListener;
import com.ociweb.pronghorn.pipe.PipeReleaseListener;

public class DidWorkMonitor implements PipePublishListener, PipeReleaseListener {

	private boolean didWork;
	
	public static boolean didWork(DidWorkMonitor that) {
		return that.didWork;
	}
	
	public static void clear(DidWorkMonitor that) {
		that.didWork = false;
	}
	
	@Override
	public void released(long position) {
		didWork = true;
		
	}

	@Override
	public void published(long position) {
		didWork = true;
	}

}
