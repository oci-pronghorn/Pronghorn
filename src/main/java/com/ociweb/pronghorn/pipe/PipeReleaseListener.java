package com.ociweb.pronghorn.pipe;

public interface PipeReleaseListener {
	
	public PipeReleaseListener NO_OP = new PipeReleaseListener() {
		@Override
		public void released(long position) {
		}
	};
	
	void released(long position);
	
}
