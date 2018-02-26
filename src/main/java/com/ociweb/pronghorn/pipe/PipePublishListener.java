package com.ociweb.pronghorn.pipe;

public interface PipePublishListener {

	public PipePublishListener NO_OP = new PipePublishListener(){
		@Override
		public void published(long position) {
		}
	};
	
	void published(long position);
	
}
