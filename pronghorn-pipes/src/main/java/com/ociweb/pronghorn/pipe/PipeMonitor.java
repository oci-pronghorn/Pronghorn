package com.ociweb.pronghorn.pipe;

public class PipeMonitor {

	private static Pipe[] targetPipes = new Pipe[0];
	
	public static synchronized <S extends MessageSchema<S>> Pipe<S> addMonitor(Pipe<S> sourcePipe) {
		//can only have 1 monitoring pipe, that pipe however could feed a replicator stage
		if (isMonitored(sourcePipe.id)) {
			throw new UnsupportedOperationException("This pipe is already configured to be monitored "+sourcePipe);
		}
		/////////////
		if (targetPipes.length <= sourcePipe.id) {
		   Pipe[] newTargetPipes = new Pipe[sourcePipe.id+1];
		   System.arraycopy(targetPipes, 0, 
				            newTargetPipes, 0, targetPipes.length);
		   targetPipes = newTargetPipes;
		}
		
		//this monitor pipe is the same size as the original
		//NOTE we could make this bigger or smaller in the future as needed ...
		Pipe pipe = new Pipe(sourcePipe.config());
		targetPipes[sourcePipe.id] = pipe;
		pipe.initBuffers();
		return pipe;
	}
	
	
	
	public static <S extends MessageSchema<S>> boolean monitor(Pipe<S> sourcePipe,
			                                      long sourceSlabPos, int sourceBlobPos) {
		
		if (isMonitored(sourcePipe.id)) {
			Pipe<S> localTarget = targetPipes[sourcePipe.id];
			
			//will report errors if the logger does not keep this pipe clear 
			//but it also blocks to ensure nothing is ever lost
			Pipe.presumeRoomForWrite(localTarget);			
			
			Pipe.copyFragment(sourcePipe, sourceSlabPos, sourceBlobPos, localTarget);
						
		}
		
		return true;
	}



	public static <S extends MessageSchema<S>> boolean isMonitored(Pipe<S> p) {
		return isMonitored(p.id);
	}
	
	private static boolean isMonitored(int id) {
		return id>=0 && id<targetPipes.length && null!=targetPipes[id];
	}

}
