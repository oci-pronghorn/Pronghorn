package com.ociweb.pronghorn.stage.scheduling;

import com.ociweb.pronghorn.pipe.Pipe;

public class DynamicDisableSubGraph {

	private int activeItem;
	private final long[]   foundToBeUnusedLastModified; //in ns
	private final long[][] foundToBeUnusedBaselines;// head values
	private final Pipe[][] foundToBeUnused; //if remains then mark this subGraph disabled	
	
	//check all these on every pass if any have data take them off the disbled subGraph
	private int disabledCount;
	private final int[] disabledIndexes;//index to which row is the disabled item.
	private final Pipe[][] foundToHaveData; //if has data we must un-disable this sequence
		
	private final long thresholdForDisableNS = 1_000_000_000; //1 full second of non use
	private SubGraphDisableable disabler;
	
	public DynamicDisableSubGraph(int countOfSubGraphs, SubGraphDisableable disableable) {
			
		foundToBeUnusedLastModified = new long[countOfSubGraphs];
		foundToBeUnusedBaselines = new long[countOfSubGraphs][];
		foundToBeUnused = new Pipe[countOfSubGraphs][];
		
		disabledIndexes = new int[countOfSubGraphs];
		foundToHaveData = new Pipe[countOfSubGraphs][];
		disabler = disableable;
	}
	
	//each one must be added before we can call process unit;
	public void addSubGraph(int idx, SubGraphDisableable disableable, Pipe[] inputPipes, Pipe[] allPipes) {
		
		foundToHaveData[idx] = inputPipes;
		
		foundToBeUnusedLastModified[idx] = System.nanoTime();
		
		int i = allPipes.length;
		long[] baseLines = new long[i];
		while (--i>=0) {
			//we want to know if anything gets added to the head in the future
			baseLines[i] = Pipe.workingHeadPosition(allPipes[i]);
		}
		foundToBeUnusedBaselines[idx] = baseLines;		
		foundToBeUnused[idx] = allPipes; //NOTE: can assert that inputPipes are included in this list...
			
		disabler = disableable;
				
	}
	
	//call this periodicly but not so fast as to take up lots of cycles.
	public void processUnit() {
		
		///////////////
		//un-disable those which now have data
		//scan all the disabled subgraphs to catch this sooner
		//////////////
		
		int d = disabledCount;
		while (--d>=0) {
			int i = disabledIndexes[d];
			if (i>=0) {
				if (foundToHaveData(foundToHaveData[i])) {
					//un-disable the object
					
					//call method on the external subGraph to change its state
					disabler.disable(i,false);
					
					//remove from list since it is now disabled
					disabledIndexes[d] = -1;
					
					//shrink list of possible
					if (d+1==disabledCount) {
						disabledCount--;
					}				
				}
			} else {
				if (d+1==disabledCount) {
					disabledCount--;
				}
			}
		}
		
		//////////////
		//scan just the next for unused 
		//update the last modified value OR 
		//if unmodified and above time limit, then mark disabled
		/////////////
		
		if (--activeItem<0) {
			activeItem = foundToBeUnusedLastModified.length-1;
		}
		
		long[] baseLines = foundToBeUnusedBaselines[activeItem];		
		Pipe[] outputs = foundToBeUnused[activeItem];
		boolean considerForDisable = true;
		int i = outputs.length;
		while (--i>=0) {
			long newHead = Pipe.workingHeadPosition(outputs[i]);
			if (newHead != baseLines[i]) {
				baseLines[i] = newHead;
				//this can not be considered since work was done.
				considerForDisable = false;
			}
		}
		if (considerForDisable) {
			//if not already disabled and over time
			if ( (!disabler.disabled(activeItem))
				&& (System.nanoTime()-foundToBeUnusedLastModified[activeItem])>thresholdForDisableNS 
				) {
							
				//add to list				
				disabledIndexes[disabledCount++] = activeItem;
							
				//mark disabled since it has not been used in over the theshold time
				disabler.disable(activeItem,true);
			}		
			
		} else {
			foundToBeUnusedLastModified[activeItem] = System.nanoTime();
		}
	}

	private boolean foundToHaveData(Pipe[] pipes) {
		int i = pipes.length;
		while (--i>=0) {
			if (Pipe.contentRemaining(pipes[i])>0) {
				return true;
			}
		}
		return false;
	}
	
}
