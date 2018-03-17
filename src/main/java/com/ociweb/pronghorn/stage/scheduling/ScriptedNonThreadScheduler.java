package com.ociweb.pronghorn.stage.scheduling;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.util.Appendables;
import com.ociweb.pronghorn.util.ma.RunningStdDev;
import com.ociweb.pronghorn.util.math.PMath;
import com.ociweb.pronghorn.util.math.ScriptedSchedule;

public class ScriptedNonThreadScheduler extends StageScheduler implements Runnable {

    public static Appendable debugStageOrder = null; //turn on to investigate performance issues.
	
    private static final int NS_OPERATOR_FLOOR = 1000; //1 micro seconds
	private AtomicBoolean shutdownRequested = new AtomicBoolean(false);;
    private long[] rates;
    private long[] lastRun;
    public PronghornStage[] stages;

    private DidWorkMonitor didWorkMonitor;
    
    private static AtomicInteger threadGroupIdGen = new AtomicInteger();
    
    private long maxRate;
    private static final Logger logger = LoggerFactory.getLogger(ScriptedNonThreadScheduler.class);

    private long nextRun = 0; //keeps times of the last pass so we need not check again

    private volatile Throwable firstException;//will remain null if nothing is wrong

    //Time based events will poll at least this many times over the period.
    // + ensures that the time trigger happens "near" the edge
    // + ensures that this non-thread scheduler in unit tests can capture the time delayed events.
    public static final int granularityMultiplier = 4;
    private static final long MS_TO_NS = 1_000_000;

    //when false this uses low granularity timer, this will optimize volume not latency
	public boolean lowLatencyEnforced = true;

    private int[] producersIdx;

    private Pipe[] producerInputPipes;
    private long[] producerInputPipeHeads;

    private Pipe[] inputPipes;
    private long[] inputPipeHeads;
    public final boolean reverseOrder;

    private AtomicInteger isRunning = new AtomicInteger(0);
    private final GraphManager graphManager;
    private String name = "";
    private PronghornStage lastRunStage = null;
  
    private static final boolean debugNonReturningStages = false;

    private ScriptedSchedule schedule = null;
    private int[] enabled; //used for turning off subGraph sequences when not in use. has jumps for jump over
    private int[] sequenceLookup; //schedule

    private boolean recordTime;    
    
    private long nextLongRunningCheck;
    private final long longRunningCheckFreqNS = 60_000_000_000L;//1 min
    private final StageVisitor checksForLongRuns;
    
    
    private byte[] stateArray;
    
    public int indexOfStage(PronghornStage stage) {
    	int i = stages.length;
    	while (--i>=0) {
    		if (stage == stages[i]) {
    			return i;
    		}
    	}
		return -1;
	}
    
    private void buildSchedule(GraphManager graphManager, 
    		                   PronghornStage[] stages, 
    		                   boolean reverseOrder) {

        this.stages = stages;
    	this.stateArray = GraphManager.stageStateArray(graphManager);    	
    	this.recordTime = GraphManager.isTelemetryEnabled(graphManager);
    	
    	final int groupId = threadGroupIdGen.incrementAndGet();
    	
    	if (recordTime) {
    		didWorkMonitor = new DidWorkMonitor();
    	}
    	
    	if (null==stages) {
    		schedule = new ScriptedSchedule(0, new int[0], 0);
    		//skipScript = new int[0];
    		return;
    	}
    	        
		PronghornStage pronghornStage = stages[stages.length-1];
		name = pronghornStage.stageId+":"+pronghornStage.getClass().getSimpleName()+"...";

        // Pre-allocate rates based on number of stages.
    	final int defaultValue = 2_000_000;
        long rates[] = new long[stages.length];

        int k = stages.length;
        while (--k>=0) {
        	
        	//set thread name
        	if (groupId>=0) {
        		GraphManager.recordThreadGroup(stages[k], groupId, graphManager);
        	}
        	
        	//add monitoring to each pipe
        	if (recordTime) {  
        		PronghornStage.addWorkMonitor(stages[k], didWorkMonitor);
        		GraphManager.setPublishListener(graphManager, stages[k], didWorkMonitor);
        		GraphManager.setReleaseListener(graphManager, stages[k], didWorkMonitor);
        		
        	}
        	
        	// Determine rates for each stage.
			long scheduleRate = Long.valueOf(String.valueOf(GraphManager.getNota(graphManager, stages[k], GraphManager.SCHEDULE_RATE, defaultValue)));
            rates[k] = scheduleRate;
        }

        // Build the script.
        schedule = PMath.buildScriptedSchedule(rates, reverseOrder);

        boolean newBlock = true;
        int scriptLength = schedule.script.length;
        
        sequenceLookup = new int[scriptLength];
        enabled = new int[scriptLength];//zeros are not used
                
        int lastBlockIdx = -1;
        int blockRun = 0;
        int blockId = 0;
        for(int i = 0; i<scriptLength; i++) {
        	
        	int value = schedule.script[i];
     
        	if (newBlock) {
        		//record this position as a new block
        		sequenceLookup[blockId] = i; //allows lookup to the right index for enable/disable call and is disbled
        		lastBlockIdx = i;
        		blockRun = 0;
        		     		
        		//TODO: need all the pipes associated with this sequence
        		//TODO: need all the pipes going into this sequence;
        		
        		
        		
        		
        		blockId++;
        	}
        	blockRun++;
        	
        	if (-1==value) { //end of run
        	    //write the skip size but negative to enable all blocks for now.
        		enabled[lastBlockIdx] = (0-blockRun);
        		
        		newBlock = true;
        	} else {
        		newBlock = false;
        	}
        	    	
        	
        }
        
             

        if (null != debugStageOrder) {	
        	try {
	        	debugStageOrder.append("----------full stages -------------Clock:");
	        	Appendables.appendValue(debugStageOrder, schedule.commonClock);
	        	if (null!=graphManager.name) {
	        		debugStageOrder.append(" ").append(graphManager.name);
	        	}
	        	
	        	debugStageOrder.append("\n");
	        	
		        for(int i = 0; i<stages.length; i++) {
		        	
		        	debugStageOrder.append("   ");
		        	debugStageOrder.append(i+" full stages "+stages[i].getClass().getSimpleName()+":"+stages[i].stageId);
		        	debugStageOrder.append("  inputs:");
		    		GraphManager.appendInputs(graphManager, debugStageOrder, stages[i]);
		    		debugStageOrder.append(" outputs:");
		    		GraphManager.appendOutputs(graphManager, debugStageOrder, stages[i]);
		    		debugStageOrder.append("\n");
		        	
		        }
		        
        	} catch (IOException e) {
        		throw new RuntimeException(e);
        	}
        }
        
        nextLongRunningCheck = System.nanoTime()+(longRunningCheckFreqNS/6);//first check is quicker.
        
        //System.err.println("commonClock:"+schedule.commonClock);
        
    }

    
    public void detectHangingThread(long now, long timeoutNS) {
    	if (null != didWorkMonitor) {
    		if (didWorkMonitor.isOverTimeout(now, timeoutNS)) {
    			didWorkMonitor.interrupt();
    		}
    	}	
    }
    
    
    //TODO: rename this, its not so much about low latency as it is near real time clock?
    //NOTE: this can be toggled at runtime as needed.
    public void setLowLatencyEnforced(boolean value) {
    	lowLatencyEnforced = value;
    }
    
    public ScriptedNonThreadScheduler(GraphManager graphManager,
    								  StageVisitor checksForLongRuns,
    		                          boolean reverseOrder) {
        super(graphManager);
        this.graphManager = graphManager;
        this.checksForLongRuns = checksForLongRuns;
        this.reverseOrder = reverseOrder;
        
        PronghornStage[] temp = null;

	    PronghornStage[][] orderedStages = ScriptedFixedThreadsScheduler.buildStageGroups(graphManager, 1, true);
	   
	    int i = orderedStages.length;
	    while (--i>=0) {
	    	if (null != orderedStages[i]) {
	    		
	    		if (null == temp) {
	    			temp = orderedStages[i];
	    		} else {
	    			logger.trace("warning had to roll up, check the hard limit on threads");
	    			
	    			//roll up any stages
	    			PronghornStage[] additional = orderedStages[i];
	    			PronghornStage[] newList = new PronghornStage[temp.length+additional.length];
	    			
	    			System.arraycopy(temp, 0, newList, 0, temp.length);
	    			System.arraycopy(additional, 0, newList, temp.length, additional.length);
	    			
	    			temp = newList;
	    			
	    		}
	    	} 
	    }
	    
        buildSchedule(graphManager, temp, reverseOrder);
    }

    public ScriptedNonThreadScheduler(GraphManager graphManager, boolean reverseOrder, PronghornStage[] stages) {
    	this(graphManager, reverseOrder, null, stages);
    }
        
    
    public ScriptedNonThreadScheduler(GraphManager graphManager, boolean reverseOrder, StageVisitor checksForLongRuns, PronghornStage[] stages) {
        super(graphManager);
        this.graphManager = graphManager;
        this.checksForLongRuns = checksForLongRuns;
        this.reverseOrder = reverseOrder;

        buildSchedule(graphManager, stages, reverseOrder);
    }
    
    public ScriptedSchedule schedule() {
    	return schedule;
    }
    
    RunningStdDev stdDevRate = null;

    public RunningStdDev stdDevRate() {
        if (null == stdDevRate) {
            stdDevRate = new RunningStdDev();
            int i = stages.length;
            while (--i >= 0) {
                Number n = (Number) GraphManager.getNota(graphManager, stages[i].stageId, GraphManager.SCHEDULE_RATE,
                                                         1_200);
                RunningStdDev.sample(stdDevRate, n.doubleValue());
            }

        }
        return stdDevRate;
    }

    public boolean checkForException() {
        if (firstException != null) {
        	if (firstException instanceof AssertionError) {
        		throw (AssertionError)firstException;
        	} else {
        		throw new RuntimeException(firstException);
        	}
        }
        return true;
    }

    public String name() {
        return name;
    }

    @Override
    public void startup() {
    	if (null==stages) {
    		return;
    	}

        startupAllStages(stages.length);
       
        setupHousekeeping();

    }

	private void setupHousekeeping() {
		producersIdx = buildProducersList(0, 0, graphManager, stages);
        producerInputPipes = buildProducersPipes(0, 0, 1, producersIdx, stages, graphManager);
        producerInputPipeHeads = new long[producerInputPipes.length];

        inputPipes = buildInputPipes(0, 0, 1, stages, graphManager);
        inputPipeHeads = new long[inputPipes.length];

        syncInputHeadValues(producerInputPipes, producerInputPipeHeads);
        syncInputHeadValues(inputPipes, inputPipeHeads);
	}

    private static void syncInputHeadValues(Pipe[] pipes, long[] heads) {
        int i = pipes.length;
        while (--i >= 0) {//keep these so we know that it has changed and there is new content
            heads[i] = Pipe.headPosition(pipes[i]);
        }
    }

    private static boolean isSyncInputHeadValues(Pipe[] pipes, long[] heads) {
        int i = pipes.length;
        while (--i >= 0) {//keep these so we know that it has changed and there is new content
            if (heads[i] != Pipe.headPosition(pipes[i])) {
                return false;
            }
        }
        return true;
    }

    private static int[] buildProducersList(int count, int idx, final GraphManager graphManager, PronghornStage[] stages) {

        //skip over the non producers
        while (idx < stages.length) {

            if (null != GraphManager.getNota(graphManager, stages[idx].stageId, GraphManager.PRODUCER, null) ||
                    (0 == GraphManager.getInputPipeCount(graphManager, stages[idx]))) {
                int[] result = buildProducersList(count + 1, idx + 1, graphManager, stages);
                result[count] = idx;
                return result;
            }

            idx++;
        }

        return new int[count];

    }

    private static Pipe[] buildProducersPipes(int count, int indexesIdx, int outputIdx, final int[] indexes, final PronghornStage[] stages, final GraphManager graphManager) {

        while (indexesIdx < indexes.length) {

            int outputCount = GraphManager.getOutputPipeCount(graphManager, stages[indexes[indexesIdx]].stageId);
            while (outputIdx <= outputCount) {

                Pipe pipe = GraphManager.getOutputPipe(graphManager, stages[indexes[indexesIdx]], outputIdx);

                //is the consumer of this pipe inside the graph?
                int consumerId = GraphManager.getRingConsumerId(graphManager, pipe.id);

                int k = stages.length;
                while (--k >= 0) {
                    if (stages[k].stageId == consumerId) {

                        Pipe[] result = buildProducersPipes(count + 1, indexesIdx, outputIdx + 1, indexes, stages,
                                                            graphManager);
                        result[count] = pipe;
                        return result;

                    }
                }
                outputIdx++;
            }
            outputIdx = 1;
            indexesIdx++;
        }
        return new Pipe[count];

    }

    private static Pipe[] buildInputPipes(int count, int stageIdx, int inputIdx, final PronghornStage[] stages, final GraphManager graphManager) {

        while (stageIdx < stages.length) {

            int inputCount = GraphManager.getInputPipeCount(graphManager, stages[stageIdx]);
            while (inputIdx <= inputCount) {

                Pipe pipe = GraphManager.getInputPipe(graphManager, stages[stageIdx], inputIdx);

                int producerId = GraphManager.getRingProducerId(graphManager, pipe.id);

                boolean isFromOutside = true;
                int k = stages.length;
                while (--k >= 0) {
                    if (stages[k].stageId == producerId) {
                        isFromOutside = false;
                        break;
                    }
                }
                if (isFromOutside) {
                    Pipe[] result = buildInputPipes(count + 1, stageIdx, inputIdx + 1, stages, graphManager);
                    result[count] = pipe;
                    return result;

                }

                inputIdx++;
            }
            inputIdx = 1;
            stageIdx++;
        }
        return new Pipe[count];

    }


    /**
     * Stages have unknown dependencies based on their own internal locks and the pipe usages.  As a result we do not
     * know the right order for starting them. 
     */
    private void startupAllStages(final int stageCount) {

        int j;
        boolean isAnyRateLimited = false;
        //to avoid hang we must init all the inputs first
        j = stageCount;
        while (--j >= 0) {
            //this is a half init which is required when loops in the graph are discovered and we need to initialized cross dependent stages.
            if (null != stages[j]) {
                GraphManager.initInputPipesAsNeeded(graphManager, stages[j].stageId);
                isAnyRateLimited |= GraphManager.isRateLimited(graphManager, stages[j].stageId);
            }
        }

        int unInitCount = stageCount;

        while (unInitCount > 0) {

            j = stageCount;
            while (--j >= 0) {
                final PronghornStage stage = stages[j];

                if (null != stage && !GraphManager.isStageStarted(graphManager, stage.stageId)) {

                    GraphManager.initAllPipes(graphManager, stage.stageId);

                    try {
                        logger.debug("begin startup of    {}", stage);

                       // Thread thread = Thread.currentThread();
                       // new ThreadLocal<Integer>();

        		    	long start = 0;
        		    	if (recordTime) {
        		    		start = System.nanoTime();	
        		    	}
        		    	
                        setCallerId(stage.boxedStageId);
                        stage.startup();
                        clearCallerId();
                        
        				if (recordTime) {
        					final long now = System.nanoTime();		        
        		        	long duration = now-start;
        		 			GraphManager.accumRunTimeNS(graphManager, stage.stageId, duration, now);
        				}
                        
                        logger.debug("finished startup of {}", stage);

                        //client work is complete so move stage of stage to started.
                        GraphManager.setStateToStarted(graphManager, stage.stageId);
                        unInitCount--;
                    } catch (Throwable t) {
                        recordTheException(stage, t, this);
                        try {
                            if (null != stage) {
                                setCallerId(stage.boxedStageId);
                                GraphManager.shutdownStage(graphManager, stage);
                                clearCallerId();
                            }
                        } catch (Throwable tx) {
                            recordTheException(stage, tx, this);
                        } finally {
                            if (null != stage) {
                                GraphManager.setStateToShutdown(graphManager,
                                                                stage.stageId); //Must ensure marked as terminated
                            }
                        }
                        //TODO: need to halt more startups
                        //      while j< stage count must shutdown those started
                        //      then call shutdown on this scheduler
                        System.exit(-1); //this is a hack for now until the above is completed.
                        return;
                    }
                }
            }
        }


        rates = new long[stageCount + 1];
        lastRun = new long[stageCount + 1];


        int idx = stageCount;
        while (--idx >= 0) {
            final PronghornStage stage = stages[idx];

            //determine the scheduling rules
            if (null == GraphManager.getNota(graphManager, stage, GraphManager.UNSCHEDULED, null)) {

                Object value = GraphManager.getNota(graphManager, stage, GraphManager.SCHEDULE_RATE, Long.valueOf(0));
                long rate = value instanceof Number ? ((Number) value).longValue() : null == value ? 0 : Long.parseLong(
                        value.toString());

                //System.out.println("NTS schedule rate for "+stage+" is "+value);

                if (0 == rate) {
                    //DEFAULT, RUN IN TIGHT LOOP
                    rates[idx] = 0;
                    lastRun[idx] = 0;
                } else {
                    //SCHEDULE_rate, RUN EVERY rate ns
                    rates[idx] = rate;
                    if (rate > maxRate) {
                        maxRate = rate;
                    }
                    lastRun[idx] = 0;
                }
            } else {
                //UNSCHEDULED, NEVER RUN
                rates[idx] = -1;
                lastRun[idx] = 0;
            }
        }

    }

    public long nextRun() {
        return nextRun;
    }

    // Pre-allocate startup information.
    // this value is continues to keep time across calls to run.
    private long blockStartTime = System.nanoTime();
    
    private int platformThresholdForSleep = 0;


	
	private ReentrantLock modificationLock = new ReentrantLock();
    
    @Override
    public void run() {

    	assert(null != shutdownRequested) : "startup() must be called before run.";
        
    	playScript(graphManager, stages, schedule.script, recordTime);
				
    }

	private void playScript(GraphManager gm, PronghornStage[] localStages, 
			                int[] script, final boolean recordTime) {

		if (modificationLock.tryLock()) try {
			
			//TODO: before running the full group must compute part of the DynamicDisableSubGraph...
			
			
			final int length = script.length;
			int scheduleIdx = 0;
	
	        //play the script
			while (scheduleIdx < length) {
	    		        	   
				if (enabled[scheduleIdx] > 0) {
					//skip over this one, it is disabled due to lack of use.
					scheduleIdx += enabled[scheduleIdx];
				} else {			
					
					////////////////////
					//before we compute the wait time 
					//call yield to let the OS get in a time slice swap
					Thread.yield();
					//////////////////
					
		            // We need to wait between scheduler blocks, or else
		            // we'll just burn through the entire schedule nearly instantaneously.
		            //
		            // If we're still waiting, we need to wait until its time to run again.
					long now = System.nanoTime();
		        	final long wait = blockStartTime - now; 
		            if (wait > 0) {
		            	try {
		            		 waitForBatch(wait, blockStartTime);		            		
		            	} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							return;
						}
		            }

		            //NOTE: this moves the blockStartTime forward by the common clock unit.
					scheduleIdx = runBlock(scheduleIdx, script, localStages, gm, recordTime);
				
				}
	        }
			
		} finally {
			//release the modification lock so the schedule can be changed. 
			modificationLock.unlock();
		}
		
    	//this must NOT be inside the lock because this visit can cause a modification
		//to this same scheduler which will require the lock.
    	if (null!=checksForLongRuns && System.nanoTime()>nextLongRunningCheck) {
    		//NOTE: this is only called by 1 of the nonThreadSchedulers and not often
    		GraphManager.visitLongRunningStages(gm, checksForLongRuns );
		//////
			nextLongRunningCheck = System.nanoTime() + longRunningCheckFreqNS;
    	}
	
	}

	/////////////////
	//members for automatic switching of timer from fast to slow 
	//based on the presence of work load
	long totalRequiredSleep = 0;
	int noWorkCounter = 0;
	/////////////////
	
	private void waitForBatch(long wait, long blockStartTime) throws InterruptedException {
		
		totalRequiredSleep+=wait;
		long a = (totalRequiredSleep/1_000_000L);
		
		if (a>0) {
			long now = System.nanoTime();
			Thread.yield();
			Thread.sleep(a);
			long duration = System.nanoTime()-now;
			totalRequiredSleep -= (duration>0?duration:(a*1_000_000));
		} 
		
		automaticLoadSwitchingDelay();

	}

	private void automaticLoadSwitchingDelay() {
		accumulateWorkHistory();
		
		//if we have over 1000 cycles of non work found then
		//drop CPU usage to greater latency mode since we have no work
		//once work appears stay engaged until we again find 1000 
		//cycles of nothing to process, for 40mircros with is 40 ms switch.
		if (noWorkCounter<1000) {//do it since we have had recent work
			
			long now2 = System.nanoTime()/1_000_000l;
			
			if (0!=(now2&3)) {// 1/4 of the time every 1 ms we take a break for task manager
				while (totalRequiredSleep>100_000) {
					long now = System.nanoTime();
					if (totalRequiredSleep>500_000) {
						LockSupport.parkNanos(totalRequiredSleep);
					} else {
						Thread.yield();
					}
					long duration = System.nanoTime()-now;
					if (duration<=0) {
						break;
					}
					totalRequiredSleep-=duration;
				}
			} else {
				//let the task manager know we are not doing work.
				long now = System.nanoTime();
				LockSupport.parkNanos(totalRequiredSleep);
				long duration = System.nanoTime()-now;
				if (duration>0) {
					totalRequiredSleep -= duration;
				}
			}
		}
	}

	private void accumulateWorkHistory() {
		boolean hasData=false;
		int p = inputPipes.length;
		while (--p>=0) {			
			if (Pipe.contentRemaining(inputPipes[p])>0) {
				hasData=true;
				break;
			}
		}
		if (hasData) {
			noWorkCounter = 0;	
		} else {			
			noWorkCounter++;
		}
	}

	private int storeNewThreshold(long wait) {
		int platformThresholdForSleep;
		platformThresholdForSleep = (int)Math.min( wait*2, 2_000_000);//2 MS max value
		//logger.trace("new sleep threshold {}", platformThresholdForSleep);
		return platformThresholdForSleep;
	}

	boolean shownLowLatencyWarning = false;
	
	private int runBlock(int scheduleIdx, int[] script, 
			             PronghornStage[] stages,
			             GraphManager gm, final boolean recordTime) {
			
		boolean shutDownRequestedHere = false;
		int inProgressIdx;
		// Once we're done waiting for a block, we need to execute it!
		top:
		do {
		    // If it isn't out of bounds or a block-end (-1), run it!
		    if ((scheduleIdx<script.length) && ((inProgressIdx = script[scheduleIdx++]) >= 0)) {

		    	long start = 0;
		    	if (recordTime) {
		    		start = System.nanoTime();
		    		DidWorkMonitor.begin(didWorkMonitor,start);	
		    	}
		    	
		    	if (!run(gm, stages[inProgressIdx], this)) {
					shutDownRequestedHere = true;
				}
		        		    	
				if (recordTime && DidWorkMonitor.didWork(didWorkMonitor)) {
					final long now = System.nanoTime();		        
		        	long duration = now-start;
		 			if (!GraphManager.accumRunTimeNS(gm, stages[inProgressIdx].stageId, duration, now)){
						if (!shownLowLatencyWarning) {
							shownLowLatencyWarning = true;
							logger.warn("\nThis platform is unable to measure ns time slices due to OS or hardware limitations.\n Work was done by an actor but zero time was reported.\n");
						}
					}
				}
		    } else {
		    	break;
		    }
		} while (true);

		long now = System.nanoTime();
		
		//only add if we have already consumed the last cycle
		if (blockStartTime-schedule.commonClock<now) {
			// Update the block start time.
			blockStartTime += schedule.commonClock;
		}
		
		//if we have long running cycles which are longer than then common clock
		//bump up the time so it does not keep falling further behind.  this allows
		//the script to stop on the first "fast" cycle instead of taking multiple
		//runs at the script to catch up when this will not help the performance.
		if (blockStartTime <= now) {
			blockStartTime = now;
		}
        		
		// If a shutdown is triggered in any way, shutdown and halt this scheduler.
		if (!(shutDownRequestedHere || shutdownRequested.get())) {
			return scheduleIdx;
		} else {
			if (!shutdownRequested.get()) {
				shutdown();
			}
		    return Integer.MAX_VALUE;
		}
	}

	public boolean isContentForStage(PronghornStage stage) {
		int inC = GraphManager.getInputPipeCount(graphManager, stage.stageId);
		for(int k = 1; k <= inC; k++) {
			if (Pipe.contentRemaining((Pipe<?>)
					GraphManager.getInputPipe(graphManager, stage.stageId, k)
					) != 0) {
				return true;
			}
		}
		return false;
	}

    private static boolean run(GraphManager graphManager, 
    		                   PronghornStage stage, 
    		                   ScriptedNonThreadScheduler that) {
        try {
            if (!GraphManager.isStageShuttingDown(that.stateArray, stage.stageId)) {

                if (debugNonReturningStages) {
                    logger.info("begin run {}", stage);///for debug of hang
                }
                that.setCallerId(stage.boxedStageId);
                stage.run();
                that.clearCallerId();

                if (debugNonReturningStages) {
                    logger.info("end run {}", stage);
                }
                return true;
            } else {
                if (!GraphManager.isStageTerminated(graphManager, stage.stageId)) {
                    GraphManager.shutdownStage(graphManager, stage);
                    GraphManager.setStateToShutdown(graphManager, stage.stageId);
                }
                return false;
            }
        } catch (Throwable t) {
            recordTheException(stage, t, that);
            throw t;
        }

    }

    private Object key = "key";
    
    @Override
    public void shutdown() {
    	
        if (null!=stages && shutdownRequested.compareAndSet(false, true)) {

        	synchronized(key) {
                boolean debug = false;
                if (debug) {	
        	        System.err.println();
        	        System.err.println("----------full stages ------------- clock:"+schedule.commonClock);
        	        for(int i = 0; i<stages.length; i++) {
        	        	
        	        	StringBuilder target = new StringBuilder();
        	    		target.append("full stages "+stages[i].getClass().getSimpleName()+":"+stages[i].stageId);
        	    		target.append("  inputs:");
        	    		GraphManager.appendInputs(graphManager, target, stages[i]);
        	    		target.append(" outputs:");
        	    		GraphManager.appendOutputs(graphManager, target, stages[i]);
        	    		        		
        	    		System.err.println("   "+target);
        	        	
        	        }
                }
            }
        	
        	
        	
            int s = stages.length;
            while (--s >= 0) {
                //ensure every non terminated stage gets shutdown called.
                if (null != stages[s] && !GraphManager.isStageTerminated(graphManager, stages[s].stageId)) {
                    GraphManager.shutdownStage(graphManager, stages[s]);
                    GraphManager.setStateToShutdown(graphManager, stages[s].stageId);
                    //System.err.println("terminated "+stages[s]+"  "+GraphManager.isStageTerminated(graphManager, stages[s].stageId));
                }
            }

            PronghornStage temp = lastRunStage;
            if (null != temp) {
                logger.info("ERROR: this stage was called but never returned {}", temp);
            }
        }
        
    }

    public static boolean isShutdownRequested(ScriptedNonThreadScheduler nts) {
        return nts.shutdownRequested.get();
    }

    @Override
    public void awaitTermination(long timeout, TimeUnit unit, Runnable clean, Runnable dirty) {
        if (awaitTermination(timeout, unit)) {
            clean.run();
        } else {
            dirty.run();
        }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {

        if (!shutdownRequested.get()) {
            throw new UnsupportedOperationException("call shutdown before awaitTerminination");
        }
        long limit = System.nanoTime() + unit.toNanos(timeout);

        if (isRunning.get() != 2) {
            //wait until we get shutdown or timeout.
            while (!isRunning.compareAndSet(0, 2)) {
                Thread.yield();
                if (System.nanoTime() > limit) {
                    return false;
                }
            }
        }

        int s = stages.length;
        while (--s >= 0) {
            PronghornStage stage = stages[s];

            if (null != stage && !GraphManager.isStageTerminated(graphManager, stage.stageId)) {
                GraphManager.shutdownStage(graphManager, stage);
                GraphManager.setStateToShutdown(graphManager, stage.stageId);
            }
        }

        return true;

    }

    @Override
    public boolean TerminateNow() {
        shutdown();
        return true;
    }

    private static void recordTheException(final PronghornStage stage, Throwable t, ScriptedNonThreadScheduler that) {
        synchronized (that) {
            if (null == that.firstException) {
                that.firstException = t;
            }
        }

        GraphManager.reportError(that.graphManager, stage, t, logger);
    }
    
    

	public static int[] buildSkipScript(ScriptedSchedule schedule, 
			                            GraphManager gm, 
			                            PronghornStage[] stages,
			                            int[] script) {
		
		int[] skipScript = new int[script.length];
		int lastPointIndex = 0;
		
		for(int idx = 0; idx<script.length; idx++) {
			if (script[idx] != -1) {
				int stageId = stages[schedule.script[idx]].stageId;

				final int inC1 = GraphManager.getInputPipeCount(gm, stageId);
				if ((inC1 == 0) || (GraphManager.hasNota(gm, stageId, GraphManager.PRODUCER))) {
					//producer, all producers must always be run.
					skipScript[idx] = -2;
					lastPointIndex = idx+1; //this starts a new run from here.
					continue;
				} else {
					for(int k = 1; k <= inC1; k++) {
						int id = GraphManager.getInputPipe(gm, stageId, k).id;
						//this id MUST be found as one of the previous outs
						//if not found this is a new point
						if (!isInputLocal(lastPointIndex, idx, gm, stages, script, id)) {
							lastPointIndex = idx;
						}
					}
				}
				
				//count up how long this run is at the head position of the run
				if (lastPointIndex!=-1) {
					skipScript[lastPointIndex]++;
				}
				
			} else {
				skipScript[idx] = -1;
				lastPointIndex = idx+1;
			}
			
			
		}
		return skipScript;
	}

	private static boolean isInputLocal(int startIdx,
										int stopIdx, 
			                            GraphManager gm, 
			                            PronghornStage[] stages, 
			                            int[] script,
			                            int goalId) {
		//scan for an output which matches this goal Id
		
		for(int i = startIdx; i<=stopIdx; i++) {
			int stageId = stages[script[i]].stageId;
			int outC = GraphManager.getOutputPipeCount(gm, stageId);
			for(int k = 1; k <= outC; k++) {
				if (goalId == GraphManager.getOutputPipe(gm, stageId, k).id) {
					return true;
				}
			}
		}
		return false;
	}

	////////////////////
	//these elapsed time methods are for thread optimization
	///////////////////
	public long nominalElapsedTime(GraphManager gm) {
		long sum = 0;
		int i = stages.length;
		while (--i>=0) {
			sum += GraphManager.elapsedAtPercentile(gm,stages[i].stageId, .80f);
		}
		return sum;
	}
	
	//if this stage has no inputs which came from this
	//same array of stages then yes it has no local inputs
	private boolean hasNoLocalInputs(int idx) {
		
		int id = stages[idx].stageId;
		
		int count = GraphManager.getInputPipeCount(graphManager, id);
		for(int c=1; c<=count; c++) {
			int inPipeId = GraphManager.getInputPipeId(graphManager, id, c);			
			int producerId = GraphManager.getRingProducerStageId(graphManager, inPipeId);
			
			int w = idx;
			while (--w>=0) {
				if (stages[w].stageId == producerId) {
					return false;
				}
			}
		}		
		return true;
	}
	
	public int recommendedSplitPoint(GraphManager gm) {
		
		long aMax = -1;
		int aMaxIdx = -1;
		
		long bMax = -1;
		int bMaxIdx = -1;
				
		int i = stages.length;
		while (--i>=0) {
			long elap = GraphManager.elapsedAtPercentile(gm,stages[i].stageId, .80f);
		
			//find the general largest
			if (elap > aMax) {
				aMax = elap;
				aMaxIdx = i;
			}
			
			//find the largest of those which have no local input pipes
			if ((elap > bMax) && hasNoLocalInputs(i)) {
				bMax = elap;
				bMaxIdx = i;
			}			
		}		
		//return the natural split if it was found else return the large one.
		return (-1 != bMaxIdx) ? bMaxIdx : aMaxIdx;				
	}
	///////////////////
	///////////////////
	
	public ScriptedNonThreadScheduler splitOn(int idx) {
		assert(idx<stages.length);
		assert(idx>=0);
		//stop running while we do the split.
		modificationLock.lock();
		//all running of this script is not blocked until we finish this modification.
		try {
			
			ScriptedNonThreadScheduler result = null;
			
			PronghornStage[] localStages;
			PronghornStage[] resultStages;
			if (reverseOrder) {
				resultStages = Arrays.copyOfRange(stages, 0, idx+1);
				localStages = Arrays.copyOfRange(stages, idx+1, stages.length);
				assert(resultStages.length>0);
				assert(localStages.length>0);
			} else {
				resultStages = Arrays.copyOfRange(stages, idx, stages.length);
				localStages = Arrays.copyOfRange(stages, 0, idx);
				assert(resultStages.length>0) : "stages "+stages.length+" at "+idx;
				assert(localStages.length>0);
			}

			result = new ScriptedNonThreadScheduler(graphManager, reverseOrder, resultStages);
			buildSchedule(graphManager, localStages, reverseOrder);
	        setupHousekeeping();

	        result.setupHousekeeping();
	        
			return result;
		} finally {
			modificationLock.unlock();
		}
	}

	
	
}
