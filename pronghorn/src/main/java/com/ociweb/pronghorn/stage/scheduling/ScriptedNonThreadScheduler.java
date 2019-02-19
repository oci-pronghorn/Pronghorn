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

	//should have Numa truned on           -XX:+UseNUMA
	//should have priorities on for linux  -XX:+UseThreadPriorities -XX:+UseNUMA
	//thread pinning may be good as well.
	
    public static Appendable debugStageOrder = null; //turn on to investigate performance issues.
	
	private AtomicBoolean shutdownRequested = new AtomicBoolean(false);;
    private long[] rates;
    private long[] lastRun;
    public PronghornStage[] stages;
    private long[] sla;
	
    private final int defaultValue = 4_000; //Default for this scheduler
	
    private DidWorkMonitor didWorkMonitor;
	private long deepSleepCycleLimt;
	
    private static AtomicInteger threadGroupIdGen = new AtomicInteger();
    
    private long maxRate;
    private static final Logger logger = LoggerFactory.getLogger(ScriptedNonThreadScheduler.class);

    private long nextRun = 0; //keeps times of the last pass so we need not check again

    private long timeStartedRunningStage;
    private PronghornStage runningStage;
    
    
    private volatile Throwable firstException;//will remain null if nothing is wrong

    //Time based events will poll at least this many times over the period.
    // + ensures that the time trigger happens "near" the edge
    // + ensures that this non-thread scheduler in unit tests can capture the time delayed events.
    public static final int granularityMultiplier = 4;
    private static final long MS_TO_NS = 1_000_000;
    private static final long realWorldLimitNS = 2_000_000_000;
    
    public static boolean hangDetectorEnabled = true;

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
  
    private ScriptedSchedule schedule = null;


    
    public static boolean globalStartupLockCheck = false;
    static {
    	assert(globalStartupLockCheck=true);//yes this assigns and returns true   	
    }
    
    private long nextLongRunningCheck;
    private final long longRunningCheckFreqNS = 60_000_000_000L;//1 min
    private final StageVisitor checksForLongRuns;

	private boolean shownLowLatencyWarning = false;
    
	private ElapsedTimeRecorder sleepETL = null;//new ElapsedTimeRecorder();
	
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

    	int groupId = threadGroupIdGen.incrementAndGet();
    	
    	this.didWorkMonitor = new DidWorkMonitor();
    	
    	
    	if (null==stages) {
    		schedule = new ScriptedSchedule(0, new int[0], 0);
    		//skipScript = new int[0];
    		return;
    	}
    	
    	this.sla = new long[stages.length];
    	int j = stages.length;
    	while (--j>=0) {
    		Number num = (Number) GraphManager.getNota(graphManager, stages[j].stageId, 
    				GraphManager.SLA_LATENCY, null);
    		this.sla[j] = null==num?Long.MAX_VALUE:num.longValue(); 
    	}
    	        
    	StringBuilder totalName = new StringBuilder();
    	
    	for(int s = 0; s<stages.length; s++) {
    		PronghornStage pronghornStage = stages[s];
    		Appendables.appendValue(totalName, pronghornStage.stageId).append(":");
    		totalName.append(pronghornStage.getClass().getSimpleName()
    				 .replace("Stage","")
    				 .replace("Supervisor","")
    				 .replace("Extraction","")
    				 .replace("Listener","L")
    				 .replace("Reader","R")
    				 .replace("Request","Req")
    				 .replace("Reactive","R")
    				 .replace("Writer","W") 
    				 .replace("Module","M")
    				 .replace("SSLEngine","TLS")
    				 .replace("Wrap","W") 
    				 .replace("Parser", "P")
    				 .replace("Response", "Resp")
    				 .replace("Server","S")
    				 .replace("Client","C"));
    		if (s<stages.length-1) {
    			totalName.append(",");
    		}
    	}
    	name = totalName.toString();

        // Pre-allocate rates based on number of stages.

        long rates[] = new long[stages.length];

        int k = stages.length;
        while (--k>=0) {
        	
        	//set thread name
        	if (groupId>=0) {
        		GraphManager.recordThreadGroup(stages[k], groupId, graphManager);
        	}
        	
        	//add monitoring to each pipe
 
    		PronghornStage.addWorkMonitor(stages[k], didWorkMonitor);
    		//NOTE: stages have "done work" if something was published to an output
    		GraphManager.addPublishFromListener(graphManager, stages[k], didWorkMonitor);
   		
    		
    		//TODO: ask about upstream if they did not work?
    		
    		
        	// Determine rates for each stage.
			long scheduleRate = Long.valueOf(String.valueOf(GraphManager.getNota(graphManager, stages[k], GraphManager.SCHEDULE_RATE, defaultValue)));
            rates[k] = scheduleRate;
        }

        // Build the script.
        schedule = PMath.buildScriptedSchedule(rates, reverseOrder);

        //in cycles but under the human perception of time
        deepSleepCycleLimt = realWorldLimitNS/schedule.commonClock;

        if (hangDetectorEnabled) {
        	assert(hangDetectInit(Math.max(schedule.commonClock*100*schedule.script.length, 20_000_000_000L)));
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


	private long threadId;
	public void setThreadId(long id) {
		id = threadId;
	}
    
    public void detectHangingThread(long now, long timeoutNS) {

		if (didWorkMonitor.isOverTimeout(now, timeoutNS)) {
			didWorkMonitor.interrupt();
		}
	
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
    	modificationLock.lock(); //hold the lock
    	try {
    		startupAllStages(stages.length, globalStartupLockCheck);
    		setupHousekeeping();
    	} catch (RuntimeException re) {
    		shutdown();    		
    	}

    }
    
    public void startup(boolean watchForHang) {
    	if (null==stages) {
    		return;
    	}

    	if (!watchForHang) {
    		startup();
    	} else {
    		
	    	modificationLock.lock(); //hold the lock

	        startupAllStages(stages.length, watchForHang);
	        logger.info("wait for collection of housekeeping data");
	        setupHousekeeping();
	        logger.info("finished startup");
    	}
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

            PronghornStage pronghornStage = stages[idx];
			if (isProducer(graphManager, pronghornStage)) {
                int[] result = buildProducersList(count + 1, idx + 1, graphManager, stages);
                result[count] = idx;
                return result;
            }

            idx++;
        }

        return new int[count];

    }

	private static boolean isProducer(final GraphManager graphManager, PronghornStage pronghornStage) {
		return GraphManager.hasNota(graphManager, pronghornStage.stageId, GraphManager.PRODUCER) ||
		        (0 == GraphManager.getInputPipeCount(graphManager, pronghornStage));
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

    	int s=stageIdx;
    	//walk it twice, can not recurse because large graphs cause overflow...
        while (s < stages.length) {

            int inputCount = GraphManager.getInputPipeCount(graphManager, stages[s]);
          
            ///copy how many pipes we will have
            while (inputIdx <= inputCount) {
                Pipe inputPipe = GraphManager.getInputPipe(graphManager, stages[s], inputIdx);
                int producerStageId = GraphManager.getRingProducerId(graphManager, inputPipe.id);

                boolean isFromOutside = true;
                int k = stages.length;
                while (--k >= 0) {
                    if (stages[k].stageId == producerStageId) {
                        isFromOutside = false;
                        break;
                    }
                    
                }
                if (isFromOutside) {
                	count++;             
                }
                inputIdx++;
            }
            inputIdx = 1;
            s++;
        }
        ///////////////////////////
        int i = 0;       
        Pipe[] result = new Pipe[count];
        ///fill the array
        s=stageIdx;
        while (s < stages.length) {
	        int inputCount = GraphManager.getInputPipeCount(graphManager, stages[s]);
	          
	        while (inputIdx <= inputCount) {
	
	            Pipe inputPipe = GraphManager.getInputPipe(graphManager, stages[s], inputIdx);
	
	            int producerStageId = GraphManager.getRingProducerId(graphManager, inputPipe.id);
	
	            boolean isFromOutside = true;
	            int k = stages.length;
	            while (--k >= 0) {
	                if (stages[k].stageId == producerStageId) {
	                    isFromOutside = false;
	                    break;
	                }
	                
	            }
	            if (isFromOutside) { //skip repeats to same producer
	            	result[i++] = inputPipe;
	            }
	          
	            inputIdx++;
	        }
	        inputIdx = 1;
	        s++;
        }
        assert(result.length==i);
        return result;
    }


    /**
     * Stages have unknown dependencies based on their own internal locks and the pipe usages.  As a result we do not
     * know the right order for starting them. 
     */
    private void startupAllStages(final int stageCount, boolean log) {

        int j;
        //to avoid hang we must init all the inputs first
        j = stageCount;
        while (--j >= 0) {
            //this is a half init which is required when loops in the graph are discovered and we need to initialized cross dependent stages.
            if (null != stages[j]) {
                GraphManager.initInputPipesAsNeeded(graphManager, stages[j].stageId);
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
                    	assert(hangDetectBegin(stage, Thread.currentThread()));

                    	long start = 0;
        		    	if (GraphManager.isTelemetryEnabled(graphManager)) {
        		    		start = System.nanoTime();	
        		    	}
        		    	
                        setCallerId(stage.boxedStageId);
                        stage.startup();
                        clearCallerId();
                        
        				if (GraphManager.isTelemetryEnabled(graphManager)) {
        					final long now = System.nanoTime();		        
        		        	long duration = now-start;
        		 			GraphManager.accumRunTimeNS(graphManager, stage.stageId, duration, now);
        				}
        				assert(hangDetectFinish());

                        
                        //client work is complete so move stage of stage to started.
                        GraphManager.setStateToStarted(graphManager, stage.stageId);
                        unInitCount--;
                    } catch (Exception t) {
                        recordTheException(stage, t, this);
                        try {
                            if (null != stage) {
                                setCallerId(stage.boxedStageId);
                                GraphManager.shutdownStage(graphManager, stage);
                                clearCallerId();
                            }
                        } catch (Exception tx) {
                            recordTheException(stage, tx, this);
                        } finally {
                            if (null != stage) {
                                GraphManager.setStateToShutdown(graphManager,
                                                                stage.stageId); //Must ensure marked as terminated
                            }
                        }
                        
                        for(int k=j+1;j<stageCount;j++) {
                            final PronghornStage stage2 = stages[k];
                            if (null != stage2 && GraphManager.isStageStarted(graphManager, stage2.stageId)) {
                            	stage2.requestShutdown();
                            }
                        }
                        this.shutdown();
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

    	try {
    		playScript(this);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			shutdown();
		}
    }

	public static void playScript(ScriptedNonThreadScheduler that) throws InterruptedException {

				assert(null != that.shutdownRequested) : "startup() must be called before run.";
					
				checkForModifications(that);			
			
		        //play the script
	   			int scheduleIdx = 0;
				int[] script = that.schedule.script;
				while (scheduleIdx < script.length) {	
						
						waitBeforeRun(that, System.nanoTime());
		
						scheduleIdx = that.runBlock(scheduleIdx, script, that.stages, 
								GraphManager.isTelemetryEnabled(that.graphManager));
		        }
		
				checkForLongRun(that);
				
	
	}

	private static void waitBeforeRun(ScriptedNonThreadScheduler that, long now) throws InterruptedException {
		final long wait = that.blockStartTime - now;
		
			assert(wait<=that.schedule.commonClock) : "wait for next cycle was longer than cycle definition";
	
			//skip wait if it is short AND if this thread has max proirity
			if (wait<10_000 && //short is < 10 microseconds
				Thread.currentThread().getPriority()==Thread.MAX_PRIORITY) {
				boolean isNormalCase = that.accumulateWorkHistory();
				if (that.noWorkCounter > 100_000) { 
					that.deepSleep(isNormalCase);
					that.noWorkCounter=0;
				}	
			} else {
				if (wait > 0) {
					assert(wait<=that.schedule.commonClock) : "wait for next cycle was longer than cycle definition";
					that.waitForBatch(that.totalRequiredSleep+wait); //updates totalRequiredSleep
				} else {
					Thread.yield();
					that.totalRequiredSleep=0;//clear running sleep.
					//warning clock rate is faster than the tasks
				}
			}
			
			if (null!=that.sleepETL) {
				ElapsedTimeRecorder.record(that.sleepETL, System.nanoTime()-now);
			}
		
	}

	private static void checkForLongRun(ScriptedNonThreadScheduler that) {
		//this must NOT be inside the lock because this visit can cause a modification
		//to this same scheduler which will require the lock.
    	//TODO: disable until we fix slowness
		if (false && null!=that.checksForLongRuns && System.nanoTime()>that.nextLongRunningCheck) {
    		
    		long now = System.nanoTime();
    		//NOTE: this is only called by 1 of the nonThreadSchedulers and not often
    		GraphManager.visitLongRunningStages(that.graphManager, that.checksForLongRuns );
		//////
    		that.nextLongRunningCheck = System.nanoTime() + that.longRunningCheckFreqNS;
    		
    		long duration = System.nanoTime()-now;
    		logger.info("Thread:{} checking for long runs, took {}",that.name,
    				Appendables.appendNearestTimeUnit(new StringBuilder(), duration)
    				);
    		
    	}
	}

	private static void checkForModifications(ScriptedNonThreadScheduler that) {
		if (that.modificationLock.hasQueuedThreads()) {
			that.modificationLock.unlock();
			Thread.yield();//allow for modification				
			that.modificationLock.lock();				
		}
	}

	/////////////////
	//members for automatic switching of timer from fast to slow 
	//based on the presence of work load
	long totalRequiredSleep = 0;
	long noWorkCounter = 0;
	/////////////////
	
	private void waitForBatch(long local) throws InterruptedException {
		////////////////
		//without any delays the telemetry is likely to get backed up
		////////////////
		
		
		///////////////////////////////
		//this section is only for the case when we need to wait long
		//it is run it often dramatically lowers the CPU usage due to parking
		//using many small yields usage of this is minimized.
		//////////////////////////////
		totalRequiredSleep = local;
		//if the sleep rate is less than this the server will
		//show CPU consumed while it waits for more work. Above this
		//value however the server should not consume significant resources.
		if (local<100_000) {//ns timer only accurate above this
			automaticLoadSwitchingDelay();
		} else {		
			long now = System.nanoTime();
			//Thread.yield();
			LockSupport.parkNanos(totalRequiredSleep);
			long duration = System.nanoTime()-now;
			totalRequiredSleep -= (duration>0?duration:1);
			
			//////////////////////////////
			//////////////////////////////
					
			automaticLoadSwitchingDelay();
		}
	}

	private void automaticLoadSwitchingDelay() {

		boolean isNormalCase = accumulateWorkHistory();
						
		int cyclesOfNoWorkBeforeSleep = 10_000_000; //do not sleep too soon, may cause issues..
		
		//if we have over X cycles of non work found then
		//drop CPU usage to greater latency mode since we have no work
		//once work appears stay engaged until we again find 1000 
		//cycles of nothing to process, for 40mircros with is 40 ms switch.
		if ( (noWorkCounter<cyclesOfNoWorkBeforeSleep || deepSleepCycleLimt<=0)) {//do it since we have had recent work
			
			if (isInDeepSleepMode) {
				logger.info("waking up from deep sleep for :\n "+this.name());
			}
			isInDeepSleepMode = false;
			
			long now = System.nanoTime();
			long nowMS = now/1_000_000l;
	
			if (0!=(nowMS&7)) {// 1/8 of the time every 1 ms we take a break for task manager
				long loopTop = -1;
				while (totalRequiredSleep>1000) {//was 400
					loopTop = now;
					if (totalRequiredSleep>20_000) { //was 500_000
						LockSupport.parkNanos(totalRequiredSleep);				
					} else {
						Thread.yield();					
					}
					now = System.nanoTime();
					long duration = now-loopTop;
					if (duration<0) {
						duration = 1;
					}
					totalRequiredSleep-=duration;
				}
				//System.out.println("loop "+x+" "+y);
			} else {	
				//let the task manager know we are not doing work.
				LockSupport.parkNanos(totalRequiredSleep);
				Thread.yield();
				
				long duration = System.nanoTime()-now;
				//System.err.println(totalRequiredSleep+" vs "+duration);
				if (duration>0) {
					totalRequiredSleep -= duration;
				}
			}
		} else {
				
			//TODO: for deep sleep of producers we need..
			//      1. the producer stage split from trailing tasks
			//      2. pipe listener from the producer to wake down stream..
			//NOTE: what we have here is fine but later we will improve it.			
			
			if (!isInDeepSleepMode) {
				logger.info("entering deep sleep for :\n "+this.name());
			}
			isInDeepSleepMode = true;
			deepSleep(isNormalCase);
		}
	}

	private boolean isInDeepSleepMode = false;
	
	private void deepSleep(boolean isNormalCase) {

		if (isNormalCase) {
			int maxIterations = 20;//this is limited or we may be sleeping during shutdown request.
			while (--maxIterations>=0 && (noWorkCounter > deepSleepCycleLimt)) {
				LockSupport.parkNanos(realWorldLimitNS);
				//we stay in this loop until we find real work needs to be done
				//but not too long or we will not be able to shutdown.
				if (!accumulateWorkHistory()) {
					break;
				}	
			}	
		} else {
			Thread.yield();
		}
	}

	@SuppressWarnings("unchecked")
	private boolean accumulateWorkHistory() {
		int p = inputPipes.length;
		if (p==0) {
			noWorkCounter = 0;
			return false;
		} else {
		
			boolean hasData=false;
			while (--p>=0) {		
				//this does dirty checks so we must be sure no asserts are used
				if (!Pipe.isEmpty(inputPipes[p])) {
					hasData=true;
					break;
				}
			}
			if (hasData) {
				noWorkCounter = 0;	
			} else {			
				noWorkCounter++;
			}
			return producersIdx.length==0;
		}
		
	}
	
	private int runBlock(int scheduleIdx, int[] script, 
			             PronghornStage[] stages,
			             final boolean recordTime) {
			
		final long startNow = System.nanoTime();
		boolean shutDownRequestedHere = false;
		int inProgressIdx;
		// Once we're done waiting for a block, we need to execute it!
		long SLABase = System.currentTimeMillis();
		long SLAStart = SLABase;
		long SLAStartNano = System.nanoTime();
		// If it isn't out of bounds or a block-end (-1), run it!
		while ((scheduleIdx<script.length)
				&& ((inProgressIdx = script[scheduleIdx++]) >= 0)) {
			
			long start = System.nanoTime();
			if (start > SLAStartNano) {
				SLAStart = SLABase + ((start-SLAStartNano)/1_000_000);  				
			} else {
				SLABase = System.currentTimeMillis();
				SLAStart = SLABase;
				SLAStartNano = System.nanoTime();
			}			
	
			shutDownRequestedHere |= runStage(recordTime, 
					didWorkMonitor, 
			        inProgressIdx, SLAStart, start,
			        stages[inProgressIdx]);		
			
		}

		//given how long this took to run set up the next run cycle.
		blockStartTime = startNow+schedule.commonClock;
		
		boolean debugHighCPU = false;
		if (debugHighCPU && blockStartTime<System.nanoTime()) {
			System.out.println("slow clock or make stages faster");
			System.out.println("warning clock is faster than tasks in thread: "+name);
		}
		
		return detectShutdownScheduleIdx(scheduleIdx, shutDownRequestedHere);
	}

	private final int detectShutdownScheduleIdx(int scheduleIdx, boolean shutDownRequestedHere) {

		// If a shutdown is triggered in any way, shutdown and halt this scheduler.
		return (!(shutDownRequestedHere || shutdownRequested.get())) ? scheduleIdx : triggerShutdown();
	}

	private int triggerShutdown() {
     	
		if (!shutdownRequested.get()) {
			shutdown();
		}
		return Integer.MAX_VALUE;
	}

	//boolean noDeepAssertChecks = true;
	
	private boolean runStage(final boolean recordTime,
			final DidWorkMonitor localDidWork, 
			int inProgressIdx, long SLAStart,
			long start, final PronghornStage stage) {
		
        //////////these two are for hang detection
		runningStage = stage;	
		timeStartedRunningStage = start;
		
		DidWorkMonitor.begin(localDidWork, start);		
		boolean shutDownRequestedHere = false;
		
//		if (noDeepAssertChecks) {
//			try {		
//				//NOTE: if no stages have shutdown we could elminate this check with a single boolean. TODO: if this shows up in profiler again.
//				if (!(stateArray[stage.stageId] >= GraphManagerStageStateData.STAGE_STOPPING)) {
//						stage.run();	        
//				        timeStartedRunningStage = 0;
//				} else {				
//				    processShutdown(graphManager, stage);
//				    shutDownRequestedHere = true;
//				}
//			} catch (Exception e) {			
//				shutDownRequestedHere = processExceptionAndCleanup(this, stage, e);
//			}		    
//		} else {		
			shutDownRequestedHere = runStageImpl(this, stage);	
//		}
		if (!DidWorkMonitor.didWork(localDidWork)) {
			GraphManager.recordNoWorkDone(graphManager,stage.stageId);			
		} else {
			ScriptedNonThreadScheduler.recordRunResults(
					         this, recordTime, 
					         inProgressIdx, start, SLAStart, stage);
		}
		return shutDownRequestedHere;
	}

	private static boolean runStageImpl(final ScriptedNonThreadScheduler that, final PronghornStage stage) {
		try {		
			//NOTE: if no stages have shutdown we could elminate this check with a single boolean. TODO: if this shows up in profiler again.
			if (!(that.stateArray[stage.stageId] >= GraphManagerStageStateData.STAGE_STOPPING)) {
					
					that.setCallerId(stage);		        
					assert(that.hangDetectBegin(stage, Thread.currentThread()));
					stage.run();
			        assert(that.hangDetectFinish());
			        that.clearCallerId();		        
			        that.timeStartedRunningStage = 0;
			        return false;
			} else {				
			    processShutdown(that.graphManager, stage);
			    return true;
			}
		} catch (Exception e) {			
			return processExceptionAndCleanup(that, stage, e);
		}		        
	}

	private static boolean processExceptionAndCleanup(final ScriptedNonThreadScheduler that, final PronghornStage stage,
			Exception e) {
		that.processException(stage, e);
		
		assert(that.hangDetectFinish());
		that.clearCallerId();		        
		that.timeStartedRunningStage = 0;
		return false;
	}

	private HangDetector hd;
	
    private boolean hangDetectInit(long timeout) {
		hd = new HangDetector(timeout);
		return true;
	}
	
	private boolean hangDetectBegin(PronghornStage stage, Thread t) {
		if (null!=hd) {
			hd.begin(stage,t);
		}
		return true;
	}

	private boolean hangDetectFinish() {
		if (null!=hd) {
			hd.finish();
		}
		return true;
	}

	int msgConsumerTrigger = 0;
	
	private static void recordRunResults(
			ScriptedNonThreadScheduler that, 
			final boolean recordTime, int inProgressIdx, long start,
			long SLAStart, PronghornStage stage) {
			        
		
		if (recordTime) {		
			long now = System.nanoTime(); //this takes time, avoid if possible
			if (!GraphManager.accumRunTimeNS(that.graphManager, stage.stageId, now-start, now)){
				assert(reportLowAccuracyClock(that));
			}

			//No need to run on every call, we run 1 out of every 4
			if (0== (0x3 & that.msgConsumerTrigger++)) {
				int c = GraphManager.getInputPipeCount(that.graphManager, stage.stageId);
				for(int i = 1; i<=c ;i++) {
					Pipe<?> pipe = GraphManager.getInputPipe(that.graphManager, stage.stageId, i);
					pipe.markConsumerPassDone();
				}	
			}
			
			
		}
	}

	private static boolean reportLowAccuracyClock(ScriptedNonThreadScheduler that) {
		if (that.shownLowLatencyWarning) {
		} else {
			that.shownLowLatencyWarning = true;
			logger.warn("\nThis platform is unable to measure ns time slices due to OS or hardware limitations.\n Work was done by an actor but zero time was reported.\n");
		}
		return true;
	}

	private void processException(PronghornStage stage, Exception e) {
		recordTheException(stage, e, this);
		//////////////////////
		//check the output pipes to ensure no writes are in progress
		/////////////////////
		int c = GraphManager.getOutputPipeCount(graphManager, stage.stageId);
		for(int i=1; i<=c; i++) {		
			if (Pipe.isInBlobFieldWrite((Pipe<?>) GraphManager.getOutputPipe(graphManager, stage.stageId, i))) {
				//we can't recover from this exception because write was left open....
				shutdown();	
				break;
			}
		}
	}

	private void reportSLAViolation(String stageName, GraphManager gm, int inProgressIdx, long SLAStart,
			long duration) {
		int nameLen = stageName.indexOf('\n');
		if (-1==nameLen) {
			nameLen = stageName.length();
		}
		Appendables.appendEpochTime(
				Appendables.appendEpochTime(
						Appendables.appendNearestTimeUnit(System.err.append("SLA Violation: "), duration)
						.append(" ")
						.append(stageName.subSequence(0, nameLen))
						.append(" ")
						,SLAStart).append('-')
				,System.currentTimeMillis())
		.append(" ").append(gm.name).append("\n");
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



	private static boolean processShutdown(GraphManager graphManager, PronghornStage stage) {
		if (!GraphManager.isStageTerminated(graphManager, stage.stageId)) {
		    GraphManager.shutdownStage(graphManager, stage);
		    GraphManager.setStateToShutdown(graphManager, stage.stageId);
		}
		return false;
	}

    private Object key = "key";
    
    @Override
    public void shutdown() {

        if (null!=stages && shutdownRequested.compareAndSet(false, true)) {
        	synchronized(key) {
        		
        		if (null!=sleepETL) {        			
        			System.err.println("sleep: "+graphManager.name+" "+name);
        			System.err.println(sleepETL.toString());
        		}
        		
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
            	 if (null != stages[s]) {
            		 //ensure every non terminated stage gets shutdown called.
            		 if (!GraphManager.isStageTerminated(graphManager, stages[s].stageId)) {
            			 GraphManager.shutdownStage(graphManager, stages[s]);
            			 GraphManager.setStateToShutdown(graphManager, stages[s].stageId);
                    //System.err.println("terminated "+stages[s]+"  "+GraphManager.isStageTerminated(graphManager, stages[s].stageId));
            		 }
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
    public boolean terminateNow() {
        shutdown();
        return true;
    }

    private static void recordTheException(final PronghornStage stage, Exception e, ScriptedNonThreadScheduler that) {
        synchronized (that) {
            if (null == that.firstException) {
                that.firstException = e;
            }
        }

        GraphManager.reportError(that.graphManager, stage, e, logger);
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
		
		logger.info("-------------- splitting scheduler -------------------------");
		
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

			//clear old monitor before we build new ones.
			int k = stages.length;
			while (--k>=0) {
				if (null!=stages[k]) {
		    		GraphManager.removePublishFromListener(graphManager, stages[k], didWorkMonitor);
				}
			}
			/////////////////////
    		
			result = new ScriptedNonThreadScheduler(graphManager, reverseOrder, resultStages);
			buildSchedule(graphManager, localStages, reverseOrder);
	        setupHousekeeping();

	        result.setupHousekeeping();
	        
			return result;
		} finally {
			modificationLock.unlock();
		}
	}

	//                                        ms  mi ns  must use longs!
	private final static long hangTimeNS = 1_000_000_000L * 60L * 60L * 2L;//2 hrs;
	public PronghornStage hungStage(long nowNS) {
		final long local = timeStartedRunningStage;
		if (((0!=timeStartedRunningStage) && (local>0)) 
			&& ((nowNS-timeStartedRunningStage)>hangTimeNS)) {
			//watches for dirty read
			if (local == timeStartedRunningStage) {
				return runningStage;
			}
		}
		return null;
	}

	public long hangTime(long nowNS) {
		if (0!=timeStartedRunningStage) {
			return nowNS-timeStartedRunningStage;
		} else {
			return 0;
		}
	}
	
	
}
