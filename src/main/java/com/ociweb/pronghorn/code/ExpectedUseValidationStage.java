package com.ociweb.pronghorn.code;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * _no-docs_
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class ExpectedUseValidationStage extends PronghornStage{

	private final Pipe[] inputs;
	private final Pipe[] outputs;
	private final GraphManager graphManager;
	private final GVSValidator validator;
	private boolean foundError = false;
	private final Logger logger = LoggerFactory.getLogger(ExpectedUseValidationStage.class);

	/**
	 *
	 * @param graphManager
	 * @param inputs _in_ Input pipes
	 * @param outputs _out_ Output pipes
	 * @param validator
	 */
	public ExpectedUseValidationStage(GraphManager graphManager, Pipe[] inputs, Pipe[] outputs, GVSValidator validator) {
		super(graphManager, inputs, outputs);
		this.inputs = inputs;
		this.outputs = outputs;
		this.graphManager = graphManager;
		this.validator = validator;
	}

	@Override
	public void run() {
		Object failureDetails = validator.validate(graphManager, inputs, outputs); 
		if (null!=failureDetails) {
			foundError = true;
			//force hard shut down of stage under test and generator
			GraphManager.terminateInputStages(graphManager);
			//force hard shut down of this stage
			GraphManager.setStateToShutdown(graphManager, stageId);	
			throw new AssertionError("Validation Failure: "+failureDetails);
		}
	}
	
	@Override
	public void shutdown() {
		logger.info("shutdown validator :{}",validator.status());
	}

	public boolean foundError() {
		return foundError;
	}

}
