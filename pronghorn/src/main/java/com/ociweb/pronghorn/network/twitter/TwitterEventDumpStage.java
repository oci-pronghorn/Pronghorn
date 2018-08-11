package com.ociweb.pronghorn.network.twitter;

import java.io.IOException;

import com.ociweb.pronghorn.network.schema.TwitterEventSchema;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.util.Appendables;

/**
 * _no-docs_
 */
public class TwitterEventDumpStage extends PronghornStage {

	private final Pipe<TwitterEventSchema> input;
	private final Appendable target;
	
	public TwitterEventDumpStage(GraphManager graphManager,
								 Appendable target,
			                     Pipe<TwitterEventSchema> input) {
		super(graphManager, input, NONE);
		this.target = target;
		this.input = input;
	}

	@Override
	public void run() {
		
		while (PipeReader.tryReadFragment(input)) {
			try {
				target.append("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n");

				long id = PipeReader.readLong(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_USERID_51);
				Appendables.appendValue(target, "id:",id);
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_NAME_52, target);
				target.append('\n');
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_SCREENNAME_53, target);
				target.append('\n');			
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_CREATEDAT_57, target);
				target.append('\n');
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_DESCRIPTION_58, target);
				target.append('\n');
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_LANGUAGE_60, target);
				target.append('\n');
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_TIMEZONE_61, target);
				target.append('\n');
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_LOCATION_62, target);
				target.append('\n');
				
				PipeReader.readUTF8(input, TwitterEventSchema.MSG_USERPOST_101_FIELD_TEXT_22, target);
				target.append('\n');
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			PipeReader.releaseReadLock(input);
		}
	}

}
