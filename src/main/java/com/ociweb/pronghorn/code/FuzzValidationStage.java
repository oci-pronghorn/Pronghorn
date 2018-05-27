package com.ociweb.pronghorn.code;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.stream.StreamingReadVisitor;
import com.ociweb.pronghorn.pipe.stream.StreamingVisitorReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/**
 * Validates previously generated fuzz.
 * @author Nathan Tippy
 * @see <a href="https://github.com/objectcomputing/Pronghorn">Pronghorn</a>
 */
public class FuzzValidationStage extends PronghornStage{

	private final StreamingVisitorReader reader;
	private boolean foundError = false;
	private static final Logger log = LoggerFactory.getLogger(FuzzValidationStage.class);

	/**
	 *
	 * @param graphManager
	 * @param input _in_ Pipe containing fuzz.
	 */
	public FuzzValidationStage(GraphManager graphManager, Pipe<?> input) {
		super(graphManager, input, NONE);

		StreamingReadVisitor visitor = buildVisitor(Pipe.from(input));
		
        boolean processUTF8 = true;
		reader = new StreamingVisitorReader(input, visitor,processUTF8);//, new StreamingReadVisitorDebugDelegate(visitor) );
		
	}

	private StreamingReadVisitor buildVisitor(FieldReferenceOffsetManager from) {
		return new StreamingReadVisitor() {

			StringBuilder temp = new StringBuilder();
			
			@Override
			public boolean paused() {
				return false;
			}

			//TODO: B, for each field type need to confirm them against the expected ranges & behavior in from
			
			@Override
			public void visitTemplateOpen(String name, long id) {				
			}

			@Override
			public void visitTemplateClose(String name, long id) {				
			}

			@Override
			public void visitFragmentOpen(String name, long id, int cursor) {
			}

			@Override
			public void visitFragmentClose(String name, long id) {
			}

			@Override
			public void visitSequenceOpen(String name, long id, int length) {
			}

			@Override
			public void visitSequenceClose(String name, long id) {
			}

			@Override
			public void visitSignedInteger(String name, long id, int value) {
			}

			@Override
			public void visitUnsignedInteger(String name, long id, long value) {
			}

			@Override
			public void visitSignedLong(String name, long id, long value) {
			}

			@Override
			public void visitUnsignedLong(String name, long id, long value) {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void visitDecimal(String name, long id, int exp, long mant) {
			}

			@Override
			public Appendable targetASCII(String name, long id) {
				temp.setLength(0);;
				return temp;
			}

			@Override
			public Appendable targetUTF8(String name, long id) {
				temp.setLength(0);
				return temp;
			}

			@Override
			public ByteBuffer targetBytes(String name, long id, int length) {
				return length<0? null : ByteBuffer.allocate(length);
			}

			@Override
			public void visitBytes(String name, long id, ByteBuffer value) {
			}

			@Override
			public void startup() {
			}

			@Override
			public void shutdown() {
			}

            @Override
            public void visitASCII(String name, long id, CharSequence value) {
                // TODO Auto-generated method stub
                
            }

            @Override
            public void visitUTF8(String name, long id, CharSequence target) {
            	
            	boolean found = StageTester.hasBadChar(target);
            	
            	if (found) {
            		log.info("unconvertable values found in field {} {}",name,id);
            	}
            }
            
			
		};
	}

    @Override
    public void startup() {
    	reader.startup();
    }
    
    @Override
    public void run() {
    	reader.run();
    	
    	//TODO: C, need to set foundError for bad data on output ring.
    	
    }
    
    @Override
    public void shutdown() {
    	reader.shutdown();
    }

	public boolean foundError() {
		return foundError;
	}

}
