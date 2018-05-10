package com.ociweb.jpgRaster.r2j;

import com.ociweb.jpgRaster.JPG.Header;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

/*
 * Updated Forward DCT Algorithm is based on the Guetzli JPEG encoder's
 * DCT implementation. This code can be found here:
 * 	https://github.com/google/guetzli/blob/master/guetzli/dct_double.cc
 */
public class ForwardDCTStage extends PronghornStage {

	private static final Logger logger = LoggerFactory.getLogger(ForwardDCTStage.class);
			
	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	private boolean verbose;

	private Header header;
	private MCU mcu;
	private double[] temp;
	private double[] fdctMap;
	
	public ForwardDCTStage(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output, boolean verbose) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
		this.verbose = verbose;
	}

	@Override
	public void startup() {
		mcu = new MCU();
		temp = new double[64];
		fdctMap = new double[64];
		// prepare fdctMap
		for (int u = 0; u < 8; ++u) {
			double c = 1.0 / 2.0;
			if (u == 0) {
				c = 1 / Math.sqrt(2.0) / 2.0;
			}
			for (int x = 0; x < 8; ++x) {
				fdctMap[u * 8 + x] = c * Math.cos((2.0 * x + 1.0) * u * Math.PI / 16.0);
			}
		}
	}
	
	
	private void TransformColumn(short[] in, double[] out, int offset) {
		double temp;
		for (int y = 0; y < 8; ++y) {
			temp = 0;
			for (int v = 0; v < 8; ++v) {
				temp += in[v * 8 + offset] * fdctMap[8 * y + v];
			}
			out[y * 8 + offset] = temp;
		}
	}
	
	private void TransformRow(double[] in, short[] out, int offset) {
		double temp;
		for (int x = 0; x < 8; ++x) {
			temp = 0;
			for (int u = 0; u < 8; ++u) {
				temp += in[u + offset * 8] * fdctMap[8 * x + u];
			}
			out[x + offset * 8] = (short) temp;
		}
	}
	
	private void TransformBlock(short[] mcu) {
		for (int i = 0; i < 8; ++i) {
			TransformColumn(mcu, temp, i);
		}
		for (int j = 0; j < 8; ++j) {
			TransformRow(temp, mcu, j);
		}
	}
	
	private void inverseDCT(MCU mcu, Header header) {
		TransformBlock(mcu.y);
		TransformBlock(mcu.cb);
		TransformBlock(mcu.cr);
		return;
	}

	@Override
	public void run() {
		long s = System.nanoTime();
		while (PipeWriter.hasRoomForWrite(output) && PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				int last = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401);
				PipeReader.releaseReadLock(input);

				// write header to pipe
				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_HEADERMESSAGE_1)) {
					if (verbose) 
						System.out.println("Forward DCT writing header to pipe...");
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101, header.height);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201, header.width);
					PipeWriter.writeASCII(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, header.filename);
					PipeWriter.writeInt(output, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401, last);
					PipeWriter.publishWrites(output);
				}
				else {
					logger.error("Forward DCT requesting shutdown");
					requestShutdown();
				}
			}
			else if (msgIdx == JPGSchema.MSG_MCUMESSAGE_4) {
				DataInputBlobReader<JPGSchema> mcuReader = PipeReader.inputStream(input, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
				for (int i = 0; i < 64; ++i) {
					mcu.y[i] = mcuReader.readShort();
				}
				
				for (int i = 0; i < 64; ++i) {
					mcu.cb[i] = mcuReader.readShort();
				}
				
				for (int i = 0; i < 64; ++i) {
					mcu.cr[i] = mcuReader.readShort();
				}
				PipeReader.releaseReadLock(input);
				
				inverseDCT(mcu, header);
				//JPG.printMCU(mcu);

				if (PipeWriter.tryWriteFragment(output, JPGSchema.MSG_MCUMESSAGE_4)) {
					DataOutputBlobWriter<JPGSchema> mcuWriter = PipeWriter.outputStream(output);
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.y[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_Y_104);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cb[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CB_204);
					
					DataOutputBlobWriter.openField(mcuWriter);
					for (int i = 0; i < 64; ++i) {
						mcuWriter.writeShort(mcu.cr[i]);
					}
					DataOutputBlobWriter.closeHighLevelField(mcuWriter, JPGSchema.MSG_MCUMESSAGE_4_FIELD_CR_304);
					
					PipeWriter.publishWrites(output);
				}
				else {
					logger.error("Forward DCT requesting shutdown");
					requestShutdown();
				}
			}
			else {
				logger.error("Forward DCT requesting shutdown");
				requestShutdown();
			}
		}
		timer.addAndGet(System.nanoTime() - s);
	}
	
	public static AtomicLong timer = new AtomicLong(0);//NOTE: using statics like this is not recommended
	
	
}
