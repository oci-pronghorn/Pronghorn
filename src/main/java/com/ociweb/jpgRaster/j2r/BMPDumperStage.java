package com.ociweb.jpgRaster.j2r;

import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.ColorComponent;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dumps the BMP raster of a JPG Schema into a file
 */
public class BMPDumperStage extends PronghornStage {

	private static final Logger logger = LoggerFactory.getLogger(BMPDumperStage.class);
	
	private final Pipe<JPGSchema> input;
	boolean verbose;
	boolean time;
	private long timer = 0;
	long start;
	
	Header header;
	int last = 0;
	MCU mcu;
	
	short[][] pixels;
	int count;
	int mcuHeight;
	int mcuWidth;
	int numMCUs;
	int mcuHeightReal;
	int mcuWidthReal;
	int numMCUsReal;
	int pos;

	/**
	 * Takes a JPG schema and allows for verbose output with time.
	 * @param graphManager
	 * @param input _in_ Input pipe to be dumped
	 * @param verbose
	 * @param time
	 */
	public BMPDumperStage(GraphManager graphManager, Pipe<JPGSchema> input, boolean verbose, boolean time) {
		super(graphManager, input, NONE);
		this.input = input;
		this.verbose = verbose;
		this.time = time;
		start = System.nanoTime();

		GraphManager.addNota(graphManager, GraphManager.DOT_BACKGROUND, "lemonchiffon3", this);
	}

	@Override
	public void startup() {
		mcu = new MCU();
	}
	
	private static void dump(short[][] pixels, String filename) throws IOException {
		int width = pixels[0].length / 3;
		int height = pixels.length;
		int paddingSize = (4 - (width * 3) % 4) % 4;
		int size = 14 + 12 + pixels.length * pixels[0].length + height * paddingSize;
		
		FileOutputStream fileStream = new FileOutputStream(filename);
		FileChannel file = fileStream.getChannel();
		
		ByteBuffer buffer = ByteBuffer.allocate(size);
		buffer.put((byte) 'B');
		buffer.put((byte) 'M');
		putInt(buffer, size);
		putInt(buffer, 0);
		putInt(buffer, 0x1A);
		putInt(buffer, 12);
		putShort(buffer, width);
		putShort(buffer, height);
		putShort(buffer, 1);
		putShort(buffer, 24);
		
		for (int i = height - 1; i >= 0; --i) {
			for (int j = 0; j < width * 3 - 2; j += 3) {
				buffer.put((byte)(pixels[i][j + 2] & 0xFF));
				buffer.put((byte)(pixels[i][j + 1] & 0xFF));
				buffer.put((byte)(pixels[i][j + 0] & 0xFF));
			}
			for (int j = 0; j < paddingSize; j++) {
				buffer.put((byte)0);
			}
		}
		buffer.flip();
		while(buffer.hasRemaining()) {
			file.write(buffer);
		}
		file.close();
		fileStream.close();
	}
	
	private static void putInt(ByteBuffer buffer, int v) {
		buffer.put((byte)(v & 0xFF));
		buffer.put((byte)((v >> 8) & 0xFF));
		buffer.put((byte)((v >> 16) & 0xFF));
		buffer.put((byte)((v >> 24) & 0xFF));
	}
	
	private static void putShort(ByteBuffer buffer, int v) {
		buffer.put((byte)(v & 0xFF));
		buffer.put((byte)((v >> 8) & 0xFF));
	}
	
	private void copyPixels(int mcuNum) {
		int curPixelY = (mcuNum / mcuWidth) * 8;
		int curPixelX = (mcuNum % mcuWidth) * 8;
		for (int i = curPixelY; i < curPixelY + 8; ++i) {
			for (int j = curPixelX; j < curPixelX + 8; ++j) {
				if (i < header.height && j < header.width) {
					pixels[i][j * 3 + 0] = mcu.y[(i % 8) * 8 + (j % 8)];
					pixels[i][j * 3 + 1] = mcu.cb[(i % 8) * 8 + (j % 8)];
					pixels[i][j * 3 + 2] = mcu.cr[(i % 8) * 8 + (j % 8)];
				}
			}
		}
	}

	@Override
	public void run() {
		long s = System.nanoTime();
		while (PipeReader.tryReadFragment(input)) {
			
			int msgIdx = PipeReader.getMsgIdx(input);
			
			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				last = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FINAL_401);
				PipeReader.releaseReadLock(input);
				
				if (last == 1 && header.height == 0 && header.width == 0) {
					if (time) {
						timer += (System.nanoTime() - s);
						System.out.println("Time for JPGScanner/HuffmanDecoder: " + ((double)(JPGScannerStage.timer.get()) / 1000000) + " ms");
						System.out.println("Time for InverseQuantizer: " + ((double)(InverseQuantizerStage.timer.get()) / 1000000) + " ms");
						System.out.println("Time for InverseDCT: " + ((double)(InverseDCTStage.timer.get()) / 1000000) + " ms");
						System.out.println("Time for YCbCrToRGB: " + ((double)(YCbCrToRGBStage.timer.get()) / 1000000) + " ms");
						System.out.println("Time for BMPDumper: " + ((double)(timer) / 1000000) + " ms");
						System.out.println("Total time: " + ((double)(System.nanoTime() - start) / 1000000) + " ms");
					}
					
					System.exit(0);
				}

				pixels = new short[header.height][header.width * 3];
				count = 0;
				mcuHeight = mcuHeightReal = (header.height + 7) / 8;
				mcuWidth = mcuWidthReal = (header.width + 7) / 8;
				numMCUs = numMCUsReal = mcuHeight * mcuWidth;
				pos = 0;
			}
			else if (msgIdx == JPGSchema.MSG_COLORCOMPONENTMESSAGE_2) {
				// read color component data from pipe
				ColorComponent component = new ColorComponent();
				component.componentID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_COMPONENTID_102);
				component.horizontalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_HORIZONTALSAMPLINGFACTOR_202);
				component.verticalSamplingFactor = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_VERTICALSAMPLINGFACTOR_302);
				component.quantizationTableID = (short) PipeReader.readInt(input, JPGSchema.MSG_COLORCOMPONENTMESSAGE_2_FIELD_QUANTIZATIONTABLEID_402);
				header.colorComponents[component.componentID - 1] = component;
				header.numComponents += 1;
				PipeReader.releaseReadLock(input);
				if (component.componentID == 1) {
					if (header.colorComponents[0].horizontalSamplingFactor == 2 &&
						mcuWidth % 2 == 1) {
						mcuWidth += 1;
					}
					if (header.colorComponents[0].verticalSamplingFactor == 2 &&
						mcuHeight % 2 == 1) {
						mcuHeight += 1;
					}
					numMCUs = mcuHeight * mcuWidth;
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

				if (mcuHeightReal < mcuHeight && pos / mcuWidth == mcuHeightReal ||
					mcuWidthReal < mcuWidth && pos % mcuWidth == mcuWidthReal) {}
				else {
					copyPixels(pos);
				}
				
				count += 1;
				
				if (header.colorComponents[0].verticalSamplingFactor == 2 &&
					mcuHeight > 1) {
					if (pos % (mcuWidth * 2) == mcuWidth * 2 - 1) {
						pos += 1;
					}
					else if (pos < count) {
						pos += mcuWidth;
					}
					else {
						pos -= mcuWidth - 1;
					}
				}
				else {
					pos = count;
				}
				
				if (count >= numMCUs) {
					try {
						int extension = header.filename.lastIndexOf('.');
						if (extension == -1) {
							header.filename += ".bmp";
						}
						else {
							header.filename = header.filename.substring(0, extension) + ".bmp";
						}
						if (verbose) {
							System.out.println("Writing to '" + header.filename + "'...");
						}
						dump(pixels, header.filename);
						if (verbose) {
							System.out.println("Done.");
						}
						if (last == 1) {
							if (time) {
								timer += (System.nanoTime() - s);
								System.out.println("Time for JPGScanner/HuffmanDecoder: " + ((double)(JPGScannerStage.timer.get()) / 1000000) + " ms");
								System.out.println("Time for InverseQuantizer: " + ((double)(InverseQuantizerStage.timer.get()) / 1000000) + " ms");
								System.out.println("Time for InverseDCT: " + ((double)(InverseDCTStage.timer.get()) / 1000000) + " ms");
								System.out.println("Time for YCbCrToRGB: " + ((double)(YCbCrToRGBStage.timer.get()) / 1000000) + " ms");
								System.out.println("Time for BMPDumper: " + ((double)(timer) / 1000000) + " ms");
								System.out.println("Total time: " + ((double)(System.nanoTime() - start) / 1000000) + " ms");
							}
							
							System.exit(0);
						}
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			else {
				logger.error("BMPDumper requesting shutdown");
				requestShutdown();
			}
		}
		timer += (System.nanoTime() - s);
	
	}
}
