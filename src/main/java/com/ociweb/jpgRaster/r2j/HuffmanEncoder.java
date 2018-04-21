package com.ociweb.jpgRaster.r2j;

import java.io.IOException;
import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG;
import com.ociweb.jpgRaster.JPGSchema;
import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.HuffmanTable;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class HuffmanEncoder extends PronghornStage {
	
	private static class BitWriter {
		private int nextByte = -1;
		private int nextBit = 8;
		public ArrayList<Byte> data = new ArrayList<Byte>();
		
		public void putBit(int x) {
			if (nextBit > 7) {
				data.add((byte) 0);
				nextBit = 0;
				if (data.size() > 1 && data.get(nextByte) == (byte)0xFF) {
					data.add((byte) 0);
					nextByte++;
				}
				nextByte++;
			}
			
			data.set(nextByte, (byte)(data.get(nextByte) | ((x & 1) << (7 - nextBit))));
			nextBit++;
		}
		
		public void putBits(int x, int length) {
			for(int i = 0; i < length; ++i) {
				putBit(x >> (length - 1 - i));
			}
		}
		
		public void restart() {
			data.clear();
			nextByte = -1;
			nextBit = 8;
		}
	}
	
	private final Pipe<JPGSchema> input;
	boolean verbose;
	boolean time;
	public static long timer = 0;
	long start;
	int quality;
	
	Header header;
	int last = 0;
	MCU mcu = new MCU();
	
	int count;
	int numMCUs;

	static short[] previousDC = new short[3];

	static BitWriter b = new BitWriter();
	
	public HuffmanEncoder(GraphManager graphManager, Pipe<JPGSchema> input, boolean verbose, boolean time, int quality) {
		super(graphManager, input, NONE);
		this.input = input;
		this.verbose = verbose;
		this.time = time;
		this.quality = quality;
		start = System.nanoTime();
	}
	
	private static int bitLength(int x) {
		int len = 0;
		while (x > 0) {
			x >>= 1;
			++len;
		}
		return len;
	}

	private boolean encodeMCUComponent(
			  ArrayList<ArrayList<Integer>> DCTableCodes,
			  ArrayList<ArrayList<Integer>> ACTableCodes,
			  HuffmanTable DCTable,
			  HuffmanTable ACTable,
			  short[] component,
			  int compID) {
		
		// code DC value
		int coeff = component[0] - previousDC[compID];
		previousDC[compID] = component[0];
		int coeffLength = (coeff == 0 ? 0 : bitLength(Math.abs(coeff)));
		if (coeffLength > 11) {
			System.err.println("Error - coeffLength > 11 : " + coeffLength);
			return false;
		}
		if (coeff <= 0) {
			coeff += (1 << coeffLength) - 1;
		}
		boolean found = false;
		for (int j = 0; j < 16; ++j) {
			for (int k = 0; k < DCTable.symbols.get(j).size(); ++k) {
				if (DCTable.symbols.get(j).get(k) == coeffLength) {
					int code = DCTableCodes.get(j).get(k);
					b.putBits(code, j+1);
					b.putBits(coeff, coeffLength);
					found = true;
					break;
				}
			}
			if (found) break;
		}
		
		// code AC values
		for (int i = 1; i < 64; ++i) {
			// find zero run length
			int numZeroes = 0;
			while (i < 64 && component[JPG.zigZagMap[i]] == 0) {
				++numZeroes;
				++i;
			}
			
			if (i == 64) {
				// write terminator code
				for (int j = 0; j < 16; ++j) {
					// find code that gives the symbol 0
					for (int k = 0; k < ACTable.symbols.get(j).size(); ++k) {
						if (ACTable.symbols.get(j).get(k) == 0) {
							int code = ACTableCodes.get(j).get(k);
							b.putBits(code, j+1);
							return true;
						}
					}
				}
				return false; // never going to happen
			}
			
			while (numZeroes >= 16) {
				found = false;
				for (int j = 0; j < 16; ++j) {
					for (int k = 0; k < ACTable.symbols.get(j).size(); ++k) {
						if (ACTable.symbols.get(j).get(k) == 0xF0) {
							int code = ACTableCodes.get(j).get(k);
							b.putBits(code, j+1);
							found = true;
							break;
						}
					}
					if (found) break;
				}
				numZeroes -= 16;
			}
			
			// find coeff length
			coeff = component[JPG.zigZagMap[i]];
			coeffLength = bitLength(Math.abs(coeff));
			if (coeffLength > 10) {
				System.err.println("Error - coeffLength > 10 : " + coeffLength);
				return false;
			}
			if (coeff <= 0) {
				coeff += (1 << coeffLength) - 1;
			}
			// find symbol in table
			int symbol = numZeroes << 4 | coeffLength;
			found = false;
			for (int j = 0; j < 16; ++j) {
				for (int k = 0; k < ACTable.symbols.get(j).size(); ++k) {
					if (ACTable.symbols.get(j).get(k) == symbol) {
						int code = ACTableCodes.get(j).get(k);
						b.putBits(code, j+1);
						b.putBits(coeff, coeffLength);
						found = true;
						break;
					}
				}
				if (found) break;
			}
		}
		return true;
	}


	public void encodeHuffmanData(MCU mcu) {
		if (!encodeMCUComponent(JPG.DCTableCodes0, JPG.ACTableCodes0, JPG.hDCTable0, JPG.hACTable0, mcu.y, 0)) {
			System.err.println("Error during Y component Huffman coding");
		}
		if (!encodeMCUComponent(JPG.DCTableCodes1, JPG.ACTableCodes1, JPG.hDCTable1, JPG.hACTable1, mcu.cb, 1)) {
			System.err.println("Error during Cb component Huffman coding");
		}
		if (!encodeMCUComponent(JPG.DCTableCodes1, JPG.ACTableCodes1, JPG.hDCTable1, JPG.hACTable1, mcu.cr, 2)) {
			System.err.println("Error during Cr component Huffman coding");
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
						System.out.println("Time for BMPScanner: " + ((double)(BMPScanner.timer) / 1000000) + " ms");
						System.out.println("Time for RGBToYCbCr: " + ((double)(RGBToYCbCr.timer) / 1000000) + " ms");
						System.out.println("Time for ForwardDCT: " + ((double)(ForwardDCT.timer) / 1000000) + " ms");
						System.out.println("Time for Quantizer: " + ((double)(Quantizer.timer) / 1000000) + " ms");
						System.out.println("Time for JPGDumper/HuffmanEncoder: " + ((double)(timer) / 1000000) + " ms");
						System.out.println("Total time: " + ((double)(System.nanoTime() - start) / 1000000) + " ms");
					}
					
					System.exit(0);
				}
				
				count = 0;
				numMCUs = ((header.height + 7) / 8) * ((header.width + 7) / 8);
				previousDC[0] = 0;
				previousDC[1] = 0;
				previousDC[2] = 0;
				b.restart();
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

				encodeHuffmanData(mcu);
				
				count += 1;
				if (count >= numMCUs) {
					try {
						JPGDumper.Dump(b.data, header, verbose, quality);
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
					if (last == 1) {
						if (time) {
							timer += (System.nanoTime() - s);
							System.out.println("Time for BMPScanner: " + ((double)(BMPScanner.timer) / 1000000) + " ms");
							System.out.println("Time for RGBToYCbCr: " + ((double)(RGBToYCbCr.timer) / 1000000) + " ms");
							System.out.println("Time for ForwardDCT: " + ((double)(ForwardDCT.timer) / 1000000) + " ms");
							System.out.println("Time for Quantizer: " + ((double)(Quantizer.timer) / 1000000) + " ms");
							System.out.println("Time for JPGDumper/HuffmanEncoder: " + ((double)(timer) / 1000000) + " ms");
							System.out.println("Total time: " + ((double)(System.nanoTime() - start) / 1000000) + " ms");
						}
						
						System.exit(0);
					}
				}
			}
			else {
				System.err.println("Huffman Encoder requesting shutdown");
				requestShutdown();
			}
		}
		timer += (System.nanoTime() - s);
	}

}
