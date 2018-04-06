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
				nextByte++;
			}
			
			data.set(nextByte, (byte)(data.get(nextByte) | ((x & 1) << nextBit)));
			nextBit++;
		}
		
		public void putBits(int x, int length) {
			for(int i = 0; i < length; ++i) {
				putBit(x);
				x >>= 1;
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
	
	Header header;
	MCU mcu = new MCU();
	
	int count;
	int numMCUs;

	static Short previousYDC = 0;
	static Short previousCbDC = 0;
	static Short previousCrDC = 0;

	static BitWriter b = new BitWriter();
	
	public HuffmanEncoder(GraphManager graphManager, Pipe<JPGSchema> input, boolean verbose) {
		super(graphManager, input, NONE);
		this.input = input;
		this.verbose = verbose;
	}
	
	private static int bitLength(int x) {
		return (int) Math.ceil(Math.log(x) / Math.log(2));
	}

	private static boolean encodeMCUComponent(
			  ArrayList<ArrayList<Integer>> DCTableCodes,
			  ArrayList<ArrayList<Integer>> ACTableCodes,
			  HuffmanTable DCTable,
			  HuffmanTable ACTable,
			  short[] component,
			  Short previousDC) {
		
		// code DC value
		int coeff = component[0] - previousDC;
		previousDC = (short) coeff;
		int coeffLength = bitLength(Math.abs(coeff));
		if (coeff <= 0) {
			coeff += (1 << coeffLength) - 1;
		}
		boolean found = false;
		for (int j = 0; j < 16; ++j) {
			for (int k = 0; k < DCTable.symbols.get(j).size(); ++k) {
				if (DCTable.symbols.get(j).get(k) == coeffLength) {
					int code = DCTableCodes.get(j).get(k);
					int codeLength = bitLength(code);
					b.putBits(code, codeLength);
					found = true;
					break;
				}
			}
			if (found) break;
		}
		if (!found) return false;
		
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
							int codeLength = bitLength(code);
							b.putBits(code, codeLength);
							return true;
						}
					}
				}
				return false;
			}
			
			while (numZeroes > 15) {
				b.putBits(15, 4);
				b.putBits(0, 4);
				numZeroes -= 15;
			}
			
			// find coeff length
			coeff = component[JPG.zigZagMap[i]];
			coeffLength = bitLength(Math.abs(coeff));
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
						int codeLength = bitLength(code);
						b.putBits(code, codeLength);
						b.putBits(coeff, coeffLength);
						found = true;
						break;
					}
				}
				if (found) break;
			}
			if (!found) return false;
		}
		return true;
	}


	public static void encodeHuffmanData(MCU mcu) {
		encodeMCUComponent(JPG.DCTableCodes0, JPG.ACTableCodes0, JPG.hDCTable0, JPG.hACTable0, mcu.y, previousYDC);

		encodeMCUComponent(JPG.DCTableCodes1, JPG.ACTableCodes1, JPG.hDCTable1, JPG.hACTable1, mcu.cb, previousCbDC);

		encodeMCUComponent(JPG.DCTableCodes1, JPG.ACTableCodes1, JPG.hDCTable1, JPG.hACTable1, mcu.cr, previousCrDC);
	}


	@Override
	public void run() {
		while (PipeReader.tryReadFragment(input)) {

			int msgIdx = PipeReader.getMsgIdx(input);

			if (msgIdx == JPGSchema.MSG_HEADERMESSAGE_1) {
				// read header from pipe
				header = new Header();
				header.height = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_HEIGHT_101);
				header.width = PipeReader.readInt(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_WIDTH_201);
				header.filename = PipeReader.readASCII(input, JPGSchema.MSG_HEADERMESSAGE_1_FIELD_FILENAME_301, new StringBuilder()).toString();
				PipeReader.releaseReadLock(input);
				
				count = 0;
				numMCUs = ((header.height + 7) / 8) * ((header.width + 7) / 8);
				previousYDC = 0;
				previousCbDC = 0;
				previousCrDC = 0;
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
					
					// temporary, remove later
					//b.putBits(0b11111011, 8);
					//header.height = 8;
					//header.width = 8;
					
					try {
						JPGDumper.Dump(b.data, header);
					}
					catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			}
			else {
				System.err.println("Huffman Encoder requesting shutdown");
				requestShutdown();
			}
		}
	}

}
