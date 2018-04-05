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
		
//		private int[] masks = new int[]{1, 3, 7, 15, 31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383};
		
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
				x = x >> 1;
			}
		}
		
		public void printData() {
			for (int i = 0; i < data.size(); ++i) {
				System.out.print(String.format("%8s", Integer.toBinaryString(data.get(i) & 0xFF)).replace(" ", "0") + "  ");
			}
		}
		
		public void restart() {
			data.clear();
			nextByte = -1;
			nextBit = 8;
		}
	}
	
	public static void main(String[] args) {
//		System.out.print(String.format("%8s", Integer.toBinaryString(sizeCode(-7) & 0xFF)).replace(" ", "0") + "  ");
	}
	
	private final Pipe<JPGSchema> input;
	boolean verbose;
	
	Header header;
	MCU mcu = new MCU();
	
	int count;
	int mcuHeight;
	int mcuWidth;
	int numMCUs;
	
	public HuffmanEncoder(GraphManager graphManager, Pipe<JPGSchema> input, boolean verbose) {
		super(graphManager, input, NONE);
		this.input = input;
		this.verbose = verbose;
	}

	static short previousYDC = 0;
	static short previousCbDC = 0;
	static short previousCrDC = 0;

	static BitWriter b = new BitWriter();
	
	/*private static int sizeCode(int x) {
		if ( x < 0) {
			int length = (int) Math.ceil(Math.log(x) / Math.log(2));
			int y = 2147483647;
			x = 0 - x;
			return (x^y);
		}
		return x;
	}*/
	
	private static int bitLength(int x) {
		return (int) Math.ceil(Math.log(x) / Math.log(2));
	}

	private static boolean encodeMCUComponent(
			  ArrayList<ArrayList<Integer>> DCTableCodes,
			  ArrayList<ArrayList<Integer>> ACTableCodes,
			  HuffmanTable DCTable,
			  HuffmanTable ACTable,
			  short[] component,
			  short previousDC) {
		
		boolean broken = false;
		for (int j = 63; j > 0; --j) {
			int numZeroes = 0;
			if (component[j] == 0) {
				++numZeroes;
				continue;
			}
			if (numZeroes > 15) {
				System.out.println("Huffman Error creating zero run length");
				broken = true;
				break;
			}
			
			component[j] -= previousDC;
			int length = bitLength(component[j]);
			if (component[j] < 0) {
				component[j] = (short)((0 - component[j])^0xFFFF);
			}
			
			for (int i = 0; i < DCTable.symbols.get(length).size(); ++i) {
				if (component[j] == DCTable.symbols.get(length).get(i)) {
					b.putBits(component[j], length);
					b.putBits(DCTableCodes.get(length).get(i), bitLength(DCTableCodes.get(length).get(i)));
					b.putBits(numZeroes, 4);
					break;
				}
			}
		}
		if (broken) {
			return false;
		}
		
		component[0] -= previousDC;
		int length = bitLength(component[0]);
		if ( component[0] < 0) {
			component[0] = (short)((0 - component[0])^32767);
		}
		
		for (int i = 0; i < DCTable.symbols.get(length).size(); ++i) {
			if (component[0] == DCTable.symbols.get(length).get(i)) {
				b.putBits(component[0], length);
				b.putBits(DCTableCodes.get(length).get(i), bitLength(DCTableCodes.get(length).get(i)));
				break;
			}
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
				mcuHeight = (header.height + 7) / 8;
				mcuWidth = (header.width + 7) / 8;
				numMCUs = mcuHeight * mcuWidth;
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
					b.putBits(0b11111011, 8);
					header.height = 8;
					header.width = 8;
					
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
