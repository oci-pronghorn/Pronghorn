package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPG.QuantizationTable;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class InverseQuantizer extends PronghornStage {

	private final Pipe<JPGSchema> input;
	private final Pipe<JPGSchema> output;
	
	
	protected InverseQuantizer(GraphManager graphManager, Pipe<JPGSchema> input, Pipe<JPGSchema> output) {
		super(graphManager, input, output);
		this.input = input;
		this.output = output;
	}
	
	private static void dequantizeMCU(short[] MCU, QuantizationTable table) {
		/*System.out.print("Before Inverse Quantization:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(MCU[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
		
		for (int i = 0; i < MCU.length; ++i) {
			// type casting is unsafe for 16-bit precision quantization tables
			MCU[i] = (short)(MCU[i] * table.table[i]);
		}
		
		/*System.out.print("After Inverse Quantization:");
		for (int i = 0; i < 8; ++i) {
			for (int j = 0; j < 8; ++j) {
				if (j % 8 == 0) {
					System.out.println();
				}
				System.out.print(MCU[i * 8 + j] + " ");
			}
		}
		System.out.println();*/
	}
	
	public static void dequantize(ArrayList<MCU> mcus, Header header) {
		for (int i = 0; i < mcus.size(); ++i) {
			dequantizeMCU(mcus.get(i).y, header.quantizationTables.get(header.colorComponents.get(0).quantizationTableID));
			dequantizeMCU(mcus.get(i).cb, header.quantizationTables.get(header.colorComponents.get(1).quantizationTableID));
			dequantizeMCU(mcus.get(i).cr, header.quantizationTables.get(header.colorComponents.get(2).quantizationTableID));
		}
		return;
	}

	@Override
	public void run() {
		
	}
}
