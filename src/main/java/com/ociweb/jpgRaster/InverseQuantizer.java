package com.ociweb.jpgRaster;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPG.QuantizationTable;

public class InverseQuantizer {
	public static void dequantizeMCU(short[] MCU, QuantizationTable table) {
		for (int i = 0; i < MCU.length; ++i) {
			// type casting is unsafe for 16-bit precision quantization tables
			MCU[i] = (short)(MCU[i] * table.table[i]);
		}
	}
	
	public static void dequantize(ArrayList<MCU> mcus, Header header) {
		for (int i = 0; i < mcus.size(); ++i) {
			dequantizeMCU(mcus.get(i).y, header.quantizationTables.get(header.colorComponents.get(0).quantizationTableID));
			dequantizeMCU(mcus.get(i).cb, header.quantizationTables.get(header.colorComponents.get(1).quantizationTableID));
			dequantizeMCU(mcus.get(i).cr, header.quantizationTables.get(header.colorComponents.get(2).quantizationTableID));
		}
		return;
	}
}
