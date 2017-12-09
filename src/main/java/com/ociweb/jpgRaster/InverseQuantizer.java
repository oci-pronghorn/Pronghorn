package com.ociweb.jpgRaster;

import static java.lang.Math.sqrt;

import java.util.ArrayList;

import com.ociweb.jpgRaster.JPG.Header;
import com.ociweb.jpgRaster.JPG.MCU;
import com.ociweb.jpgRaster.JPG.QuantizationTable;

public class InverseQuantizer {
    public static void dequantize(short[] MCU, QuantizationTable table) {
        for (int i = 0; i < MCU.length; ++i) {
        	// type casting is unsafe for 16-bit precision quantization tables
            MCU[i] = (short)(MCU[i] * table.table[i]);
        }
    }

    public static int[][] reverseZigZag(int[] inputArr, int length) {
        final int EAST = 0;
        final int SOUTH = 1;
        final int SOUTHWEST = 2;
        final int NORTHEAST = 3;
        double rawSideLength = sqrt(length);
        int sideLength = (int) rawSideLength;
        if (sideLength != rawSideLength) {
            sideLength += 1;
        }
        int[][] result = new int[sideLength][sideLength];
        int insert_i = 0;
        int insert_j = 0;
        int direction = SOUTHWEST;
        for (int i = 0; i < length; ++i) {
            result[insert_i][insert_j] = inputArr[i];
            switch (direction) {
                case SOUTHWEST:
                    if (insert_j == sideLength - 1) {
                        insert_i++;
                        direction = NORTHEAST;
                    } else if (insert_i == 0) {
                        insert_j++;
                        direction = NORTHEAST;
                    } else {
                        insert_i--;
                        insert_j++;
                    }
                    break;
                case NORTHEAST:
                    if (insert_i == sideLength - 1) {
                        insert_j++;
                        direction = SOUTHWEST;
                    } else if (insert_j == 0) {
                        insert_i++;
                        direction = SOUTHWEST;
                    } else {
                        insert_i++;
                        insert_j--;
                    }
                    break;
                default:
                    break;
            }
        }
        return result;
    }
    
    public static void Dequantize(ArrayList<MCU> mcus, Header header) {
		for (int i = 0; i < mcus.size(); ++i) {
			dequantize(mcus.get(i).yAc, header.quantizationTables.get(0));
			dequantize(mcus.get(i).cbAc, header.quantizationTables.get(0));
			dequantize(mcus.get(i).crAc, header.quantizationTables.get(0));
		}
		return;
    }
}
