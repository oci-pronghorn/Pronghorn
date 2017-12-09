package com.ociweb.jpgRaster;

import static java.lang.Math.sqrt;

public class InverseQuantizer {
    int quantizationTable[];

    // TODO populate/read in quantization table
    void readTable(int[] inputArr, int length) {
        quantizationTable = new int[length];
        for (int i = 0; i < length; ++i) {
            quantizationTable[i] = inputArr[i];
        }
    }

    void dequantize(int[][][] MCU) {
        for (int i = 0; i < MCU.length; ++i) {
            for (int j = 0; j < MCU[0].length; ++j) {
                for (int k = 0; k < MCU[0][0].length; ++k) {
                    MCU[i][j][k] = MCU[i][j][k] * quantizationTable[k];
                }
            }
        }
    }

    int[][] reverseZigZag(int[] inputArr, int length) {
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
}
