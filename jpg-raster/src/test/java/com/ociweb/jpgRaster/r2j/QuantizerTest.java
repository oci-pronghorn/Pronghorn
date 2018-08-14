package com.ociweb.jpgRaster.r2j;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.ociweb.jpgRaster.JPG;

public class QuantizerTest {
    private JPG.Header header;

    @Before
    public void initializeHeader() {
        // These tables are obtained from running djpeg on jpeg_test.jpg
        // from the test_jpeg folder.
        header = new JPG.Header();
        header.quantizationTables[0] = JPG.qTable0_50;
        header.quantizationTables[1] = JPG.qTable1_50;
        // Setup Color Components
        JPG.ColorComponent colorComponent;
        colorComponent = new JPG.ColorComponent();
        colorComponent.quantizationTableID = 0;
        header.colorComponents[0] = colorComponent;
        colorComponent = new JPG.ColorComponent();
        colorComponent.quantizationTableID = 1;
        header.colorComponents[1] = colorComponent;
        colorComponent = new JPG.ColorComponent();
        colorComponent.quantizationTableID = 1;
        header.colorComponents[2] = colorComponent;
        header.numComponents = 3;
    }
    
    private void quantizeCheck(JPG.MCU inputmcu, JPG.MCU outputmcu) {
    		for (int i = 0; i < 64; i++) {
        		assertTrue(outputmcu.y[JPG.zigZagMap[i]] ==
        				(inputmcu.y[JPG.zigZagMap[i]] / header.quantizationTables[0].table[i]));
        		assertTrue(outputmcu.cb[JPG.zigZagMap[i]] ==
        				(inputmcu.cb[JPG.zigZagMap[i]] / header.quantizationTables[1].table[i]));
        		assertTrue(outputmcu.cr[JPG.zigZagMap[i]] ==
        				(inputmcu.cr[JPG.zigZagMap[i]] / header.quantizationTables[1].table[i]));
    		}
    }

    @Test
    public void zeroDequantizeTest() {
        JPG.MCU inputmcu;
        JPG.MCU outputmcu;
        // Initialize input MCU
        inputmcu = new JPG.MCU();
        for (int i = 0; i < 64; i++) {
            inputmcu.y[i] = 0;
            inputmcu.cb[i] = 0;
            inputmcu.cr[i] = 0;
        }
        // Initialize output MCU
        outputmcu = new JPG.MCU();
        for (int i = 0; i < 64; i++) {
            outputmcu.y[i] = inputmcu.y[i];
            outputmcu.cb[i] = inputmcu.cb[i];
            outputmcu.cr[i] = inputmcu.cr[i];
        }
        // Call function
        QuantizerStage.quantize(outputmcu, 50);
        // Check output MCU against expected result
        quantizeCheck(inputmcu, outputmcu);
    }

    @Test
    public void randomDequantizeTest() {
        Random rand;
        JPG.MCU inputmcu;
        JPG.MCU outputmcu;
        // Initialize Random
        rand = new Random();
        // Initialize input MCU
        inputmcu = new JPG.MCU();
        for (int i = 0; i < 64; i++) {
            inputmcu.y[i] = (short) rand.nextInt(256);
            inputmcu.cb[i] = (short) rand.nextInt(256);
            inputmcu.cr[i] = (short) rand.nextInt(256);
        }
        // Initialize output MCU
        outputmcu = new JPG.MCU();
        for (int i = 0; i < 64; i++) {
            outputmcu.y[i] = inputmcu.y[i];
            outputmcu.cb[i] = inputmcu.cb[i];
            outputmcu.cr[i] = inputmcu.cr[i];
        }
        // Call function
        QuantizerStage.quantize(outputmcu, 50);
        // Check output MCU against expected result
        quantizeCheck(inputmcu, outputmcu);
    }
}
