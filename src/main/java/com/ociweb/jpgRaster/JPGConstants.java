package com.ociweb.jpgRaster;

public class JPGConstants {
    // Start of Frame markers, non-differential, Huffman coding
    public static final int SOF0 = 0xFFC0; // Baseline DCT
    public static final int SOF1 = 0xFFC1; // Extended sequential DCT
    public static final int SOF2 = 0xFFC2; // Progressive DCT
    public static final int SOF3 = 0xFFC3; // Lossless (sequential)

    // Start of Frame markers, differential, Huffman coding
    public static final int SOF5 = 0xFFC5; // Differential sequential DCT
    public static final int SOF6 = 0xFFC6; // Differential progressive DCT
    public static final int SOF7 = 0xFFC7; // Differential lossless (sequential)

    // Huffman Table specification
    public static final int DHT = 0xFFC4; // Define Huffman Table(s)

    // Start of Frame markers, non-differential, arithmetic coding
    public static final int JPG = 0xFFC8; // Reserved for JPEG extensions
    public static final int SOF9 = 0xFFC9; // Extended sequential DCT
    public static final int SOF10 = 0xFFCA; // Progressive DCT
    public static final int SOF11 = 0xFFCB; // Lossless (sequential)

    // Start of Frame markers, differential, arithmetic coding
    public static final int SOF13 = 0xFFCD; // Differential sequential DCT
    public static final int SOF14 = 0xFFCE; // Differential progressive DCT
    public static final int SOF15 = 0xFFCF; // Differential lossless (sequential)

    // Arithmetic coding conditioning specification
    public static final int DAC = 0xFFCC; // Define Arithmetic Coding Conditioning(s)

    // Restart interval termination
    public static final int RST0 = 0xFFD0; // Restart interval modulo 8 count 0
    public static final int RST1 = 0xFFD0; // Restart interval modulo 8 count 1
    public static final int RST2 = 0xFFD0; // Restart interval modulo 8 count 2
    public static final int RST3 = 0xFFD0; // Restart interval modulo 8 count 3
    public static final int RST4 = 0xFFD0; // Restart interval modulo 8 count 4
    public static final int RST5 = 0xFFD0; // Restart interval modulo 8 count 5
    public static final int RST6 = 0xFFD0; // Restart interval modulo 8 count 6
    public static final int RST7 = 0xFFD0; // Restart interval modulo 8 count 7

    // Other Markers
    public static final int SOI = 0xFFD8; // Start of Image
    public static final int EOI = 0xFFD9; // End of Image
    public static final int SOS = 0xFFDA; // Start of Scan
    public static final int DQT = 0xFFDB; // Define Quantization Table(s)
    public static final int DNL = 0xFFDC; // Define Number of Lines
    public static final int DRI = 0xFFDD; // Define Restart Interval
    public static final int DHP = 0xFFDE; // Define Hierarchical Progression
    public static final int EXP = 0xFFDF; // Expand Reference Component(s)
}
