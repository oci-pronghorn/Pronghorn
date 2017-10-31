package com.ociweb.jpgRaster;

public class JPGConstants {
    // Non-differential, Huffman coding
    public static final int SOF0 = 0xFFC0; // Baseline DCT
    public static final int SOF1 = 0xFFC1; // Extended sequential DCT
    public static final int SOF2 = 0xFFC2; // Progressive DCT
    public static final int SOF3 = 0xFFC3; // Lossless (sequential)
    // Differential, Huffman coding
    public static final int SOF5 = 0xFFC5; // Differential sequential DCT
    public static final int SOF6 = 0xFFC6; // Differential progressive DCT
    public static final int SOF7 = 0xFFC7; // Differential lossless (sequential)
    // Non-differential, arithmetic coding
    public static final int JPG = 0xFFC8;   // Reserved for JPEG extensions
    public static final int SOF9 = 0xFFC9;  // Extended sequential DCT
    public static final int SOF10 = 0xFFCA; // Progressive DCT
    public static final int SOF11 = 0xFFCB; // Lossless (sequential)
    // Differential, arithmetic coding
    public static final int SOF13 = 0xFFCD; // Differential sequential DCT
    public static final int SOF14 = 0xFFCE; // Differential progressive DCT
    public static final int SOF15 = 0xFFCF; // Differential lossless (sequential)
    // Huffman Table Specification
    public static final int DHT = 0xFFC4;
    // Arithmetic Conditioning Specification
    public static final int DAC = 0xFFCC;
    // Other markers
    public static final int SOI = 0xFFD8; // Start of image
    public static final int EOI = 0xFFD9; // End of image
    public static final int SOS = 0xFFDA; // Start of scan
    public static final int DQT = 0xFFDB; // Define quantization table
    public static final int DNL = 0xFFDC; // Define number of lines
    public static final int DRI = 0xFFDD; // Define restart interval
    public static final int DHP = 0xFFDE; // Define hierarchical progression
    public static final int EXP = 0xFFDF; // Expand reference component(s)
    public static final int COM = 0xFFFE; // Comment
}
