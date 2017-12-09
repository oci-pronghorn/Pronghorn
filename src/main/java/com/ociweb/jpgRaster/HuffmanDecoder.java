//for some reason adding to the package is breaking this. 
// package com.ociweb.jpgRaster; 
package jpgRaster;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import apache.*;


public class HuffmanDecoder {

    class HuffmanTable {
        byte tableChannel;
        byte tableComponent;

        ArrayList<Byte>[] values;
        ArrayList<Short>[] codes;

        public HuffmanTable(byte tableChannel, byte tableComponent, 
                            ArrayList<Byte>[] values, ArrayList<Short>[] codes) {
            this.tableChannel   = tableChannel;
            this.tableComponent = tableComponent;
            this.values         = values;
            this.codes          = codes;
        }

        public void printHuffCodesValues() {
            
            for(int i = 0; i < this.codes.length; ++i){
                    // println("Codes  " + c[i]);
                    // println("Values " + v[i]);
                for(int j = 0; j < this.values[i].size(); ++j) {
                    println("Length: " + (i + 1) + "  Code: " + formatShort(this.codes[i].get(j)) + "  Value: " + formatByte(this.values[i].get(j)));
                }
            }
        }

        public String formatByte(byte b){
            return String.format("%02x", b);
        }
        public String formatShort(short s){
            return String.format("%04x", s);
        }
        public void print(Object o){
            System.out.print(o);
        }
        public void println(Object o){
            System.out.println(o);
        }
    }

    
    class MCU {
        int yDc;
        int cbDc;
        int crDc;

        byte[] yAc = new byte[64];
        byte[] cbAc = new byte[64];
        byte[] crAc = new byte[64];

    }

    HuffmanTable table1;
    HuffmanTable table2;
    HuffmanTable table3;
    HuffmanTable table4;

    public void interpretHuffmanSegment(byte[] b) throws IOException {
        //I will use an input stream to keep my place in the data. 
        ByteArrayInputStream bytes = new ByteArrayInputStream(b);
        table1 = interpretDHT(bytes);
        table2 = interpretDHT(bytes);
        table3 = interpretDHT(bytes);
        table4 = interpretDHT(bytes);
    }

    public HuffmanTable interpretDHT(ByteArrayInputStream bytes){
        byte firstByte = 0;
        byte firstHalf = 0;
        byte secondHalf = 0;
        byte[] numElements = new byte[16];
        ArrayList<Byte>[] values = (ArrayList<Byte>[])new ArrayList[16];
        for(int i = 0; i < 16; ++i) { values[i] = new ArrayList<Byte>(); }
        ArrayList<Short>[] codes;


        //First byte (in halves) should be HT information for each table
        firstByte = (byte)bytes.read();
        firstHalf = (byte)(firstByte & 0xF0); //Does this need shifted 4 to the right?
        secondHalf = (byte)(firstByte & 0x0F);

        // println("numbers of elements:");
        for(int i = 0; i < 16; ++i){
            numElements[i] = (byte)bytes.read();
            // println(formatByte(numElements[i]) + " " + (i + 1)); //print byte as hex
        }

        print("\n\n\n");
        for(int i = 0; i < 16; ++i) {
            for(int j = 0; j < numElements[i]; ++j){
                values[i].add((byte)bytes.read());
            }
        }

        codes = generateCodes(numElements);
        
        // printHuffCodesValues(codes, values);

        HuffmanTable table = new HuffmanTable(firstHalf, secondHalf, values, codes);

        table.printHuffCodesValues();

        // printHuffValues(values);
        // printHuffCodes(codes);
        println("+++++++++++++++++++++++++++++++++++++++++++\n\n\n");
        return table;
    }

    public static ArrayList<Short>[] generateCodes(byte[] sizes){
        ArrayList<Short>[] codes = (ArrayList<Short>[])new ArrayList[16];
        for(int i = 0; i < 16; ++i) { codes[i] = new ArrayList<Short>(); }

        short code = 0;
        for(int i = 0; i < sizes.length; ++i){
            for(int j = 0; j < sizes[i]; ++j){
                codes[i].add(code);
                ++code;
            }
            code <<= 1;
        }
        return codes;
    }

    public void decodeFourTables(ByteArrayInputStream bytes) throws IOException {
        BitStream b = new BitStream((InputStream)bytes);
        int currentCode = b.nextBit();
        ArrayList<MCU> out = new ArrayList<MCU>();
        // ByteArrayOutputStream out = new ByteArrayOutputStream();

        MCU newMCU = new MCU();

        try{ //Right now I'm going to run this process in a try catch block
             //while waiting to get a piece of valid input from the scanner
             //class, and then I will debug from there. 


        while(true){
            

            //Y ======================================================================

            //get the Y DC value for this MCU
            boolean found = false;
            for(int i = 0; i < 16 && !found; ++i) {
                for(int j = 0; j <= table1.codes[i].size(); ++j){
                    if(currentCode == table1.codes[i].get(j)) {
                        int length = table1.values[i].get(j);
                        newMCU.yDc = (int)b.nextBits(length);
                        found = true;
                        break;
                    }
                }
                currentCode = (currentCode << 1) & b.nextBit();
            }


            //Get the Y AC values for this MCU
            boolean acFinish = false;
            found = false;
            for(int k = 0; k < 64 && !acFinish; ++k){
                currentCode = (0 & b.nextBit());
                for(int i = 0; i < 16 && !found; ++i){
                    for(int j = 0; j <= table2.codes[i].size(); ++j){
                        if(currentCode == table2.codes[i].get(j)) {
                            newMCU.yAc[k] = table2.values[i].get(j);
                            if(table2.values[i].get(j) == 0) {
                                for(; k < 64; ++k){
                                    newMCU.yAc[k] = 0;
                                }
                                acFinish = true;
                            }
                            found = true;
                            break;
                        }
                        currentCode = (currentCode << 1) & b.nextBit();
                    }
                }
            }


            //CB=========================================================================


            currentCode = (0 & b.nextBit());
            //get the cb DC value for this MCU
            found = false;
            for(int i = 0; i < 16 && !found; ++i) {
                for(int j = 0; j <= table3.codes[i].size(); ++j){
                    if(currentCode == table3.codes[i].get(j)) {
                        int length = table3.values[i].get(j);
                        newMCU.cbDc = (int)b.nextBits(length);
                        found = true;
                        break;
                    }
                }
                currentCode = (currentCode << 1) & b.nextBit();
            }

            //Get the cb AC values for this MCU
            acFinish = false;
            found = false;
            for(int k = 0; k < 64 && !acFinish; ++k){
                currentCode = (0 & b.nextBit());
                for(int i = 0; i < 16 && !found; ++i){
                    for(int j = 0; j <= table4.codes[i].size(); ++j){
                        if(currentCode == table4.codes[i].get(j)) {
                            newMCU.cbAc[k] = table4.values[i].get(j);
                            if(table4.values[i].get(j) == 0) {
                                for(; k < 64; ++k){
                                    newMCU.cbAc[k] = 0;
                                }
                                acFinish = true;
                            }
                            found = true;
                            break;
                        }
                        currentCode = (currentCode << 1) & b.nextBit();
                    }
                }
            }


            //CR ===========================================================================

            currentCode = (0 & b.nextBit());
            //get the cr DC value for this MCU
            found = false;
            for(int i = 0; i < 16 && !found; ++i) {
                for(int j = 0; j <= table3.codes[i].size(); ++j){
                    if(currentCode == table3.codes[i].get(j)) {
                        int length = table3.values[i].get(j);
                        newMCU.cbDc = (int)b.nextBits(length);
                        found = true;
                        break;
                    }
                }
                currentCode = (currentCode << 1) & b.nextBit();
            }

            //Get the cr AC values for this MCU
            acFinish = false;
            found = false;
            for(int k = 0; k < 64 && !acFinish; ++k){
                currentCode = (0 & b.nextBit());
                for(int i = 0; i < 16 && !found; ++i){
                    for(int j = 0; j <= table4.codes[i].size(); ++j){
                        if(currentCode == table4.codes[i].get(j)) {
                            newMCU.crAc[k] = table4.values[i].get(j);
                            if(table2.values[i].get(j) == 0) {
                                for(; k < 64; ++k){
                                    newMCU.crAc[k] = 0;
                                }
                                acFinish = true;
                            }
                            found = true;
                            break;
                        }
                        currentCode = (currentCode << 1) & b.nextBit();
                    }
                }
            }
            out.add(newMCU);
        }//end while
        }catch(Exception e){
            println(e);
        }

        //return out
    }

    
    public static String formatByte(byte b){
        return String.format("%02x", b);
    }
    public static String formatShort(short s){
        return String.format("%04x", s);
    }
    public static void print(Object o){
        System.out.print(o);
    }
    public static void println(Object o){
        System.out.println(o);
    }


}

