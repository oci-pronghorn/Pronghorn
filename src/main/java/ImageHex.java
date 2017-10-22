import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ImageHex {

    public static void main(String[] args){
    	int in;
    	String current = "";
    	String last = "";

        try {
            FileInputStream f = new FileInputStream("huff_simple0.jpg");
            
            while((in = f.read()) != -1){

            	current = String.format("%02x", (byte)in);
            	
            	if(last.equals("ff") && current.equals("c4")){
            		System.out.println(current);
            		System.out.println("Huffman Indicator found\n");

            		grabData(f);

            		break;
            	}
            	last = current;
            	System.out.println(last);
            }
        } catch(IOException e) {

        }
        System.out.println();
    }

    public static void grabData(FileInputStream f) throws IOException{
        int huffLength;
        int huffLength1;
        int huffLength2;
        int[] huffData;

        //The number denoting the length of huffman data is two bytes long and follows the huffman indicator. 
        huffLength1 = f.read();
        huffLength2 = f.read();
        huffLength = ((huffLength1 << 8) | (huffLength2 & 0xFF)); //This is basically concatenation of two binary strings. 
        huffData = new int[huffLength];
        for(int i = 0; i < huffLength; ++i){
            huffData[i] = f.read();
        }

        System.out.println("The huffman data length is: " + huffLength);
        for(int i = 0; i < huffLength; ++i) {
            if (i % 4 == 0 && i != 0) System.out.print("  ");
            if (i % 8 == 0 && i != 0) System.out.print("\n");
            System.out.print(String.format("%02x", huffData[i]) + " ");
        }
    }
}