package com.ociweb.jpgRaster;

import java.io.File;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;

public class StandardJPGDecode {
	
	public static void main(String[] args) {
		try
	    {
			String defaultFiles = "huff_simple0 robot cat car squirrel nathan earth dice pyramids static turtle";
			String[] filenames = defaultFiles.split(" ");
			
			long start = System.nanoTime();
			for(String name: filenames) {
				BufferedImage image = ImageIO.read(new File("test_jpgs/" + name + ".jpg"));
				ImageIO.write(image, "bmp", new File("output_bmps/" + name + ".bmp"));
			}
			long end = System.nanoTime();
			
			double duration = (double)(end - start) / 1000000;
			System.out.println("Time in milliseconds: " + duration);
	    } 
	    catch (IOException e)
	    {
	    	
	    }
	}

}
