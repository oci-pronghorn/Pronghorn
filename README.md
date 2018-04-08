# JPG-Raster
## What is it?
JPG-Raster (or JPG-to-Raster or J2R) is a JPG file decoder and encoder.
It reads a JPG file and decompresses all of its data which is then accessible as an array of raw RGB pixel values.
It can also read an uncompressed BMP file, compress the data and output a corresponding JPG file.

## How does it work?
From the JPG file, it reads the quantization tables and the Huffman tables, which are the rules that were used by the encoder that created the file.
It then parses the compressed image data and expands it into many 8x8 pixel blocks - called MCUs - based on the Huffman tables.
Then using the quantization tables, it scales each value in the MCUs which now represent coefficients of a sum of 8^2 cosine functions that describe how much each cosine function contributes to the relative frequency changes of each pixel within the MCU.
It then performs Inverse Discrete Cosine Transformation on these coefficients to calculate the values of the three color components for each pixel in each MCU.
Then it converts these three color components from the YCbCr color space to the RGB color space.
Lastly, it exports this array of RGB pixels to a bitmap file with the same name as the input file.

Currently, J2R supports and successfully decompresses nearly all of the most standard JPG formats, such as baseline images, progressive images, grayscale images, and chroma subsampled images.

The encoding process is exactly the same as the decoding process but in reverse.

J2R uses Pronghorn-Pipes for multi-threading and dataflow management to speed up (de)compression and to process many files at once. The decoding/encoding process is split into the 5 major stages and each stage is a separate thread. Data is passed in small chunks from thread to thread, being transformed at every stage.

## How to use it?
From the command line, you can specify an input JPG file (or a list of many files) with the `-f` option.
You can also specify a directory with the `-d` option to process all files in a directory.
Then it will produce a `.bmp` file with the same name after decompressing the JPG file.
If including J2R in your own project, you can opt to not create BMP files and instead just use the RGB pixel array in memory for your own purposes.
BMP was chosen as the output file type due to its very small header (26 bytes) followed by the raw, uncompressed RGB values.

J2R uses decoding mode by default, but you can enable encoding mode with the `-e` option.
Then use the `-f` or `-d` option to specify input BMP files.
This encodes the BMP files using the standard JPG quantization tables and Huffman tables.

## Known Bugs
Some progressive images (especially progressive images that also use chroma subsampling) can produce wrong output. This will be fixed soon.

