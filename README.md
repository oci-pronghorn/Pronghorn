# JPG-Raster
## What is it?
JPG-Raster (or JPG-to-Raster or J2R) is a JPG file decoder.
It reads a JPG file and decompresses all of its data which is then accessible as an array of raw RGB pixel values.

## How does it work?
From the JPG file, it reads the quantization tables and the Huffman tables, which are the rules that were used by the encoder that created the file.
It then parses the compressed image data and expands it into many 8x8 pixel blocks - called MCUs - based on the Huffman tables.
Then using the quantization tables, it scales each value in the MCUs which now represent coefficients of a sum of 8^2 cosine functions that describe how much each cosine function contributes to the relative frequency changes of each pixel within the MCU.
It then performs Inverse Discrete Cosine Transformation on these coefficients to calculate the values of the three color components for each pixel in each MCU.
Then it converts these three color components from the YCbCr color space to the RGB color space.
Lastly, it exports this array of RGB pixels to a bitmap file with the same name as the input file.

Currently, it supports and successfully decompresses the most standard JPG format, with wider support coming soon.

J2R uses Pronghorn-Pipes for multi-threading and dataflow management to speed up decompression and to process many JPG files at once. The decoding process is split into the 5 major stages and each stage is a separate thread. Data is passed in small chunks from thread to thread, being transformed at every stage.

## How to use it?
From the command line, you can specify an input JPG file with the `-f` option (you do not have to include the `.jpg` extension).
You can also provide a space-separated list of many JPG files.
Then it will produce a `.bmp` file with the same name after decompressing the JPG file.
If including J2R in your own project, you can opt to not create BMP files and instead just use the RGB pixel array in memory for your own purposes.
BMP was chosen as the output file type due to its very small header (26 bytes) followed by the raw, uncompressed RGB values.

## Planned Features
 - Support more Start of Frame markers (i.e. Progressive)
 - Support files with one color component (grayscale)
 - Support horizontal and vertical sampling factors other than 1
 - Support 16-bit precision quantization tables
 - Support non-standard start/end of selection and successive
   approximation
