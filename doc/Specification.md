Phast  (Pronghorn Archive Storage)
=====

This project is derived from parts of the FAST encoding specification.


Changes to the existing protocol were made to:
* Emphasize Mechanical sympathy
* Better balance between speed and compression
* No state or schema required for first stage of decode (unpacking)

# Packing

Everything on the wire is either packed longs (64 bits) or raw bytes. This enables the packing/unpacking stages to be generic and general purpose regardless of schema.


Packed values are big endian encoded as defined in the FAST specification and as implemented in PronghonPipes: the DataOutputBlobWriter and DataInputBlobReader. The encoded value -63 is special and represents the ‘escape’ value for the encoding of raw bytes.  


If you need to encode the value of -63, you will have to encode it twice in a row. When encoding raw bytes, the -63 will be written, followed by a packed number equal to the number of bytes that follow which are to be copied as-is.


Example: encoding the escape value (-63)
-63
-63


Example: encoding raw bytes
-63
Blob Length (Packed)
Raw Bytes of that length (all the bytes needed ahead of the, packed, data using it)


# Encoding

All encoding is done on one message fragment at a time. A message fragment is a continuous block of simple fields. Most simple messages are just one fragment. A sequence is a known count of child fragment instances.


Packets each start with Huffman bits in a packed long. The remaining bits are used to pass meta data about the stream.


Bit | Packet Name | Description
--- | ----------- | -----------
0 | Fragment Packet | Presence Map  <br> Optional Run Length this present applies to
10 | Fragment Identification Packet | Fragment Index <br> Assumed same packet until another 10 packet is encountered
110 | Ending Packet | 0001 : Message <br> 0010 : Block <br> 0100 : Runtime
11110 | Version Packet | Version production releases: <br> 00 - 11 developer releases <br> >= 1000000000
1111110 | Reset Packet | 0 : reset all dictionary values <br> 1 : custom reset for specific field sets
111111110 | Schema Definition Packet | TBD

# Presence Map

The presence map operator is based on the order of it’s appearance. Each field is given an unique bit based on the definition of its operation. In addition, a second optional bit is also given to each field if the field is nullable. Nullable fields have a 1 in that bit which indicates that the value has not been transmitted.

Operators | Description 
--------- | -----------
Increment | 1 : sending value to reset count++ <br> 0 : use last value
Copy | 1 : sending value <br> 0 : use last value
Delta | 1 : sending delta value from previous source <br> 0 : use default value
Default | 1 : sending value <br> 0 : use default value
Constant | Never sends data 


Other Notes:


Will have a need for Enum type, not yet implemented.


Blocks of bytes may be serialized objects or encoded strings. No compression is applied because this type of unstructured data is better compressed by LZMA or other deflation technologies.


