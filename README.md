jFAST
=====

FIX Adapted for STreaming optimized for the JVM 

http://en.wikipedia.org/wiki/FAST_protocol


Under active development expect many breaking changes


# Design Goals

* Fastest FAST protocol implementation for the Java platform.
* Garbage free implementation.
* APIs designed for performance.
 * Low latency
 * Compression
 * Low power/CPU usage
* Templates for streaming format
* Dynamic template selection (stream format version)	
* Android
* Compatibility with other language implementations.

# Why

* Financial industry has already proven the protocols value
* Works best on relatively slow networks in comparison to processor speed.
* The gap between CPU speeds and memory/network/IO continues to widen.
* On mobile devices the processor continues to get faster but data providers speed lags greatly.
* Low latency solution also implies low power which helps mobile application.
* As persistent serialization with on the fly compression it can dramatically reduce IO time to/from SSD or spinning drives.
* Template based serialization formats allow for dynamic versions add more upgrade paths for deployed applications.
* Implementations already exist in C,C++,C# and it may be desirable to have a mobile client in Java.
* There are no open source(free) low latency Java implementations that do not produce garbage.




