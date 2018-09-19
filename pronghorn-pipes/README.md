
PronghornPipes  
=====
Click for build status [![Powered by CloudBees](https://www.cloudbees.com/sites/default/files/styles/large/public/Button-Powered-by-CB.png?itok=uMDWINfY)](https://pronghorn.ci.cloudbees.com/)    

Ring buffer based queuing utility for applications that require high performance and/or a small footprint.  Well suited for embedded and stream based processing. 

Some of the key ideas embodied in the project are:
- Messages are data not objects
- Using memory sequentially is faster than random access
- Exclusive use of primitives provides for a smaller memory footprint
- Stream processing must be garbage free to eliminate stalls and support deterministic behavior
- Single producer single consumer lock free queues provide for simple designs and good performance

#Purpose

The purpose of this project was to build a light weight data passing queue for large concurrent application development.  The [Pronghorn](https://github.com/oci-pronghorn/Pronghorn) framework makes use of these queues for all message passing between actors.

Applications using Embedded and/or Compact Profiles will find this project most helpful.  None of the collections APIs are used and all the processing is done with primitives.  The only external dependencies are JUnit for testing and SLF4J for logging.  

#Design

These queues are similar to the [Disruptor](https://github.com/LMAX-Exchange/disruptor) and also fill the same purpose which is to pass data between threads.  

Unlike other Java projects PronghornPipes defines messages as data and therefore are always serialized and do not have a (first class) object representation.  Messages passed on the queue are defined in an XML file and are loaded into a schema object (FieldReferenceOffsetManager).  This object can be used directly eliminating any need to use XML or parsing in a deliverable product.

All the APIs read and write the fields directly in place (eg on the queue).  This eliminates pointer chasing overhead and encourages sequential usage and optimal cache line usage.

The RingBuffer class contains 2 private array buffers, one for fixed data and one for variable length data.  This design enables direct access to any fields within the messages while supporting variable length field types.

#Ring Buffer Features

* Garbage free, runtime data passing does not allocate or free any memory
* Lock free, CAS used for setting position of head/tail on ring buffer 
* Non-blocking, try methods provided to enable continuous progress 
* Support for both simple messages and complex messages with nested sequences

#Multiple Usage APIs

Internally there is one layout format for the data however to support all the different uses cases there are 4 different API to access the rings.

* Low level API for serialized data directly on the ring
* High level API for field specific access
* Streaming API using call-backs for each field for dynamic applications.
* Event Consumer/PRoduceer API for Object mapping

For examples on how these APIs may be used see the projects:
- https://github.com/oci-pronghorn/PronghornExampleInputStages
- https://github.com/oci-pronghorn/PronghornExampleOutputStages

#Usage

  To use this in your maven project add the following dependency.

    <dependency>
      <groupId>com.ociweb</groupId>
      <artifactId>PronghornPipes</artifactId>
      <version>1.0.3</version>
    </dependency> 
   
     

------------------------------------------

Looking for the release jar? This project is under active development.

Please consider getting involved and sponsoring the completion of [PronghornPipes](mailto:info@ociweb.com;?subject=Pronghorn%20Sponsor%20Inquiry)


Nathan Tippy, Principal Software Engineer [OCI](http://ociweb.com)  
Twitter: @NathanTippy
