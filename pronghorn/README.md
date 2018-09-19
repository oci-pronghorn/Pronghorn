Pronghorn  [![Powered by CloudBees](https://www.cloudbees.com/sites/default/files/styles/large/public/Button-Powered-by-CB.png?itok=uMDWINfY)](https://pronghorn.ci.cloudbees.com/)
=====
### This is the Active Development Repo. For documentation and marketing materials, please visit [this repo](https://github.com/objectcomputing/Pronghorn).

Staged event driven single machine embedded micro-framework.

* Garbage Free (core runtime)
* Lock Free
* Block Free
* Small memory footprint
* Smart thread scheduling

**Garbage free message passing** design eliminates garbage collector stalls providing predictable data rates.  
**Lock free non-blocking message passing** enables cores to make continuous progress at all times.  
**Staged pipeline scheduler** enables optimization of the workload across cores. 

## Documentation
Please refer to the [wiki](https://oci-pronghorn.gitbook.io/pronghorn/chapter-0-what-is-pronghorn/home) for documentation, how to get started, and examples.

## Usage

  To use this in your Maven project, add the following dependency:
```xml
    <dependency>
      <groupId>com.ociweb</groupId>
      <artifactId>Pronghorn</artifactId>
      <version>1.0.3</version>
    </dependency> 
        

------------------------------------------

For more technical details please check out the Specification.md file.
Looking for the release jar? This project is under active development.

Please consider getting involved and sponsoring the completion of [Pronghorn](mailto:info@ociweb.com;?subject=Pronghorn%20Sponsor%20Inquiry)


Nathan Tippy, Principal Software Engineer [OCI](http://objectcomputing.com)  
Twitter: @NathanTippy
