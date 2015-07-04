
Pronghorn  [![Powered by CloudBees](https://www.cloudbees.com/sites/default/files/styles/large/public/Button-Powered-by-CB.png?itok=uMDWINfY)](https://pronghorn.ci.cloudbees.com/)
=====

Staged event driven framework using actors in the small scale, within one machine.

* Garbage Free
* Lock Free
* Block Free
* Small memory footprint
* Smart thread scheduling



Garbage free message passing design eliminates garbage collector stalls providing predictable data rates.  
Lock free non-blocking message passing enables cores to make continuous progress at all times.  
Staged pipeline scheduler enables optimization of the workload across cores.  


#Simple stage examples

- https://github.com/oci-pronghorn/PronghornExampleInputStages
- https://github.com/oci-pronghorn/PronghornExampleOutputStages



#Expected usage plan

Most projects using this framework will follow these steps.

1. Define your data flow graph.
2. Define the contracts between each stage.
3. Test first development by using generative testing as the graph is implemented.

NOTE: this is being proved out in the PronghornGateway project.

#Usage

  To use this in your maven project add the following dependency.

    <dependency>
      <groupId>com.ociweb</groupId>
      <artifactId>Pronghorn</artifactId>
      <version>0.0.1-SNAPSHOT</version>
    </dependency> 
   
  Also add this public repository to your pom or settings.

    <repository>
      <releases>
        <enabled>false</enabled>
      </releases>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
      <id>repository-pronghorn.forge.cloudbees.com</id>
      <name>Active Repo for PronghornPipes</name>
      <url>http://repository-pronghorn.forge.cloudbees.com/snapshot/</url>
      <layout>default</layout>
    </repository>
        
        

------------------------------------------

Looking for the release jar? This project is under active development.

Please consider getting involved and sponsoring the completion of [Pronghorn](mailto:info@ociweb.com;?subject=Pronghorn%20Sponsor%20Inquiry)


Nathan Tippy, Principal Software Engineer [OCI](http://ociweb.com)  
Twitter: @NathanTippy