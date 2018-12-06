# PronghornRanch (A Maven Archetype for Pronghorn projects)

## Overview
1. [Create your first Pronghorn project using PronghornRanch](#create-your-first-pronghorn-project-using-pronghornranch)
2. [Working with the Sample](#working-with-the-sample-project)
3. [Signing the Project](#signing-the-project)

## Create your first Pronghorn project using PronghornRanch

### What you need:
* Java 8
* [Maven](https://maven.apache.org/)
* Git (or any other way to download from GitHub)

To simplify creation of a Pronghorn project, we created [PronghornRanch](https://github.com/oci-pronghorn/PronghornRanch), a Maven archetype which generates a working Pronghorn example.

***


First, run the following command:

    mvn archetype:generate -DarchetypeGroupId=com.ociweb -DarchetypeArtifactId=pronghorn-ranch

You will then be prompted to enter the following properties:

***

```groupID:``` Type in com.ociweb, then press enter

```artifactID:``` Type in name of your project, lower case with - as needed, then press enter

```version: 1.0-SNAPSHOT:``` Ignore, press enter

```package: com.ociweb:``` Ignore, press enter

```mainClass:``` Type in proper case class name

```Y: :``` Type "Y" if you're okay with the name of your project. If you aren't, press any other key and repeat this step.
***

This will automatically create a folder with our project files.

Before you can start editing and compiling your code, make sure to run the following:

    cd YOUR_PROJECT_NAME
    mvn install

If all goes well, your Pronghorn project is now ready. Open up your project folder in your favorite IDE and start coding.

## Working with the Sample Project
Your newly generated project now contains a basic Pronghorn example and its source code. The example takes in a file by argument, creates a new graph, populates this graph with pipes and stages required to read, duplicate, and output the file, enables telemetry, and starts the default scheduler.

This will get you started with everything you need to write your own Pronghorn project.

***

The sample project consists of the following:

**src/main/java/YOUR_PROJECT_NAME.java**

> Main class that creates and populates the graph and starts the scheduler. You will want to focus on this file the most.

**src/main/java/SchemaOneSchema.java**

> Auto-generated example schema. Not currently related to actual demo project functionality, but useful to look at in the future.

**src/test/java/YOUR_PROJECT_NAMETest.java**

> Test for the main class. It creates a new instance of YOUR_PROJECT_NAME, checks the graph, and then  verifies the output.

**src/test/java/SchemaTest.java**

> Test that simply asserts schema. Every schema needs this assertion. It verifies that the FAST XML is formatted correctly, and if it isn't, outputs generated code to be placed in the corresponding Schema Java file (not test file).

**src/test/resources/SchemaOne.xml**

> This is an example of a FAST XML Schema. Pronghorn uses this protocol to define and enforce the format of data being passed between stages and pipes. These XML files are used as a template for the test to assert and generate the actual Schema java file.

> The SchemaOne.xml demonstrates various different types and values that will be used to communicate data (such as string, uint, etc...).

> Please refer to the [Schema page](../wiki/schema) on how to create your own schemas.

### Tests
The project also includes 2 files for testing. The SchemaTest verifies that the schema is valid. The YOUR_PROGRAM_NAMETest ensures that ChunkedStream appear in the output and that the input pipe volume matches the output pipe volume.

## Signing the Project

We have provided a key to sign your project; however, if you would like to use a key of your own choosing, you can do so by replacing the ocikeystore.jks file located in **projectname/src/main/resources/certificate/ocikeystore.jks** with your own .jks key or one provided from an official Certificate Authority (CA).

**It is not necessary for you to sign your project. This is an added feature for security reasons.**

To create your own jks file, simply navigate to your jdk_version folder on your computer and locate the bin folder. Within the folder you will see a .exe file called keytool. While at the CMD of this folder type: "keytool -genkey -alias my_certificate_alias -keyalg RSA -keysize 4096 -keystore keystore.jks" press enter and you will prompted with a wizard to fill in the rest. Take note of your password and alias. Next, using whichever IDE you prefer; navigate to the POM file within the main folder and edit the properties section at the top to fit the values of your own key. Replace the alias with the alias you just used as well as the password. Change the last part of the **keystore.path** section from "../ocikeystore.jks" to the name of your "../keystore.filetype".

If you are using a Certificate Authority (CA) to sign your project simply put the information related to the CA in the properties of the POM file. The project will then automatically be signed when you perform a mvn install in the following step.
