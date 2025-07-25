# netchdf
_last updated: 7/25/2025_

This is a rewrite in Kotlin of parts of the devcdm and netcdf-java libraries. 

The intention is to create a maintainable, read-only, thread-safe, pure JVM library allowing full access to 
netcdf3, netcdf4, hdf4, hdf5, hdf-eos2, and hdf-eos5 data files. 

The library is close to feature complete. We are currently extensively testing and comparing to the reference libraries.

Please contact me if you'd like to help out. Especially needed are test datasets from all the important data archives!!

<!-- TOC -->
* [netchdf](#netchdf)
    * [Building](#building)
      * [What version of the JVM, Kotlin, and Gradle?](#what-version-of-the-jvm-kotlin-and-gradle)
    * [Why this library?](#why-this-library-)
    * [Why do we need another library besides the standard reference libraries?](#why-do-we-need-another-library-besides-the-standard-reference-libraries)
    * [What's wrong with the standard reference libraries?](#whats-wrong-with-the-standard-reference-libraries)
    * [Why Kotlin?](#why-kotlin)
    * [What about performance?](#what-about-performance)
    * [Goals and scope](#goals-and-scope)
    * [Non-goals](#non-goals)
    * [Testing](#testing)
      * [Code Coverage](#code-coverage)
      * [Testing against the reference libraries](#testing-against-the-reference-libraries)
    * [Data Model notes](#data-model-notes)
      * [Type Safety and Generics](#type-safety-and-generics)
      * [Cdl Names](#cdl-names)
      * [Datatype](#datatype)
      * [Typedef](#typedef)
      * [Dimension](#dimension)
      * [Compare with HDF5 data model](#compare-with-hdf5-data-model)
      * [Compare with HDF4 data model](#compare-with-hdf4-data-model)
      * [Compare with HDF-EOS data model](#compare-with-hdf-eos-data-model)
  * [Elevator blurb](#elevator-blurb)
<!-- TOC -->

### Building

* Download Java 21 JDK and set JAVA_HOME.
* Download git and add to PATH.

````
cd <your_build_dir>
git clone https://github.com/JohnLCaron/netchdf.git
cd netchdf
./gradlew clean assemble
````

Also see:
  * [Building and Running native library](docs/Building.md)
  * [Building and Running ncdump](cli/Readme.md)

#### What version of the JVM, Kotlin, and Gradle?

We use the latest LTS (long term support) Java version, and will not be explicitly supporting older versions.
Currently that is Java 21.

We also use the latest stable version of Kotlin that is compatible with the Java version. Currently that is Kotlin 2.1.

Gradle is our build system. We will use the latest stable version of Gradle compatible with our Java and Kotlin versions.
Currently that is Gradle 8.14.

For now, you must download and build the library yourself. Eventually we will publish it to Maven Central.
The IntelliJ IDE is highly recommended for all JVM development.


### Why this library? 

The scientific data stored in NetCDF and HDF file formats must remain forever readable. 

The Netcdf-Java library prototyped a "Common Data Model" (CDM) to provide a single API to access various file formats. 
The netcdf* and hdf* file formats are similar enough to make a common API a practical and useful goal. 
By focusing on read-only access to just these formats, the API and the code are kept simple. In short, a library that 
focuses on simplicity and clarity is a safeguard for the irreplaceable investment in these scientific datasets.

A second motivation is to allow multithreaded access to these files. The lack of thread safety in the HDF5-C library
is a major failing that needs to be fixed.

### Why do we need another library besides the standard reference libraries?

It's necessary to have independent implementations of any standard. If you don't have multiple implementations, it's
easy for the single implementer to mistake the implementation for the actual standard. It's easy to hide problems 
that are actually in the standard by adding work arounds in the code, instead of documenting problems and creating new
versions of the standard with clear fixes. For Netcdf/Hdf, the standard is the file formats, along with their semantic 
descriptions. The API is language and library specific, and is secondary to the standard.

More subtly, it's very hard to see the elegance (or otherwise) of your own design; you need independent review of your
data structures and API by people truly invested in getting them right.

Having multiple implementations is a huge win for the reference library, in that bugs are more quickly found, and 
ambiguities more quickly identified. 

### What's wrong with the standard reference libraries?

The reference libraries are well maintained but complex. They are coded in C, which is a difficult language to master
and keep bug free, with implications for memory safety and security. The libraries require various machine and OS dependent
toolchains. Shifts in funding could wipe out much of the institutional knowledge needed to maintain them.

The HDF file formats are overly complicated, which impacts code complexity and clarity. The data structures do not
always map to a user-understandable data model. Semantics are left to data-writers to document (or not). 
While this problem isn't specific to HDF file users, it is exacerbated by a "group of messages" design approach. 

The HDF4 C library is a curious hodgepodge of disjointed APIs. The HDF5 API is better and the Netcdf4 API much better.
But all suffer from the limitations of the C language, the difficulty of writing good documentation for all skill levels 
of programmers, and the need to support legacy APIs. 

HDF-EOS uses an undocumented "Object Descriptor Language (ODL)" text format, which adds a dependency on the SDP Toolkit 
and possibly other libraries. These toolkits also provide functionality such as handling projections and coordinate system 
conversions, and arguably it's impossible to process HDF-EOS without them. So the value added here by an independent 
library for data access is less clear. For now, we will provide a "best-effort" to expose the internal 
contents of the file.

Currently, the Netcdf-4 and HDF5 libraries are not thread safe, not even for read-only applications. 
This is a serious limitation for high performance, scalable applications, and it is disappointing that it hasn't been fixed.
See [Toward Multi-Threaded Concurrency in HDF5](https://www.hdfgroup.org/wp-content/uploads/2022/05/Toward-MT-HDF5.pdf),
and [RFC:Multi-Thread HDF5](https://support.hdfgroup.org/releases/hdf5/documentation/rfc/RFC_multi_thread.pdf) for more information.


### Why Kotlin?

Kotlin is a modern, statically typed, garbage-collected language suitable for large development projects. 
It has many new features for safer (like null-safety) and more concise (like functional idioms) code, and is an important 
improvement over Java, without giving up any of Java's strengths. Kotlin will attract the next generation of serious 
open-source developers, and hopefully some of them will be willing to keep this library working into the unforeseeable future.


### What about performance?

We are aiming to be within 2x of the C libraries for reading data. Preliminary tests indicate that's a reasonable goal. 
For HDF5 files using deflate filters, the deflate library dominates the read time, and standard Java deflate libraries 
are about 2X slower than native code. Unless the deflate libraries get better, there's not much gain in trying to make
other parts of the code faster.

We are seeing 10x speedup on data reading. see https://github.com/JohnLCaron/netchdf/issues/189.


### Goals and scope

Our goal is to give read access to all the content in NetCDF, HDF5, HDF4, and HDF-EOS files.

The library will be thread-safe for reading multiple files concurrently. We are also exploring concurrent reading within
the same file.

We are focussing on earth science data, and don't plan to support other uses except as a byproduct.

The core module will remain pure Kotlin with very minimal library dependencies and no write capabilities. In particular, 
there will be no dependency on the reference C libraries (except for testing). 

There will be no dependencies on native libraries in the core module, but other modules or
projects that use the core are free to use dependencies as needed, for example to use HDF5 filters that link to native libraries.


### Non-goals

It's not a goal to duplicate netcdf-java functionality.

It's not a goal to duplicate Netcdf-C library functionality.

It's not a goal to provide remote access to files.


### Testing

Currently most of the test files do not live in the github repo because they are too big. 
Eventually we will make them available in a separate download.

There are four levels of testing:

1. Unit testing that doesn't require reading files.
2. Testing with files in core/commonTest/data. These are fast and are run in a Github Action.
3. Testing with files in TestFiles.testData in module testfiles. These are medium fast (< 11 min wallclock).
4. Testing with files in TestFiles.testData in module testclibs. These are slow.

Currently we have 1500+ test files in the core and testdata modules:

````
hdf-eos2    = 440 files
hdf-eos5    = 18 files
hdf4        = 32 files
hdf5        = 175 files
netcdf3     = 664 files
netcdf3.2   = 81 files
netcdf3.5   = 1 files
netcdf4     = 119 files

total # files = 1530
````

We will continue to add representative samples of recent files for improved testing and code coverage.

####  Code Coverage

Currently we have this test coverage from the core and testfiles modules:

````
 cdm      88% (1560/1764) LOC
 hdf4     84% (1743/2071) LOC
 hdf5     81% (2278/2792) LOC
 netcdf3  77% (230/297) LOC
 ````

7/18/2025
````
 cdm.api        94% (532/567) LOC
 cdm.array      95% (662/698) LOC
 cdm.iosp       68% (146/213) LOC
 cdm.layout     89% (277/310) LOC
 cdm.util       76% (106/139) LOC
 hdf4           82% (1638/2008) LOC
 hdf5           80% (2740/3417) LOC
 netcdf3        80% (213/266) LOC
 
 all            83% (6314/7618) LOC
 ````

The core library has ~7600 LOC.

#### Testing against the reference libraries

More and deeper test coverage is provided in the testclibs module, which compares netchdf metadata and data against
the Netcdf, HDF5, and HDF4 C libraries. Note that the clibs module is not part of the released netchdf library and is 
only supported for test purposes.

We use the Java [Foreign Function & Memory API](https://docs.oracle.com/en/java/javase/21/core/foreign-function-and-memory-api.html)
for testing against the Netcdf, HDF5, and HDF4 C libraries.
With these tools we can be confident that our library gives the same results as the reference libraries.

Currently using
* HDF5 library version: 1.14.6.
* netcdf-c library version 4.10.0-development of May 23 2025
* HDF-4 library version: ???

In order to run, you must install the C libraries on your computer and ad them to the LD_LIBRARY_PATH.

### Data Model notes

Also see [Netchdf core UML](https://docs.google.com/drawings/d/1lkouJBUG5uy8aUtbKfAZN9D5h_v22JNWf6QUQWjPNBc)

#### Type Safety and Generics

Datatype\<T\>, Attribute\<T\>, Variable\<T\>, StructureMember\<T\>, Array\<T\> and ArraySection\<T\> are all generics, 
with T indicating the data type returned when read, eg:

````
    fun <T> readArrayData(v2: Variable<T>, section: SectionPartial? = null) : ArrayTyped<T>
````

For example, a Variable of datatype Float will return an ArrayFloat, which is ArrayTyped\<Float\>.

#### Cdl Names

* spaces are replaced with underscores

#### Datatype
* _Datatype.ENUM_ returns an array of the corresponding UBYTE/USHORT/UINT. Call _data.convertEnums()_ to turn this into
  an ArrayString of corresponding enum names.
* CHAR vs STRING: 
  * Attributes of type CHAR are always assumed to be Strings. 
  * Netcdf-3 does not have STRING or UBYTE types, and in practice, CHAR is used for either. Variables of type CHAR 
    return data as ArrayUByte. 
  * Netcdf-4 encodes CHAR values as HDF5 string type with elemSize = 1, so we use that convention to detect 
    legacy CHAR variables in HDF5 format. (NC_CHAR should not be used in new Netcdf-4 files, use NC_UBYTE or NC_STRING.) 
    Variables of type CHAR return data as STRING, since users can use UBYTE if thats what they intend.
  * Netcdf-4/HDF5 String variables may be fixed or variable length. For fixed Strings, we set the size of Datatype.STRING to 
    the fixed size. For both fixed and variable length Strings, the string will be truncated at the first zero byte, if any.
  * HDF4 does not have a STRING type, but does have signed and unsigned CHAR, and signed and unsigned BYTE. 
    Both signed and unsigned CHAR are mapped to Datatype.CHAR, whose data is returned as Strings for Attributes, 
    and ArrayUByte for Variables.
  * Call _data.makeStringsFromBytes() to turn ArrayUByte into ArrayString with the array reduced by one.
* _Datatype.STRING_ always appears to be variable length to the user, regardless of whether the data in the file is variable or fixed length.
* _Datatype.STRING_ is always encoded as UTF8. TODO add option to change encoding, probably when opening the file.

#### Typedef
Unlike Netcdf-Java, we follow Netcdf-4 "user defined types" and add typedefs for Compound, Enum, Opaque, and Vlen.
* _Datatype.ENUM_ typedef has a map from integer to name (same as Netcdf-Java)
* _Datatype.COMPOUND_ typedef contains a description of the members of the Compound (aka Structure).
* _Datatype.OPAQUE_ typedef may contain the byte length of OPAQUE data.
* _Datatype.VLEN_ typedef has the base type. An array of VLEN may have different lengths for each object.

#### Dimension
* Unlike Netcdf-3 and Netcdf-4, dimensions may be "anonymous", in which case they have a length but not a name, and are 
local to the variable they are referenced by.
* There are no UNLIMITED dimensions. These are unneeded since we do not support writing.

#### Compare with HDF5 data model
* Creation order is ignored
* We don't include soft (aka symbolic) links in a group, as these point to an existing dataset (variable).
* Opaque: hdf5 makes arrays of Opaque all the same size, which gives up some of its usefulness. If there's a need,
  we will allow Opaque(*) indicating that the sizes can vary.
* Attributes can be of type REFERENCE, with value the full path name of the referenced dataset.
* Vlen Strings are stored on the heap. Fixed length Strings are kept in byte arrays. 
  This is more or less invisible to the User.

#### Compare with HDF4 data model
* All data access is unified under the netchdf API.

#### Compare with HDF-EOS data model
* The _StructMetadata_ ODL is gathered and applied to the file header metadata as well as possible. 
  Contact us with example files if you see something we are missing.

## Elevator blurb

An independent implementation of HDF4/HDF5/HDF-EOS in Kotlin.

This will be complementary to the important work of maintaining the primary HDF libraries.
The goal is to give read access to all the content in NetCDF, HDF5, HDF4 and HDF-EOS files.

The core library is pure Kotlin. 
Kotlin currently runs on JVM's as far back as Java 8. However, I am targeting the latest LTS
(long term support) Java version, and will not be explicitly supporting older versions.

A separate library tests the core against the C libraries.
The key to this working reliably is if members of the HDF community contribute test files to make sure
the libraries agree. I have a large cache of test files from my work on netcdf-java, but these
are mostly 10-20 years old.

Currently the code is in alpha, and you must build it yourself with gradle. 
When it hits beta, I will start releasing compiled versions to Maven Central.

I welcome any feedback, questions, and concerns. Thanks!