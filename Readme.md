# netchdf
_last updated: 5/29/2025_

This is a rewrite in Kotlin of parts of the devcdm and netcdf-java libraries. 

The intention is to create a maintainable, read-only, pure JVM library allowing full access to 
netcdf3, netcdf4, hdf4, hdf5, hdf-eos2, and hdf-eos5 data files. 

Please contact me if you'd like to help out. Especially needed are test datasets from all the important data archives!!

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
  * [Building and Running ncdump](cli/Readme.md)

### Why this library? 

There is so much important scientific data stored in the NetCDF and HDF file formats, that those formats will 
never go away. It is important that there be maintainable, independent libraries to read these files forever.

The Netcdf-Java library prototyped a "Common Data Model" (CDM) to provide a single API to access various file formats. 
The netcdf* and hdf* file formats are similar enough to make a common API a practical and useful goal. 
By focusing on read-only access to just these formats, the API and the code are kept simple.

In short, a library that focuses on simplicity and clarity is a safeguard for the irreplaceable investment in these
scientific datasets.

### Why do we need another library besides the standard reference libraries?

It's necessary to have independent implementations of any standard. If you don't have multiple implementations, it's
easy for the single implementer to mistake the implementation for the actual standard. It's easy to hide problems 
that are actually in the standard by adding work-arounds in the code, instead of documenting problems and creating new
versions of the standard with clear fixes. For Netcdf/Hdf, the standard is the file formats, along with their semantic 
descriptions. The API is language and library specific, and is secondary to the standard.

Having multiple implementations is a huge win for the reference library, in that bugs are more quickly found, and 
ambiguities more quickly identified. 

### What's wrong with the standard reference libraries?

The reference libraries are well maintained but complex. They are coded in C, which is a difficult language to master
and keep bug free, with implications for memory safety and security. The libraries require various machine and OS dependent
toolchains. Shifts in funding could wipe out much of the institutional knowledge needed to maintain them.

The HDF file formats are overly complicated, which impacts code complexity and clarity. The data structures do not
always map to a user understandable data model. Semantics are left to data-writers to document (or not). 
While this problem isn't specific to HDF file users, it is exacerbated by a "group of messages" design approach. 

The HDF4 C library is a curious hodgepodge of disjointed APIs. The HDF5 API is better and the Netcdf4 API much better.
But all suffer from the limitations of the C language, the difficulty of writing good documentation for all skill levels 
of programmers, and the need to support legacy APIs. 

HDF-EOS uses an undocumented "Object Descriptor Language (ODL)" text format, which adds a dependency on the SDP Toolkit 
and possibly other libraries. These toolkits also provide functionality such as handling projections and coordinate system 
conversions, and arguably it's impossible to process HDF-EOS without them. So the value added here by an independent 
library for data access is less clear. For now, we will provide a "best-effort" to expose the internal 
contents of the file.

Currently, the Netcdf-4 and HDF4 libraries are apparently not thread safe, although the details are complex.
See [Toward Multi-Threaded Concurrency in HDF5](https://www.hdfgroup.org/wp-content/uploads/2022/05/Toward-MT-HDF5.pdf),
and [RFC:Multi-Thread HDF5](https://support.hdfgroup.org/releases/hdf5/documentation/rfc/RFC_multi_thread.pdf)
for more information.

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

We will investigate using Kotlin coroutines to speed up performance bottlenecks.

### What version of the JVM, Kotlin, and Gradle?

We will always use the latest LTS (long term support) Java version, and will not be explicitly supporting older versions.
Currently that is Java 21.

We also use the latest stable version of Kotlin that is compatible with the Java version. Currently that is Kotlin 2.1.

Gradle is our build system. We will use the latest stable version of Gradle compatible with our Java and Kotlin versions.
Currently that is Gradle 8.14.

For now, you must download and build the library yourself. Eventually we will publish it to Maven Central. 
The IntelliJ IDE is highly recommended for all JVM development.

### Scope

Our goal is to give read access to all the content in NetCDF, HDF5, HDF4, and HDF-EOS files.

The library will be thread-safe for reading multiple files concurrently.

We are focussing on earth science data, and don't plan to support other uses except as a byproduct.

The core module will remain pure Kotlin with very minimal dependencies and no write capabilities. In particular, 
there will be no dependency on the reference C libraries (except for testing). 

There will be no dependencies on native libraries in the core module, but other modules or
projects that use the core are free to use dependencies as needed. We will add runtime discovery to facilitate this, 
for example, to use HDF5 filters that link to native libraries.


### Testing

We use the Java [Foreign Function & Memory API](https://docs.oracle.com/en/java/javase/21/core/foreign-function-and-memory-api.html)
for testing against the Netcdf, HDF5, and HDF4 C libraries. 
With these tools we can be confident that our library gives the same results as the reference libraries.

Currently using 
* HDF5 library version: 1.14.6.
* netcdf-c library version 4.10.0-development of May 23 2025

Currently we have this test coverage from core/test:

````
 cdm      88% (1560/1764) LOC
 hdf4     84% (1743/2071) LOC
 hdf5     81% (2278/2792) LOC
 netcdf3  77% (230/297) LOC
 ````

The core library has ~6500 LOC.

More and deeper test coverage is provided in the clibs module, which compares netchdf metadata and data against
the Netcdf, HDF5, and HDF4 C libraries. The clibs module is not part of the released netchdf library and is 
only supported for test purposes.

Currently we have 1470 test files in the core test suite:

````
hdf-eos2  = 267 files
hdf-eos5  = 18 files
hdf4      = 205 files
hdf5      = 113 files
netcdf3   = 664 files
netcdf3.2 = 81 files
netcdf3.5 = 1 files
netcdf4   = 121 files

total # files = 1470
````
We need to get representative samples of recent files for improved testing and code coverage.

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
* _Datatype.CHAR_: All Attributes of type CHAR are assumed to be Strings. All Variables of type CHAR return data as
  ArrayUByte. Call _data.makeStringsFromBytes()_ to turn this into Strings with the array rank reduced by one.
  * Netcdf-3 does not have STRING or UBYTE types. In practice, CHAR is used for either. 
  * Netcdf-4/HDF5 library encodes CHAR values as HDF5 string type with elemSize = 1, so we use that convention to detect 
    legacy CHAR variables in HDF5 files. NC_CHAR should not be used in Netcdf-4, use NC_UBYTE or NC_STRING.
  * HDF4 does not have a STRING type, but does have signed and unsigned CHAR, and signed and unsigned BYTE. 
    We map both signed and unsigned to Datatype.CHAR and handle it as above (Attributes are Strings, Variables are UBytes).
* _Datatype.STRING_ is always variable length, regardless of whether the data in the file is variable or fixed length.

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