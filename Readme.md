# netchdf
_last updated: 5/14/2025_

This is a rewrite in Kotlin of parts of the devcdm and netcdf-java libraries. 

The intention is to create a maintainable, read-only, pure JVM library allowing full access to 
netcdf3, netcdf4, hdf4, hdf5, hdf-eos2 and hdf-eos5 data files. 

Please contact me if you'd like to help out. Especially needed are test datasets from all the important data archives!!

### Why this library? 

There is so much important scientific data stored in the NetCDF and HDF file formats, that those formats will 
never go away. It is important that there be maintainable, independent libraries to read these files forever.

The Netcdf-Java library prototyped a "Common Data Model" (CDM) to provide a single API to access various file formats. 
The netcdf* and hdf* file formats are similar enough to make a common API a practical and useful goal. 
By focusing on read-only access to just these formats, the API and the code are kept simple.

In short, a library that focuses on simplicity and clarity is a safeguard for the huge investment in these
scientific datasets.

### Why do we need an alternative to the standard reference libraries?

The reference libraries are well maintained but complex. They are coded in C, which is a difficult language to master
and keep bug free, with implication for memory safety and security. The libraries require various machine and OS dependent
toolchains. Shifts in funding could wipe out much of the institutional knowledge needed to maintain them.

The HDF file formats are overly complicated, which impacts code complexity and clarity. The data structures do not
always map to a user understandable data model. Semantics are left to data-writers to document (or not). 
While this problem isn't specific to HDF file users, it is exacerbated by a "group of messages" design approach. 

The HDF4 C library is a curious hodgepodge of disjointed APIs. The HDF5 API is better and the Netcdf4 API much better.
But all suffer from the limitations of the C language, the difficulty of writing good documentation for all skill levels 
of programmers, and the need to support legacy APIs. 

HDF-EOS uses an undocumented "Object Descriptor Language (ODL)" text format, which adds a dependency on the SDP Toolkit 
and possibly other libraries. These toolkits also provide functionality such as handling projections and coordinate system 
conversions, and arguably its impossible to process HDF-EOS without them. So the value added here by an independent 
library for data access is less clear. For now, we will provide a "best-effort" to expose the internal 
contents of the file.

Currently, the Netcdf-4 and HDF4 libraries are not thread safe, even when operating on different files.
The HDF5 library can be built with MPI-IO for parallel file systems. The serial HDF5 library is apparently thread safe 
but does not support concurrent reading. These are serious limitations for high performance, scalable applications.

Our library tries to ameliorate these problems for scientists and the interested public to access the data without
having to become specialists in the file formats and legacy APIs.

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

Its possible we can use Kotlin coroutines to speed up performance bottlenecks. TBD.

### Testing

We will use the Foreign Function & Memory API for testing against the Netcdf, HDF5, and HDF4 C libraries. 
With these tools we can be confident that our library gives the same results as the reference libraries.

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

Currently we have ~1500 test files:

````
 hdf4      = 205 files
 hdf-eos2  = 267 files
 hdf5      = 113 files
 hdf-eos5  =  18 files
 netcdf3   = 664 files
 netcdf3.2 =  81 files
 netcdf3.5 =   1 files
 netcdf4   = 121 files
 ````

We need to get representative samples of recent files for improved testing and code coverage.

### Scope

We have the goal to give read access to all the content in NetCDF, HDF5, HDF4, and HDF-EOS files. 

The library will be thread-safe for reading multiple files concurrently.

We are focussing on earth science data, and dont plan to support other uses except as a byproduct.

We do not plan to provide write capabilities.

### Data Model notes

#### Type Safety and Generics

Also see [Netchdf core UML](https://docs.google.com/drawings/d/1lkouJBUG5uy8aUtbKfAZN9D5h_v22JNWf6QUQWjPNBc)

Datatype\<T\>, Attribute\<T\>, Variable\<T\>, StructureMember\<T\>, Array\<T\> and ArraySection\<T\> are all generics, 
with T indicating the data type returned when read, eg:

````
    fun <T> readArrayData(v2: Variable<T>, section: SectionPartial? = null) : ArrayTyped<T>
````

#### Datatype
* __Datatype.ENUM__ returns an array of the corresponding UBYTE/USHORT/UINT. Call _data.convertEnums()_ to turn this into
  an ArrayString of corresponding enum names.
* __Datatype.CHAR__: All Attributes of type CHAR are assumed to be Strings. All Variables of type CHAR return data as
  ArrayUByte. Call _data.makeStringsFromBytes()_ to turn this into Strings with the array rank reduced by one.
  * _Netcdf-3_ does not have STRING or UBYTE types. In practice, CHAR is used for either. 
  * _Netcdf-4/HDF5_ library encodes CHAR values as HDF5 string type with elemSize = 1, so we use that convention to detect 
    legacy CHAR variables in HDF5 files. NC_CHAR should not be used in Netcdf-4, use NC_UBYTE or NC_STRING.
  * _HDF4_ does not have a STRING type, but does have signed and unsigned CHAR, and signed and unsigned BYTE. 
    We map both signed and unsigned to Datatype.CHAR and handle it as above (Attributes are Strings, Variables are UBytes).
* __Datatype.STRING__ is variable length, whether the file storage is variable or fixed length.

#### Typedef
Unlike Netcdf-Java, we follow Netcdf-4 "user defined types" and add typedefs for Compound, Enum, Opaque, and Vlen.
* __Datatype.ENUM__ typedef has a map from integer to name (same as Netcdf-Java)
* __Datatype.COMPOUND__ typedef contains a description of the members of the Compound (aka Structure).
* __Datatype.OPAQUE__ typedef may contain the byte length of OPAQUE data.
* __Datatype.VLEN__ typedef has the base type. An array of VLEN may have different lengths for each object.

#### Dimension
* Unlike Netcdf-3 and Netcdf-4, dimensions may be "anonymous", in which case they have a length but not a name, and are 
local to the variable they are referenced by.
* There are no UNLIMITED dimensions. These are unneeded since we do not support writing.

#### Compare with HDF5 data model
* Creation order is ignored
* Not including symbolic links in a group, as these point to an existing dataset (variable)
* Opaque: hdf5 makes arrays of Opaque all the same size, which gives up some of its usefulness. If there's a need,
  we will allow Opaque(*) indicating that the sizes can vary.
* Attributes can be of type REFERENCE, with value the full path name of the referenced dataset.

#### Compare with HDF4 data model
* All data access is unified under the netchdf API

#### Compare with HDF-EOS data model
* The _StructMetadata_ ODL is gathered and applied to the file header metadata as well as possible. 
  Contact us with example files if you see something we are missing.

## Elevator blurb

An independent implementation of HDF4/HDF5/HDF-EOS in Kotlin.

I am working on an independent library implementation of HDF4/HDF5/HDF-EOS in Kotlin 
[here](https://github.com/JohnLCaron/netchdf). 
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