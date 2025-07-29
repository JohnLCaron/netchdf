# building
6/14/25


## Building

`./gradlew clean assemble`

## JVM

Artifacts are at

* `core/build/libs/netchdf-XXX.jar`

## linuxX64

Artifacts are at
* `core/build/bin/linuxX64/releaseShared/libnetchdf_api.h`
* `core/build/bin/linuxX64/releaseShared/libnetchdf.so`

### add library to LD_LIBRARY_PATH (or copy to existing) (or use current directory)

````
For example, choose one:

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/dev/core/build/bin/linuxX64/releaseShared

sudo cp core/build/bin/linuxX64/debugShared/libnetchdf.so /usr/local/lib

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:.
````

### create test c program

````
cd core/src/nativeTest/c
gcc main.c \
  -I/home/stormy/dev/github/netcdf/netchdf/core/build/bin/linuxX64/debugShared \
  -L/home/stormy/dev/github/netcdf/netchdf/core/build/bin/linuxX64/debugShared \
  -lnetchdf


./a.out \
  -L/home/stormy/dev/github/netcdf/netchdf/core/build/bin/linuxX64/debugShared \
  -lnetchdf
````

### Example

````
~:$ echo $LD_LIBRARY_PATH
/usr/local/lib:/home/stormy/install/HDF_Group/HDF5/1.14.6/lib:.

cd core/src/nativeTest/c
cp /home/stormy/dev/github/netcdf/netchdf/core/build/bin/linuxX64/debugShared/libnetchdf.so .

./a.out \
  -L/home/stormy/dev/github/netcdf/netchdf/core/build/bin/linuxX64/debugShared \
  -lnetchdf

output is currently:

file simple_xy.nc
netcdf3 simple_xy.nc {
  dimensions:
    x = 6 ;
    y = 12 ;
  variables:
    int data(x, y) ;
}
get_varName=data
rank=2
 0 == 6
 1 == 12

get_varName=data
nelems=72
 0 == 6
 1 == 12

 0 == 0
 1 == 1
 2 == 2
 3 == 3
 4 == 4
 5 == 5
 6 == 6
 7 == 7
  ...

Hints:
* if you keep core dumping before anything else, make sure your have copied the latest and correct library to /usr/local/lib

### Example2

````
cd ~/dev/github/netcdf/netchdf/core/ctest
sudo cp /home/stormy/dev/github/netcdf/netchdf/core/build/bin/linuxX64/debugShared/libnetchdf.so /usr/local/lib
gcc main.c   -I/home/stormy/dev/github/netcdf/netchdf/core/build/bin/linuxX64/debugShared   -lnetchdf
./a.out   -lnetchdf
````

