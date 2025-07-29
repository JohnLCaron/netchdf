#include "libnetchdf_api.h"
#include "stdio.h"
#include "math.h"
#include <sys/time.h>

void showDuration(char *what, struct timespec *start_time, struct timespec *end_time) {
    long seconds = end_time->tv_sec - start_time->tv_sec;
    long nanoseconds = end_time->tv_nsec - start_time->tv_nsec;

    if (nanoseconds < 0) {
        seconds--;
        nanoseconds += 1000000000; // Add 1 billion nanoseconds (1 second)
    }

    long millis = nanoseconds/(1000 * 1000);
    printf("%s took = %ld secs and %ld nanos \n", what, seconds, nanoseconds);
}

int main(int argc, char** argv) {
    // Obtain reference for calling Kotlin/Native functions
    libnetchdf_ExportedSymbols* lib = libnetchdf_symbols();

    const char* response = lib->kotlin.root.com.sunya.netchdfc.version();
    printf("%s\n", response);
    lib->DisposeString(response);

    ////  libnetchdf_kref_com_sunya_cdm_api_Netchdf (*openNetchdfFileC)(const char* filename);
    const char* filename = "/home/stormy/dev/github/netcdf/netchdf/core/src/commonTest/data/netcdf4/tiling.nc4";
    // const char* filename = "/home/all/testdata/cdmUnitTest/formats/netcdf4/espresso_his_20130913_0000_0007.nc";
    libnetchdf_kref_com_sunya_cdm_api_Netchdf netchdf = lib->kotlin.root.com.sunya.netchdfc.openNetchdfFile(filename);

    const char* cdl = lib->kotlin.root.com.sunya.cdm.api.cdl(netchdf);

    printf("file %s\n", filename);
    printf("%s\n", cdl);
    lib->DisposeString(cdl);

    const char* varname = "Turbulence_SIGMET_AIRMET";
    // const char* varname = "vbar"; //
    libnetchdf_kref_com_sunya_netchdfc_VariableC variableC = lib->kotlin.root.com.sunya.netchdfc.openVariable(netchdf, varname);
    printf("get_varName=%s\n", lib->kotlin.root.com.sunya.netchdfc.VariableC.get_varName(variableC));
    int rank = lib->kotlin.root.com.sunya.netchdfc.VariableC.get_rank(variableC);
    printf("rank=%d\n", rank);

    void *vshape = lib->kotlin.root.com.sunya.netchdfc.VariableC.get_pinnedShape(variableC);
    long *shape = (long *) vshape;
    printf("varshape\n");
    for (int idx=0; idx < rank; idx++) {
        printf(" %d == %ld\n", idx, *(shape+idx));
    }
    printf("\n");

    struct timeval timecheck;
    gettimeofday(&timecheck, NULL);
    long start = (long)timecheck.tv_sec * 1000 + (long)timecheck.tv_usec / 1000;

    /*************** heres the meat */
    libnetchdf_kref_com_sunya_netchdfc_VariableDataFloat variableData = lib->kotlin.root.com.sunya.netchdfc.readVariableFloat(netchdf, varname);

    gettimeofday(&timecheck, NULL);
    long end = (long)timecheck.tv_sec * 1000 + (long)timecheck.tv_usec / 1000;
    printf("%ld milliseconds elapsed\n", (end - start));

/*
    printf("readVariable=%s\n", lib->kotlin.root.com.sunya.netchdfc.VariableDataFloat.get_varName(variableData));
    int nelems = lib->kotlin.root.com.sunya.netchdfc.VariableDataFloat.get_nelems(variableData);
    printf("nelems=%d\n", nelems);

    void *vdshape = lib->kotlin.root.com.sunya.netchdfc.VariableDataFloat.get_pinnedShape(variableData);
    int *dshape = (int *) vdshape;
    for (int idx=0; idx < rank; idx++) {
        printf(" %d == %d\n", idx, *(dshape+idx));
    }
    printf("\n"); */

    int nelems = lib->kotlin.root.com.sunya.netchdfc.VariableDataFloat.get_nelems(variableData);

    void *vpdata = lib->kotlin.root.com.sunya.netchdfc.VariableDataFloat.get_pinnedData(variableData);
    float *pdata = (float *) vpdata;
    double sum = 0.0;
    int count = 0;
    for (int idx=0; idx < nelems; idx++) {
        float value = *(pdata+idx);
        // if (idx < 10) printf(" %d %g\n", idx, value);
        // if (isfinite(value) == 0) printf(" %d %g\n", idx, value);
        if (isfinite(value) != 0) sum += value;
        // if (idx % 100000 == 0) printf("sum = %g\n", sum);
        // if (idx % 100000 == 0) printf("sum = %g\n", sum);
        count++;
    }
    end = (long)timecheck.tv_sec * 1000 + (long)timecheck.tv_usec / 1000;
    printf("%ld milliseconds elapsed\n", (end - start));

    printf("total = %g\n", sum);
    printf("count elements = %d\n", count);

    return 0;
}

/*
    libnetchdf_kref_com_sunya_cdm_api_Variable variable = lib->kotlin.root.com.sunya.netchdfc.findVariable(netchdf, "data");

    libnetchdf_kref_com_sunya_netchdfc_ArrayIntSection arrayIntSection = lib->kotlin.root.com.sunya.netchdfc.readArrayInt(netchdf, variable);

    int rank = lib->kotlin.root.com.sunya.netchdfc.ArrayIntSection.get_rank(arrayIntSection);
    printf("rank=%d\n", rank);
    int nelems = lib->kotlin.root.com.sunya.netchdfc.ArrayIntSection.get_nelems(arrayIntSection);
    printf("nelems=%d\n", nelems);

    libnetchdf_kref_kotlin_IntArray shapeIntArray = lib->kotlin.root.com.sunya.netchdfc.ArrayIntSection.get_shape(arrayIntSection);
    void *shapePtr = shapeIntArray.pinned;
    int *shapePtr1 = (int *) shapePtr;
    for (int idx=0; idx < rank; idx++) {
        printf(" %d == %d\n", idx, *(shapePtr1+idx));
    }
    printf("\n");

    // libnetchdf_kref_kotlin_IntArray (*get_values)(libnetchdf_kref_com_sunya_cdm_array_ArrayInt thiz);
    //libnetchdf_kref_kotlin_IntArray intarray = arrayint->get_values(arrayint);
    //libnetchdf_kref_kotlin_IntArray intarray = lib->kotlin.root.com.sunya.netchdfc.ArrayIntSection.get_array(arrayIntSection);
    //int *data = (int *) intarray.pinned;

    //for (int idx=0; idx < 10; idx++) {
    //    printf(" %d == %d\n", idx, data[idx]);
    //}

    //lib->DisposeStablePointer(intarray.pinned);
    lib->DisposeStablePointer(shapeIntArray.pinned);
    lib->DisposeStablePointer(arrayIntSection.pinned);
    lib->DisposeStablePointer(variable.pinned);
    lib->DisposeStablePointer(netchdf.pinned);

    return 0;
    */
