#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sched.h>
#include <stdarg.h>
#include <unistd.h>
#include "lib.h"
#include "minispark.h"
#include <string.h>
#define FILENAMESIZE 100
#define GRANULARITY 8

int RowDividable(void* arg, void* needle) {
    
    struct row* row = (struct row*)arg;
    if (atoi(row->cols[0])%128) {
        return 0; //should be very rare
    }
    return 1;
}


// --- Function to read the complete file using mmap ---
// This function opens the file, obtains its size, mmaps it, then copies its content
// into a dynamically allocated buffer (with a null terminator) which is returned.
char *read_serialized_file(const char *filename) {
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        perror("open");
        return NULL;
    }
    
    struct stat st;
    if (fstat(fd, &st) < 0) {
        perror("fstat");
        close(fd);
        return NULL;
    }
    
    size_t filesize = st.st_size;
    if (filesize == 0) {
        fprintf(stderr, "Error: File is empty.\n");
        close(fd);
        return NULL;
    }
    
    // Memory-map the file for efficient reading.
    char *mapped = mmap(NULL, filesize, PROT_READ, MAP_PRIVATE, fd, 0);
    if (mapped == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return NULL;
    }
    
    // Allocate a buffer with one extra byte for the null terminator.
    char *buffer = malloc(filesize + 1);
    if (buffer == NULL) {
        perror("malloc");
        munmap(mapped, filesize);
        close(fd);
        return NULL;
    }
    
    memcpy(buffer, mapped, filesize);
    buffer[filesize] = '\0';  // Ensure the data is null-terminated.
    
    munmap(mapped, filesize);
    close(fd);
    return buffer;
}

struct sumjoin_ctx sctx = {0,1};
struct colpart_ctx pctx = {0};

// --- Helper: recursively deserialize the RDD tree ---
// 'tokens' is an array of token strings obtained from the serialized RDD.
// 'idx' points to the current token index (passed by reference so that it is updated).
RDD* deserialize_rdd_helper(char** tokens, int *idx) {
    if (!tokens[*idx]) {
        return NULL;
    }
    // Read the transformation number.
    int trans = atoi(tokens[*idx]);
    (*idx)++;
    
    if (trans == 0) {
        // For transformation 0 (map) the serializer printed two extra fields (backed and numpartitions)
        // that we don’t actually use to reconstruct the function chain.
        // Skip the next two tokens.
        int filenum = atoi(tokens[*idx]);
        (*idx)++;
        (*idx)++;  // skip the 'numpartitions' token
        
        // Build the map chain as specified:
        // map(map(RDDFromFiles("filename", 1), GetLines), SplitCols)
        
        char* filelist[GRANULARITY];
        for (int i = 0; i< GRANULARITY; i++) {
            filelist[i] = calloc(FILENAMESIZE, 1);
            sprintf(filelist[i],  "./test_files/hiddenvals%d.txt", ((filenum)/(GRANULARITY))*GRANULARITY + (filenum+i)%GRANULARITY);
        }

        RDD* source = RDDFromFiles(filelist, GRANULARITY);
        RDD* inner = map(source, GetLines);
        return map(inner, SplitCols);
    }
    else if (trans == 1) {
        // For filter: deserialize its single dependency.
        RDD* dep = deserialize_rdd_helper(tokens, idx);
        return filter(dep, RowDividable, NULL); // literal "0" as the context
    }
    else if (trans == 2) {
  
        // For join: deserialize two dependencies.
        RDD* dep0 = deserialize_rdd_helper(tokens, idx);
        RDD* dep1 = deserialize_rdd_helper(tokens, idx);
        return join(dep0, dep1, SumJoin, (void*)&sctx);
    }
    else if (trans == 3) {
        // For partitionBy: the next token holds the number of partitions.
        int numPartitions = atoi(tokens[*idx]);
        (*idx)++;
        RDD* dep = deserialize_rdd_helper(tokens, idx);
        return partitionBy(dep, ColumnHashPartitioner, numPartitions, &pctx);
    }
    else {
        // Unrecognized transformation.
        fprintf(stderr, "Error: Unrecognized transformation code %d\n", trans);
        return NULL;
    }
}

// --- Main deserialization function ---
// This function tokenizes the input serialized string (assumed to be whitespace delimited)
// and calls the helper to recursively build the RDD tree.
RDD* deserialize_rdd(const char* serialized) {
    // Make a copy so that strtok doesn’t modify the caller’s string.
    char* str = strdup(serialized);
    if (str == NULL) {
        perror("strdup");
        return NULL;
    }
    
    // First, count tokens (this simple approach assumes tokens are separated by whitespace)
    int tokenCap = 16;
    int tokenCount = 0;
    char** tokens = malloc(tokenCap * sizeof(char*));
    if (tokens == NULL) {
        free(str);
        perror("malloc");
        return NULL;
    }
    char *saveptr1;
    // Tokenize using strtok (tokens will be separated by spaces, tabs, or newlines).
    char* token = strtok_r(str, " \t\n", &saveptr1);
    while (token != NULL) {
        if (tokenCount >= tokenCap) {
            tokenCap *= 2;
            tokens = realloc(tokens, tokenCap * sizeof(char*));
            if (tokens == NULL) {
                free(str);
                perror("realloc");
                return NULL;
            }
        }
        tokens[tokenCount++] = token;
        token = strtok_r(NULL, " \t\n", &saveptr1);
    }
    
    // Use an index to keep track of parsing progress.
    int idx = 0;
    RDD* result = deserialize_rdd_helper(tokens, &idx);
    
    // Cleanup
    free(tokens);
    free(str);
    return result;
}


int main(int argc, char** argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <serialized_file>\n", argv[0]);
        return EXIT_FAILURE;
    }
    
    // Read serialized RDD data from the file.
    char *serialized_data = read_serialized_file(argv[1]);
    if (serialized_data == NULL) {
        fprintf(stderr, "Failed to read serialized data from file.\n");
        return EXIT_FAILURE;
    }
    
    // Reconstruct the RDD from the serialized data.
    RDD* rdd = deserialize_rdd(serialized_data);
    if (rdd == NULL) {
        fprintf(stderr, "Deserialization failed.\n");
        free(serialized_data);
        return EXIT_FAILURE;
    }

    MS_Run();
    print(rdd, RowPrinter);
    MS_TearDown();

    free(serialized_data);
    return EXIT_SUCCESS;
}