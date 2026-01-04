#ifndef _minispark_h_
#define _minispark_h_

#include <pthread.h>
#include <time.h>
#include <inttypes.h>

#define MAXDEPS (2)
#define TIME_DIFF_MICROS(start, end) \
  (((end).tv_sec - (start).tv_sec) * 1000000L + (((end).tv_nsec - (start).tv_nsec) / 1000))


struct RDD;
struct List;
struct TaskPQ;
struct ThreadPool;

typedef struct RDD RDD;
typedef struct List List;
typedef struct TaskPQ TaskPQ;
typedef struct ThreadPool ThreadPool;

typedef void *(*Mapper)(void *arg);
typedef int (*Filter)(void *arg, void *pred);
typedef void *(*Joiner)(void *arg1, void *arg2, void *ctx);
typedef unsigned long (*Partitioner)(void *arg, int numpartitions, void *ctx);
typedef void (*Printer)(void *arg);


typedef enum
{
  MAP,
  FILTER,
  JOIN,
  PARTITIONBY,
  FILE_BACKED
} Transform;


struct RDD
{
  Transform trans;  
  void *fn;     
  void *ctx;        
  List *partitions; 
  int numpartitions;

  RDD *dependencies[MAXDEPS];
  int numdependencies;

  pthread_mutex_t lock;
  int dependencies_remaining;
  int partitions_remaining;
  List *dependents;

  int executed;
  int id; 
  int refcount;
};

typedef struct
{
  struct timespec created;
  struct timespec scheduled;
  size_t duration; 
  RDD *rdd;
  int pnum;
} TaskMetric;

// Task structure
typedef struct
{
  RDD *rdd;
  int pnum;
  TaskMetric *metric;
} Task;

// Task Priority Queue structure
struct TaskPQ
{
  Task **tasks;
  int head;
  int tail;
  int num_tasks;
  int buffer;
  pthread_mutex_t lock;
  pthread_cond_t empty;
  pthread_cond_t full;
};

// Thread pool structure
struct ThreadPool
{
  pthread_t *threads;
  int num_threads;
  TaskPQ *queue;
  pthread_mutex_t lock;
  pthread_cond_t is_done;
  int curr_tasks;
  int stop;
};

////////// List Functions //////////
List *list_init(int capacity);
void list_add_elem(List *list, void *item);
void *list_get(List *list, int index);
void list_free(List *list);
void seek_to_start(List *list);
void *seek_next(List *list);
int list_count(List *list);

////////// TaskPQ Functions //////////
TaskPQ *taskpq_init(int capacity);
void taskpq_append(TaskPQ *pq, Task *t);
Task *taskpq_pop(TaskPQ *pq);

////////// Thread Pool Functions //////////
void threadpool_init(int numthreads);
void thread_pool_destroy(void);
void thread_pool_wait(void);
void thread_pool_submit(Task *task);

//////// actions ////////

// Return the total number of elements in "dataset"
int count(RDD* dataset);

// Print each element in "dataset" using "p".
// For example, p(element) for all elements.
void print(RDD* dataset, Printer p);

//////// transformations ////////

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation.
RDD* map(RDD* rdd, Mapper fn);

// Create an RDD with "rdd" as its dependency and "fn"
// as its transformation. "ctx" should be passed to "fn"
// when it is called as a Filter
RDD* filter(RDD* rdd, Filter fn, void* ctx);

// Create an RDD with two dependencies, "rdd1" and "rdd2"
// "ctx" should be passed to "fn" when it is called as a
// Joiner.
RDD* join(RDD* rdd1, RDD* rdd2, Joiner fn, void* ctx);

// Create an RDD with "rdd" as a dependency. The new RDD
// will have "numpartitions" number of partitions, which
// may be different than its dependency. "ctx" should be
// passed to "fn" when it is called as a Partitioner.
RDD* partitionBy(RDD* rdd, Partitioner fn, int numpartitions, void* ctx);

// Create an RDD which opens a list of files, one per
// partition. The number of partitions in the RDD will be
// equivalent to "numfiles."
RDD* RDDFromFiles(char* filenames[], int numfiles);

//////// MiniSpark ////////
// Submits work to the thread pool to materialize "rdd".
void execute(RDD* rdd);

// Creates the thread pool and monitoring thread.
void MS_Run();

// Waits for work to be complete, destroys the thread pool, and frees
// all RDDs allocated during runtime.
void MS_TearDown();


#endif // __minispark_h__
