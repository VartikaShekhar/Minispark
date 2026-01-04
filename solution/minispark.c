#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sched.h>
#include <pthread.h>
#include "minispark.h"
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>


void free_rdd(RDD *rdd);
void chg_depts(RDD *finished);
RDD *create_rdd(int numdeps, Transform t, void *fn, ...);
static ThreadPool *pool = NULL;
static List *metrics_queue = NULL;
static pthread_mutex_t metrics_lock;
static pthread_cond_t metrics_cond;
static struct timespec metrics_start_time;
static int metrics_stop = 0;
static pthread_t metrics_thread;


static int next_rdd_id = 0;
static pthread_mutex_t global_id_lock = PTHREAD_MUTEX_INITIALIZER;

static List *rdd_list = NULL;
static pthread_mutex_t all_rdds_lock = PTHREAD_MUTEX_INITIALIZER;


/* A special mapper */
void *identity(void *arg)
{
    return arg;
}
typedef struct List
{
    void **data;
    int size;
    int capacity;
    int iter_index;
} List;

List *list_init(int capacity)
{
    if (capacity <= 0)
    {
        fprintf(stderr, "wrong initial capacity\n");
        exit(1);
    }

    List *list = malloc(sizeof(List));
    if (!list)
    {
        perror("malloc");
        exit(1);
    }

    list->data = malloc(sizeof(void *) * capacity);
    if (!list->data)
    {
        perror("malloc");
        free(list);
        exit(1);
    }
    list->capacity = capacity;
    list->iter_index = 0;
    list->size = 0;
    return list;
}

void list_add_elem(List *list, void *item)
{
    if (list->size >= list->capacity)
    {
        int new_capacity = list->capacity * 1.5;
        void **new_data = realloc(list->data, new_capacity * sizeof(void *));
        if (!new_data)
        {
            perror("realloc");
            exit(1);
        }
        list->data = new_data;
        list->capacity = new_capacity;
    }
    list->data[list->size++] = item;
}

void *list_get(List *list, int index)
{
    if (!list)
        return NULL;
    else if (index < 0 || index >= list->size)
        return NULL;
    return list->data[index];
}

void list_free(List *list)
{
    if (!list)
        return;
    free(list->data);
    free(list);
}

void seek_to_start(List *list)
{
    if (list)
        list->iter_index = 0;
}

void *seek_next(List *list)
{
    if (!list || list->iter_index >= list->size)
        return NULL;
    return list->data[list->iter_index++];
}

int list_count(List *list)
{
    if (list)
    {
        return list->size;
    }
    else
    {
        return 0;
    }
}


void *print_formatted_metric()
{
    FILE *logf = fopen("metrics.log", "w");
    if (!logf)
    {
        perror("fopen");
        return NULL;
    }
    pthread_mutex_lock(&metrics_lock);
    while (1)
    {
        while (list_count(metrics_queue) == 0 && !metrics_stop)
        {
            pthread_cond_wait(&metrics_cond, &metrics_lock);
        }
        if (metrics_stop && list_count(metrics_queue) == 0)
        {
            pthread_mutex_unlock(&metrics_lock);
            break;
        }
        TaskMetric *m = list_get(metrics_queue, 0);

        if (m)
        {
            int count = metrics_queue->size;
            for (int i = 1; i < count; i++)
            {
                metrics_queue->data[i - 1] = metrics_queue->data[i];
            }
            metrics_queue->size--;
        }
        pthread_mutex_unlock(&metrics_lock);

        if (m->rdd)
        {
            Transform t = m->rdd->trans;
            if (t >= 0 && t <= 3)
            {
                fprintf(logf,
                        "RDD %p Part %d Trans %d -- creation %10jd.%06ld, scheduled %10jd.%06ld, execution (usec) %ld\n",
                        m->rdd, m->pnum, m->rdd->trans,
                        (intmax_t)m->created.tv_sec, m->created.tv_nsec / 1000,
                        (intmax_t)m->scheduled.tv_sec, m->scheduled.tv_nsec / 1000,
                        m->duration);
                fflush(logf);
            }
        }
        free(m);
        pthread_mutex_lock(&metrics_lock);
    }
    fclose(logf);
    return NULL;
}

static void init_rdd(RDD *rdd)
{
    pthread_mutex_init(&(rdd->lock), NULL);
    rdd->executed = 0;
    rdd->partitions_remaining = 0;
    rdd->dependencies_remaining = rdd->numdependencies;
    rdd->dependents = list_init(4);
}

static void init_partitions(RDD *rdd)
{
    if (rdd->partitions) return;

    int max_parts = 1;
    for (int i = 0; i < rdd->numdependencies; i++)
    {
        int dep_parts = rdd->dependencies[i]->partitions->size;
        if (dep_parts > max_parts) max_parts = dep_parts;
    }

    List *parts = malloc(sizeof(List));
    if (!parts)
    {
        perror("malloc partitions");
        return;
    }

    parts->data = calloc(max_parts, sizeof(void *));
    if (!parts->data)
    {
        perror("calloc partition data");
        free(parts);
        return;
    }
    parts->size = max_parts;
    rdd->partitions = parts;
    rdd->numpartitions = max_parts;
}

int max(int a, int b)
{
    return a > b ? a : b;
}

RDD *map(RDD *dep, Mapper fn)
{
    return create_rdd(1, MAP, fn, dep);
}

RDD *filter(RDD *dep, Filter fn, void *ctx)
{
    RDD *rdd = create_rdd(1, FILTER, fn, dep);
    rdd->ctx = ctx;
    return rdd;
}

RDD *partitionBy(RDD *dep, Partitioner fn, int numpartitions, void *ctx)
{
    RDD *rdd = create_rdd(1, PARTITIONBY, fn, dep);
    rdd->partitions = list_init(numpartitions);
    if (!rdd->partitions)
    {
        fprintf(stderr, "partitions alloc\n");
        exit(1);
    }
    int i = 0;
    while (i < numpartitions)
    {
        List *part = list_init(16);
        if (!part)
        {
            fprintf(stderr, "partition %d alloc\n", i);
            exit(1);
        }
        list_add_elem(rdd->partitions, part);
        i++;
    }
    rdd->numpartitions = numpartitions;
    rdd->ctx = ctx;
    return rdd;
}

RDD *create_rdd(int numdeps, Transform t, void *fn, ...)
{
    RDD *rdd = malloc(sizeof(RDD));
    if (!rdd)
    {
        fprintf(stderr, "error mallocing new rdd\n");
        static RDD temp;
        return &temp;
    }
    rdd->trans = t;
    rdd->fn = fn;
    rdd->ctx = NULL;
    rdd->numpartitions = 0;
    rdd->partitions = NULL;
    rdd->numdependencies = numdeps;
    rdd->refcount = 1; 

    va_list args;
    va_start(args, fn);
    for (int i = 0; i < numdeps; i++)
    {
        RDD *dep = va_arg(args, RDD *);
        rdd->dependencies[i] = dep;


        pthread_mutex_lock(&dep->lock);
        if (!dep->dependents)
        {
            dep->dependents = list_init(4);
            if (!dep->dependents)
                fprintf(stderr, "dependents list initialize\n");
        }
        list_add_elem(dep->dependents, rdd);
        dep->refcount++;  
        pthread_mutex_unlock(&dep->lock);
    }
    va_end(args);

    init_rdd(rdd);
    pthread_mutex_lock(&global_id_lock);
    rdd->id = next_rdd_id++;
    pthread_mutex_unlock(&global_id_lock);

    pthread_mutex_lock(&all_rdds_lock);
    if (!rdd_list)
    {
        rdd_list = list_init(16);
        if (!rdd_list)
            fprintf(stderr, "RDD list initialization\n");
    }
    list_add_elem(rdd_list, rdd);
    pthread_mutex_unlock(&all_rdds_lock);

    return rdd;
}

RDD *join(RDD *dep1, RDD *dep2, Joiner fn, void *ctx)
{
    RDD *rdd = create_rdd(2, JOIN, fn, dep1, dep2);
    rdd->ctx = ctx;
    return rdd;
}

RDD *RDDFromFiles(char **filenames, int numfiles)
{
    RDD *rdd = malloc(sizeof(RDD));
    if (!rdd)
    {
        perror("malloc");
        exit(1);
    }
    rdd->partitions = list_init(numfiles);
    for (int i = 0; i < numfiles; i++)
    {
        FILE *fp = fopen(filenames[i], "r");
        if (!fp)
        {
            perror("fopen");
            exit(1);
        }
        list_add_elem(rdd->partitions, fp);
    }
    rdd->numdependencies = 0;
    rdd->trans = FILE_BACKED;
    rdd->fn = (void *)identity;
    rdd->numpartitions = numfiles;
    rdd->refcount = 1;

    init_rdd(rdd);

    pthread_mutex_lock(&all_rdds_lock);
    if (rdd_list == NULL)
    {
        rdd_list = list_init(16);
    }
    list_add_elem(rdd_list, rdd);
    pthread_mutex_unlock(&all_rdds_lock);

    pthread_mutex_lock(&global_id_lock);
    rdd->id = next_rdd_id++;
    pthread_mutex_unlock(&global_id_lock);

    return rdd;
}


void rdd_process_per_partition(RDD *rdd)
{
    pthread_mutex_lock(&(rdd->lock));
    if (rdd->executed)
    {
        pthread_mutex_unlock(&(rdd->lock));
        return;
    }
    rdd->executed = 1;

    if (rdd->partitions == NULL)
    {
        init_partitions(rdd);
    }

    int tasks = (rdd->trans == PARTITIONBY) ? 1 : list_count(rdd->partitions);
    rdd->partitions_remaining = tasks;
    pthread_mutex_unlock(&(rdd->lock));

    for (int i = 0; i < tasks; i++)
    {
        Task *task = malloc(sizeof(Task));
        if (!task)
        {
            perror("malloc");
            exit(1);
        }
        task->rdd = rdd;
        task->pnum = i;
        task->metric = malloc(sizeof(TaskMetric));
        if (!task->metric)
        {
            perror("malloc");
            exit(1);
        }
        clock_gettime(CLOCK_MONOTONIC, &(task->metric->created));
        task->metric->rdd = rdd;
        task->metric->pnum = i;

        thread_pool_submit(task);
    }
}

void chg_depts(RDD *finished)
{
    int count = list_count(finished->dependents);
    if (count == 0) return;
    List *ready = list_init(4);
    for (int i = 0; i < count; i++)
    {
        RDD *dep = list_get(finished->dependents, i);
        pthread_mutex_lock(&dep->lock);
        dep->dependencies_remaining--;
        int is_ready = (dep->dependencies_remaining == 0);
        pthread_mutex_unlock(&dep->lock);

        if (is_ready)
            list_add_elem(ready, dep);
    }
    for (int i = 0; i < ready->size; i++)
    {
        rdd_process_per_partition(list_get(ready, i));
    }
    list_free(ready);
}

void materialize(Task *task)
{
    clock_gettime(CLOCK_MONOTONIC, &task->metric->scheduled);

    RDD *rdd = task->rdd;
    int part_id = task->pnum;
    List *result = NULL;

    if (rdd->trans == MAP)
    {
        RDD *parent = rdd->dependencies[0];
        void *input = list_get(parent->partitions, part_id);
        Mapper map_fn = (Mapper)rdd->fn;

        result = list_init(16);

        if (parent->trans == FILE_BACKED)
        {
            FILE *fp = (FILE *)input;
            fseek(fp, 0, SEEK_SET);  
            clearerr(fp);
            void *item;
            while ((item = map_fn(fp)) != NULL)
            {
                list_add_elem(result, item);
            }
        }
        else
        {
            List *input_list = (List *)input;
            for (int i = 0; i < input_list->size; i++)
            {
                void *elem = list_get(input_list, i);
                void *res = map_fn(elem);
                if (res) list_add_elem(result, res);
            }
        }
        rdd->partitions->data[part_id] = result;
    }
    else if (rdd->trans == FILTER)
    {
        List *input = (List *)list_get(rdd->dependencies[0]->partitions, part_id);
        Filter filter_fn = (Filter)rdd->fn;

        result = list_init(16);
        for (int i = 0; i < input->size; i++)
        {
            void *elem = list_get(input, i);
            if (filter_fn(elem, rdd->ctx))
                list_add_elem(result, elem);
        }
        rdd->partitions->data[part_id] = result;
    }
    else if (rdd->trans == JOIN)
    {
        List *left = (List *)list_get(rdd->dependencies[0]->partitions, part_id);
        List *right = (List *)list_get(rdd->dependencies[1]->partitions, part_id);
        Joiner join_fn = (Joiner)rdd->fn;

        result = list_init(16);
        for (int i = 0; i < left->size; i++)
        {
            for (int j = 0; j < right->size; j++)
            {
                void *res = join_fn(list_get(left, i), list_get(right, j), rdd->ctx);
                if (res) list_add_elem(result, res);
            }
        }
        rdd->partitions->data[part_id] = result;
    }
    else if (rdd->trans == PARTITIONBY)
    {
        RDD *parent = rdd->dependencies[0];
        Partitioner part_fn = (Partitioner)rdd->fn;
        int total_parts = list_count(parent->partitions);

        List *merged = list_init(32);
        for (int i = 0; i < total_parts; i++)
        {
            List *sub = (List *)list_get(parent->partitions, i);
            for (int j = 0; j < sub->size; j++)
            {
                list_add_elem(merged, list_get(sub, j));
            }
        }

        for (int i = 0; i < merged->size; i++)
        {
            void *item = list_get(merged, i);
            unsigned long idx = part_fn(item, rdd->numpartitions, rdd->ctx);
            if (idx >= (unsigned long)rdd->numpartitions) idx = 0;
            List *target = (List *)rdd->partitions->data[idx];
            list_add_elem(target, item);
        }
        list_free(merged);
    }

    struct timespec finish;
    clock_gettime(CLOCK_MONOTONIC, &finish);
    task->metric->duration = TIME_DIFF_MICROS(task->metric->scheduled, finish);

    pthread_mutex_lock(&rdd->lock);
    rdd->partitions_remaining--;
    int all_done = (rdd->partitions_remaining == 0);
    pthread_mutex_unlock(&rdd->lock);

    if (all_done)
        chg_depts(rdd);

    pthread_mutex_lock(&metrics_lock);
    list_add_elem(metrics_queue, task->metric);
    pthread_cond_signal(&metrics_cond);
    pthread_mutex_unlock(&metrics_lock);

    free(task);
}


void *dispatch_task()
{
    while (1)
    {
        pthread_mutex_lock(&pool->queue->lock);
        while (pool->queue->num_tasks == 0 && !pool->stop)
        {
            pthread_cond_wait(&pool->queue->full, &pool->queue->lock);
        }
        if (pool->stop && pool->queue->num_tasks == 0)
        {
            pthread_mutex_unlock(&pool->queue->lock);
            pthread_exit(NULL);
        }
        Task *current_task = pool->queue->tasks[pool->queue->head];
        pool->queue->head = (pool->queue->head + 1) % pool->queue->buffer;
        pool->queue->num_tasks--;
        pthread_cond_signal(&pool->queue->empty);
        pool->curr_tasks++;
        pthread_mutex_unlock(&pool->queue->lock);

        if (current_task)
        {
            materialize(current_task);
        }

        pthread_mutex_lock(&pool->queue->lock);
        pool->curr_tasks--;
        if (pool->curr_tasks == 0 && pool->queue->num_tasks == 0)
        {
            pthread_cond_signal(&pool->is_done);
        }
        pthread_mutex_unlock(&pool->queue->lock);
    }
    return NULL;
}


TaskPQ *taskpq_init(int capacity)
{
    TaskPQ *pq = malloc(sizeof(TaskPQ));
    if (!pq)
    {
        perror("malloc error");
        exit(1);
    }

    pq->tasks = malloc(sizeof(Task *) * capacity);
    if (!pq->tasks)
    {
        perror("malloc error");
        exit(1);
    }

    pq->buffer = capacity;
    pq->head = 0;
    pq->tail = 0;
    pq->num_tasks = 0;

    pthread_mutex_init(&pq->lock, NULL);
    pthread_cond_init(&pq->empty, NULL);
    pthread_cond_init(&pq->full, NULL);
    return pq;
}

void taskpq_append(TaskPQ *pq, Task *t)
{
    pthread_mutex_lock(&pq->lock);
    while (pq->num_tasks == pq->buffer)
    {
        pthread_cond_wait(&pq->empty, &pq->lock);
    }
    pq->tasks[pq->tail] = t;
    pq->tail = (pq->tail + 1) % pq->buffer;
    pq->num_tasks++;
    pthread_cond_signal(&pq->full);
    pthread_mutex_unlock(&pq->lock);
}

Task *taskpq_pop(TaskPQ *pq)
{
    pthread_mutex_lock(&pq->lock);
    while (pq->num_tasks == 0)
    {
        pthread_cond_wait(&pq->full, &pq->lock);
    }
    Task *t = pq->tasks[pq->head];
    pq->head = (pq->head + 1) % pq->buffer;
    pq->num_tasks--;
    pthread_cond_signal(&pq->empty);
    pthread_mutex_unlock(&pq->lock);
    return t;
}

void threadpool_init(int numthreads)
{
    pool = calloc(1, sizeof(ThreadPool));
    if (!pool)
    {
        perror("malloc thread_pool");
        exit(1);
    }

    pool->num_threads = numthreads;
    pool->threads = malloc(sizeof(pthread_t) * numthreads);
    if (!pool->threads)
    {
        perror("malloc threads");
        free(pool);
        exit(1);
    }

    pool->queue = taskpq_init(4 * numthreads);
   
    pthread_cond_init(&pool->is_done, NULL);

    for (int i = 0; i < numthreads; i++)
    {
        pthread_create(&pool->threads[i], NULL, dispatch_task, NULL);
    }
}

void thread_pool_destroy()
{
    pthread_mutex_lock(&pool->queue->lock);
    pool->stop = 1;
    pthread_cond_broadcast(&pool->queue->full);
    pthread_mutex_unlock(&pool->queue->lock);

    for (int i = 0; i < pool->num_threads; i++)
    {
        pthread_join(pool->threads[i], NULL);
    }

    free(pool->threads);
    pool->threads = NULL;
    pool->num_threads = 0;

    if (pool->queue)
    {
        pthread_mutex_destroy(&pool->queue->lock);
        pthread_cond_destroy(&pool->queue->empty);
        pthread_cond_destroy(&pool->queue->full);
        free(pool->queue->tasks);
        free(pool->queue);
    }

    pthread_cond_destroy(&pool->is_done);
    free(pool);
    pool = NULL;
}

void thread_pool_submit(Task *task)
{
    taskpq_append(pool->queue, task);
}

void thread_pool_wait()
{
    pthread_mutex_lock(&pool->queue->lock);
    while (pool->queue->num_tasks != 0 || pool->curr_tasks != 0)
    {
        pthread_cond_wait(&pool->is_done, &pool->queue->lock);
    }
    pthread_mutex_unlock(&pool->queue->lock);
}


void free_rdd(RDD *rdd)
{
    if (rdd == NULL)
        return;

    pthread_mutex_lock(&rdd->lock);
    rdd->refcount--;
    if (rdd->refcount > 0)
    {
        pthread_mutex_unlock(&rdd->lock);
        return;
    }
    pthread_mutex_unlock(&rdd->lock);

   
    if (rdd->partitions)
    {
        for (int i = 0; i < rdd->partitions->size; i++)
        {
            void *part = rdd->partitions->data[i];
            if (rdd->trans == FILE_BACKED)
            {
                if (part)
                    fclose((FILE *)part);
            }
            else
            {
                if (part)
                    list_free((List *)part);
            }
        }
        list_free(rdd->partitions);
    }

    if (rdd->dependents)
    {
        list_free(rdd->dependents);
    }

    pthread_mutex_destroy(&(rdd->lock));
    free(rdd);
}

void execute(RDD *rdd)
{
    for (int i = 0; i < rdd->numdependencies; i++)
    {
        execute(rdd->dependencies[i]);
    }

    pthread_mutex_lock(&(rdd->lock));
    int ready = (rdd->dependencies_remaining == 0);
    pthread_mutex_unlock(&(rdd->lock));

    if (ready)
    {
        rdd_process_per_partition(rdd);
    }

    thread_pool_wait();
}


int count(RDD *rdd)
{
    execute(rdd);
    int total = 0;
    int num_parts = list_count(rdd->partitions);
    for (int i = 0; i < num_parts; i++)
    {
        List *part = (List *)list_get(rdd->partitions, i);
        total += list_count(part);
    }
    return total;
}

void print(RDD *rdd, Printer p)
{
    execute(rdd);
    int num_parts = list_count(rdd->partitions);
    for (int i = 0; i < num_parts; i++)
    {
        List *part = (List *)list_get(rdd->partitions, i);
        seek_to_start(part);
        void *item;
        while ((item = seek_next(part)) != NULL)
        {
            p(item);
        }
    }
    fflush(stdout);
}

void MS_Run()
{
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == -1)
    {
        perror("sched_getaffinity");
        exit(1);
    }
    int num_cores = CPU_COUNT(&set);
    threadpool_init(num_cores);
    clock_gettime(CLOCK_MONOTONIC, &metrics_start_time);

    metrics_queue = list_init(128);
    pthread_mutex_init(&metrics_lock, NULL);
    pthread_cond_init(&metrics_cond, NULL);
    metrics_stop = 0;
    pthread_create(&metrics_thread, NULL, print_formatted_metric, NULL);
}

void MS_TearDown()
{
    thread_pool_destroy();
    pthread_mutex_lock(&metrics_lock);
    metrics_stop = 1;
    pthread_cond_signal(&metrics_cond);
    pthread_mutex_unlock(&metrics_lock);

    pthread_join(metrics_thread, NULL);

    list_free(metrics_queue);
    pthread_mutex_destroy(&metrics_lock);
    pthread_cond_destroy(&metrics_cond);

    pthread_mutex_lock(&all_rdds_lock);
    if (rdd_list != NULL)
    {
        for (int i = 0; i < rdd_list->size; i++)
        {
            RDD *r = (RDD *)rdd_list->data[i];
            if (r != NULL && list_count(r->dependents) == 0)
            {
                free_rdd(r); 
                rdd_list->data[i] = NULL;
            }
        }
        list_free(rdd_list);
        rdd_list = NULL;
    }
    pthread_mutex_unlock(&all_rdds_lock);
}
