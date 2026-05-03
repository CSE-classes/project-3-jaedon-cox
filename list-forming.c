/*
	list-forming.c:
	Each thread generates a data node, attaches it to a global list. This is repeated for K times.
	There are num_threads. The value of "num_threads" is input by the student.
*/
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/param.h>
#include <sched.h>

#define K 200

struct Node
{
    int data;
    struct Node* next;
};

struct list
{
    struct Node *header;
    struct Node *tail;
};

pthread_mutex_t mutex_lock;
struct list *List;

void bind_thread_to_cpu(int cpuid) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(cpuid, &mask);
    if (sched_setaffinity(0, sizeof(cpu_set_t), &mask)) {
        fprintf(stderr, "sched_setaffinity");
        exit(EXIT_FAILURE);
    }
}

struct Node* generate_data_node()
{
    struct Node *ptr;
    ptr = (struct Node *)malloc(sizeof(struct Node));
    if (NULL != ptr) {
        ptr->next = NULL;
    } else {
        printf("Node allocation failed!\n");
    }
    return ptr;
}

void *producer_thread(void *arg)
{
    bind_thread_to_cpu(*((int*)arg));

    /* --- Build a local list of K nodes (no locking needed) --- */
    struct list local = { NULL, NULL };
    int counter = 0;

    while (counter < K) {
        struct Node *ptr = generate_data_node();
        if (NULL != ptr) {
            ptr->data = 1;
            /* Append to local list */
            if (local.header == NULL) {
                local.header = local.tail = ptr;
            } else {
                local.tail->next = ptr;
                local.tail = ptr;
            }
            ++counter;
        }
    }

    /* --- Splice entire local list into global list with ONE lock --- */
    if (local.header != NULL) {
        pthread_mutex_lock(&mutex_lock);          /* blocking, not spinning */
        if (List->header == NULL) {
            List->header = local.header;
            List->tail   = local.tail;
        } else {
            List->tail->next = local.header;
            List->tail       = local.tail;
        }
        pthread_mutex_unlock(&mutex_lock);
    }

    return NULL;
}

int main(int argc, char* argv[])
{
    int i, num_threads;
    int NUM_PROCS;
    int *cpu_array = NULL;
    struct Node *tmp, *next;
    struct timeval starttime, endtime;

    if (argc < 2) {
        printf("Usage: %s <num_threads>\n", argv[0]);
        exit(1);
    }

    num_threads = atoi(argv[1]);
    pthread_t producer[num_threads];

    NUM_PROCS = sysconf(_SC_NPROCESSORS_CONF);
    if (NUM_PROCS > 0) {
        cpu_array = (int *)malloc(NUM_PROCS * sizeof(int));
        if (cpu_array == NULL) { printf("Allocation failed!\n"); exit(0); }
        for (i = 0; i < NUM_PROCS; i++) cpu_array[i] = i;
    }

    pthread_mutex_init(&mutex_lock, NULL);

    List = (struct list *)malloc(sizeof(struct list));
    if (NULL == List) { printf("End here\n"); exit(0); }
    List->header = List->tail = NULL;

    gettimeofday(&starttime, NULL);
    for (i = 0; i < num_threads; i++)
        pthread_create(&producer[i], NULL, (void *)producer_thread, &cpu_array[i % NUM_PROCS]);

    for (i = 0; i < num_threads; i++)
        if (producer[i] != 0)
            pthread_join(producer[i], NULL);

    gettimeofday(&endtime, NULL);

    /* Free the global list */
    next = tmp = List->header;
    while (tmp != NULL) {
        next = tmp->next;
        free(tmp);
        tmp = next;
    }
    free(List);
    if (cpu_array != NULL) free(cpu_array);
    pthread_mutex_destroy(&mutex_lock);

    printf("Total run time is %ld microseconds.\n",
           (endtime.tv_sec - starttime.tv_sec) * 1000000 +
           (endtime.tv_usec - starttime.tv_usec));
    return 0;
}
