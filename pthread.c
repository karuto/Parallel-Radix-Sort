/* File:  
 *    pth_bar.c
 *
 * Purpose:
 *    Use Pthreads barrier to synchronize threads.
 *
 * Input:
 *    none
 * Output:
 *    Time for BARRIER_COUNT barriers
 *
 * Compile:
 *    gcc -g -Wall -o pth_bar pth_bar.c -lpthread
 * Usage:
 *    ./pth_bar <thread_count>
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "timer.h"
#include "barrier.h"

#define BARRIER_COUNT 1000

int thread_count;
pthread_barrier_t barrier;

void Usage(char* prog_name);
void *Thread_work(void* rank);

/*--------------------------------------------------------------------*/
int main(int argc, char* argv[]) {
   long       thread;
   pthread_t* thread_handles; 
   double start, finish;

   if (argc != 2) Usage(argv[0]);
   thread_count = strtol(argv[1], NULL, 10);

   thread_handles = malloc (thread_count*sizeof(pthread_t));
   pthread_barrier_init(&barrier, NULL, thread_count);

   GET_TIME(start);
   for (thread = 0; thread < thread_count; thread++)
      pthread_create(&thread_handles[thread], NULL,
          Thread_work, (void*) thread);

   for (thread = 0; thread < thread_count; thread++) 
      pthread_join(thread_handles[thread], NULL);
   GET_TIME(finish);
   printf("Elapsed time = %e seconds\n", finish - start);

   pthread_barrier_destroy(&barrier);
   free(thread_handles);
   return 0;
}  /* main */


/*--------------------------------------------------------------------
 * Function:    Usage
 * Purpose:     Print command line for function and terminate
 * In arg:      prog_name
 */
void Usage(char* prog_name) {

   fprintf(stderr, "usage: %s <number of threads>\n", prog_name);
   exit(0);
}  /* Usage */


/*-------------------------------------------------------------------
 * Function:    Thread_work
 * Purpose:     Run BARRIER_COUNT barriers
 * In arg:      rank
 * Global var:  barrier
 * Return val:  Ignored
 */
void *Thread_work(void* rank) {
// long my_rank = (long) rank; 
   int i;
   printf("####### Thread_work: THREAD %d\n", rank);

   for (i = 0; i < BARRIER_COUNT; i++) {
      pthread_barrier_wait(&barrier);
   }

   return NULL;
}  /* Thread_work */