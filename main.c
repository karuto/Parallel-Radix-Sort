/* File:       main.c
 * Author:     Vincent Zhang
 *
 * Purpose:    A C program using CUDA to implement a parallel solution to the
 *             Longest Common Subsequence Problem.
 *
 * Compile:    gcc -g -Wall main.c -o main
 * Run:        main [number of thread blocks] [number of threads in each block]  
 *             [Optional suppress output(n)]
 *
 * Input:      A file containing a list of integers separated by white space.
 *
 * Output:     1. The content of the sorted list.
 *             2. The time used by the solver (not including I/O).
 *
 * Algorithm:  The list of integers is read into a global array, each thread
 *             will then proceed to partition most of the steps during sample 
 *             sort: locating sample keys, generating splitters, sorting local 
 *             data blocks, computing the distribution arrays, assigning items 
 *             to buckets and eventually sorting. For further in-depth details,
 *             please refer to http://www.cs.usfca.edu/~peter/cs625/prog3.pdf
 *             and http://en.wikipedia.org/wiki/Samplesort for a much better 
 *             explanation.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include "timer.h"
#include "barrier.h"


// Synchronization tools
#define BARRIER_COUNT 1000
pthread_barrier_t barrier;

// Function headers
void Usage(char* prog_name);
void Print_list(int *l, int size, char *name);
int Is_used(int seed, int offset, int range);
int Int_comp(const void * a,const void * b);
void *Thread_work(void* rank);

// Global variables
int i, thread_count, sample_size, list_size, suppress_output;
int *list, *sample_keys, *sorted_keys, *splitters, *tmp_list, *sorted_list;
int *raw_dist, *prefix_dist, *col_dist, *prefix_col_dist;
char *input_file;


/*--------------------------------------------------------------------
 * Function:    Usage
 * Purpose:     Print command line for function and terminate
 * In arg:      prog_name
 */
void Usage(char* prog_name) {

  fprintf(stderr, "Usage: %s [number of threads] [sample size] [list size] [name of input file] [Optional suppress output(n)]\n", prog_name);
  exit(0);
}  /* Usage */



/*--------------------------------------------------------------------
 * Function:    Print_list
 * Purpose:     Print list in formatted fashion
 * In arg:      l, size, name
 */
void Print_list(int *l, int size, char *name) {
    printf("\n======= %s =======\n", name);
    for (i = 0; i < size; i++) {
    	  printf("%d ", l[i]);
    }
    printf("\n");
}  /* Print_list */



/*--------------------------------------------------------------------
 * Function:    Is_used
 * Purpose:     Check if the random seeded key is already selected in sample
 * In arg:      seed, offset, range
 */
int Is_used(int seed, int offset, int range) {
  int i;
	for (i = offset; i < (offset + range); i++) {
		if (sample_keys[i] == list[seed]) {
			return 1;
		} else {
			return 0;
		}
	}
	return 0;
} /* Is_used */



/*--------------------------------------------------------------------
 * Function:    Int_comp
 * Purpose:     Comparison function for integer, used by qsort
 * In arg:      a, b
 */
int Int_comp(const void * a,const void * b) {
    int va = *(const int*) a;
    int vb = *(const int*) b;
    return (va > vb) - (va < vb);
}



/*-------------------------------------------------------------------
 * Function:    Thread_work
 * Purpose:     Run BARRIER_COUNT barriers
 * In arg:      rank
 * Global var:  barrier
 * Return val:  Ignored
 */
void *Thread_work(void* rank) {
  long my_rank = (long) rank;
  int i, j, seed, index, offset, local_chunk_size, local_sample_size;
  int local_pointer, s_index, my_segment, col_sum;
  int *local_data;

  local_chunk_size = list_size / thread_count;
  local_sample_size = sample_size / thread_count;
  
  // printf("Hi this is thread %ld, I have %d chunks and should do %d samples. \n", my_rank, local_chunk_size, local_sample_size);
  
  // Get sample keys randomly from original list
  srandom(my_rank + 1);  
  offset = my_rank * local_sample_size;
  
  for (i = offset; i < (offset + local_sample_size); i++) {
	  do {
		  // If while returns 1, you'll be repeating this
		  seed = (my_rank * local_chunk_size) + (random() % local_chunk_size);
	  } while (Is_used(seed, offset, local_sample_size));
	  // If the loop breaks (while returns 0), data is clean, assignment
	  sample_keys[i] = list[seed];
	  index = offset + i;
	  
	  // printf("T%ld, seed = %d\n", my_rank, seed);
	  // printf("T%ld, index = %d, i = %d, key = %d, LCS = %d\n\n", my_rank, index, i, list[seed], local_sample_size);
  }
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);
  
  // Parallel count sort the sample keys
  for (i = offset; i < (offset + local_sample_size); i++) {
	  int mykey = sample_keys[i];
	  int myindex = 0;
	  for (j = 0; j < sample_size; j++) {
		  if (sample_keys[j] < mykey) {
			  myindex++;
		  } else if (sample_keys[j] == mykey && j < i) {
			  myindex++;
		  } else {
		  }
	  }
	  // printf("##### P%ld Got in FINAL, index = %d, mykey = %d, myindex = %d\n", my_rank, i, mykey, myindex);
	  sorted_keys[myindex] = mykey;
  }
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);
  
  // Besides thread 0, every thread generates a splitter
  // splitters[0] should always be zero
  if (my_rank != 0) {
	  splitters[my_rank] = (sorted_keys[offset] + sorted_keys[offset-1]) / 2;
  }
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);

  // Using block partition to retrieve and sort local chunk
  local_pointer = my_rank * local_chunk_size;
  local_data = malloc(local_chunk_size * sizeof(int));

  j = 0;
  for (i = local_pointer; i < (local_pointer + local_chunk_size); i++) {  
	  local_data[j] = list[i];
	  j++;
  }
  
  // Quick sort on local data before splitting into buckets
  qsort(local_data, local_chunk_size, sizeof(int), Int_comp);
  
  // index in the splitter array
  s_index = 1;	
  // starting point of this thread's segment in dist arrays
  my_segment = my_rank * thread_count; 
  
  // Generate the original distribution array, loop through each local entry
  for (i = 0; i < local_chunk_size; i++) {
	  if (local_data[i] < splitters[s_index]) {
		  // If current elem lesser than current splitter
		  // That means it's within this bucket's range, keep looping
	  } else {
		  // Elem is out of bucket's range, time to increase splitter
		  // Keep increasing until you find one that fits
		  // Also make sure if equals we still increment
		  while (s_index < thread_count && local_data[i] >= splitters[s_index]) {
			  s_index++;
		  }
	  }
	  // Add to the raw distribution array, -1 because splitter[0] = 0
	  raw_dist[my_segment + s_index-1]++;
  }
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);
  
  // Generate prefix sum distribution array 
  // (NOTE: does not need to wait for the whole raw_dist to finish, thus no barrier)
  // For the specific section that this thread is in charge of...
  // +1 initially because we don't process the first element at all
  for (i = my_segment; i < (my_segment + thread_count); i++) {
	  if (i == my_segment) {
		  prefix_dist[i] = raw_dist[i];	 
		  // printf("Thread %ld ### i = %d, prefix_dist[i] = %d, raw_dist[i] = %d\n", my_rank, i, prefix_dist[i], raw_dist[i]); 	
	  } else {
		  prefix_dist[i] = raw_dist[i] + prefix_dist[i - 1];
		  // printf("Thread %ld ### i = %d, prefix_dist[i] = %d, raw_dist[i] = %d , raw_dist[i-1] = %d\n", my_rank, i, prefix_dist[i], raw_dist[i], raw_dist[i - 1]);
	  }
  }
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);
  
  // Generate column distribution array 
  // For the specific section that this thread is in charge of...
  // +1 initially because we don't process the first element at all
  for (i = my_segment; i < (my_segment + thread_count); i++) {
	  if (i == my_segment) {
		  prefix_dist[i] = raw_dist[i];	 
		  // printf("Thread %ld ### i = %d, prefix_dist[i] = %d, raw_dist[i] = %d\n", my_rank, i, prefix_dist[i], raw_dist[i]); 	
	  } else {
		  prefix_dist[i] = raw_dist[i] + prefix_dist[i - 1];
		  // printf("Thread %ld ### i = %d, prefix_dist[i] = %d, raw_dist[i] = %d , raw_dist[i-1] = %d\n", my_rank, i, prefix_dist[i], raw_dist[i], raw_dist[i - 1]);
	  }
  }
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);
  
  // Generate column sum distribution, each thread responsible for one column
  col_sum = 0;
  for (i = 0; i < thread_count; i++) {
	  col_sum += raw_dist[my_rank + i * thread_count];
  }
  col_dist[my_rank] = col_sum;
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);
  
  // Generate prefix column sum distribution, each thread responsible for one column
  // This step is very risky to conduct parallelly, I decided to not do that
  if (my_rank == 0) {
	  for (i = 0; i < thread_count; i++) {
		  if (i == 0) {
		  	prefix_col_dist[i] = col_dist[i];
		  } else {
		  	prefix_col_dist[i] = col_dist[i] + prefix_col_dist[i - 1];
		  }
	  }
  }
  
  // Reassemble the partially sorted list, prepare for retrieval
  for (i = 0; i < local_chunk_size; i++) {
	  tmp_list[local_pointer + i] = local_data[i];
  }
  
  // Ensure all threads have reached this point, and then let continue
  pthread_barrier_wait(&barrier);
  
  // Reassemble each thread's partially sorted list based on buckets
  // Allocate an array based on the column sum of this specific bucket
  int my_first_D = col_dist[my_rank];
  int *my_D = malloc(my_first_D * sizeof(int));
  printf("~~~ Thread %ld got here, my_first_D = %d\n", my_rank, my_first_D);
  
  int b_index = 0;
  // int i_manual = 0;
  // For each thread in the column...
  for (i = 0; i < thread_count; i++) {
	  // offset = i * local_chunk_size + prefix_dist[i, my_rank-1];
	  // offset = (i_manual * local_chunk_size) + prefix_dist[i*thread_count + my_rank-1];
	  
	  if (my_rank == 0) {
		  offset = (i * local_chunk_size);
		  printf("@@@ Thread %ld, prefix_dist = %d, i = %d, offset = %d\n", my_rank, prefix_dist[i*thread_count + my_rank-1], i, offset);	  	
	  } else {
	  	  offset = (i * local_chunk_size) + prefix_dist[i*thread_count + my_rank-1];
	  }
	  
	  if (raw_dist[i*thread_count + my_rank] != 0) {
		  // If this row doesn't have anything belong to this bucket
		  // Do not increase i_manual
		  // i_manual++;
		  for (j = 0; j < raw_dist[i*thread_count + my_rank]; j++) {
			  if (my_rank == 0) {
				  printf("### Thread %ld, raw_index = %d, b_index = %d, offset = %d, j = %d, offset+j = %d, elem = %d\n", my_rank, raw_dist[i*thread_count + my_rank], b_index, offset, j, offset + j, tmp_list[offset + j]);
			  }
			  my_D[b_index] = tmp_list[offset + j];
			  b_index++;
		  }
		  
	  } 
  }
  // Quick sort on local bucket
  qsort(my_D, my_first_D, sizeof(int), Int_comp);
  // Print_list(my_D, my_first_D, "Thread list");
  
  
  // Ensure all threads have reached this point, and then let continue
  // pthread_barrier_wait(&barrier);
  
  // Merge thread bucket data into final sorted list
  if (my_rank == 0) {
	  for (i = 0; i < my_first_D; i++) {
	  // printf("~~~ Thread %ld, sorted_list[%d] = %d\n", my_rank, i, my_D[i]);
		  sorted_list[i] = my_D[i];
	  }
  } else {
	  offset = prefix_col_dist[my_rank-1];
	  for (i = 0; i < my_first_D; i++) {
		  // printf("~~~ Thread %ld, offset = %d, sorted_list[%d] = %d\n", my_rank, offset, offset+i, my_D[i]);
		  sorted_list[offset + i] = my_D[i];
	  }
  }
  
  return NULL;
}  /* Thread_work */



/*--------------------------------------------------------------------*/
int main(int argc, char* argv[]) {
  long thread;
  pthread_t* thread_handles; 
  double start, finish;

  suppress_output = 0;
  // for (int i = 0; i < argc; ++i){
  //   printf("Command line args === argv[%d]: %s\n", i, argv[i]);
  // }  

  if (argc == 5) { 
  } else if (argc == 6 && (strcmp(argv[5], "n") == 0)) {
	  // printf("==== %s\n", argv[5]);
	  suppress_output = 1;
  } else {
	Usage(argv[0]);
  }
  
  thread_count = strtol(argv[1], NULL, 10);
  sample_size = strtol(argv[2], NULL, 10);
  list_size = strtol(argv[3], NULL, 10);
  input_file = argv[4];

  // Allocate memory for variables
  thread_handles = malloc(thread_count*sizeof(pthread_t));
  list = malloc(list_size * sizeof(int));
  tmp_list = malloc(list_size * sizeof(int));
  sorted_list = malloc(list_size * sizeof(int));
  sample_keys = malloc(sample_size * sizeof(int));
  sorted_keys = malloc(sample_size * sizeof(int));
  splitters = malloc(thread_count * sizeof(int));
  
  // One dimensional distribution arrays
  raw_dist = malloc(thread_count * thread_count * sizeof(int));
  col_dist = malloc(thread_count * sizeof(int));
  prefix_dist = malloc(thread_count * thread_count * sizeof(int));
  prefix_col_dist = malloc(thread_count * sizeof(int));
  
	
  // pthread_mutex_init(&barrier_mutex, NULL);
  // pthread_cond_init(&ok_to_proceed, NULL);
  pthread_barrier_init(&barrier, NULL, thread_count);
  

  // Read list content from input file
  FILE *fp = fopen(input_file, "r+");
  for (i = 0; i < list_size; i++) {
  	  if (!fscanf(fp, "%d", &list[i])) {
    	  break;
      }
  }
  Print_list(list, list_size, "original list");
  
  GET_TIME(start);
  
  for (thread = 0; thread < thread_count; thread++)
     pthread_create(&thread_handles[thread], NULL,
         Thread_work, (void*) thread);

  for (thread = 0; thread < thread_count; thread++) 
     pthread_join(thread_handles[thread], NULL);
  
  GET_TIME(finish);
  
  // Print_list(sample_keys, sample_size, "Sample keys (unsorted)");
  Print_list(sorted_keys, sample_size, "Sample keys (sorted)");
  Print_list(splitters, thread_count, "Splitters");
  Print_list(raw_dist, thread_count * thread_count, "Raw dist");
  Print_list(prefix_dist, thread_count * thread_count, "Prefix dist");
  Print_list(col_dist, thread_count, "Colsum dist");
  Print_list(prefix_col_dist, thread_count, "Prefix colsum dist");
  Print_list(tmp_list, list_size, "Temp list");
  
  // Only print list data if not suppressed
  if (suppress_output == 0) {
	  Print_list(sorted_list, list_size, "Sorted list");
  }
  
  // Print elapsed time regardless
  printf("Elapsed time = %e seconds\n", finish - start);


  pthread_barrier_destroy(&barrier);
  // pthread_mutex_destroy(&barrier_mutex);
  // pthread_cond_destroy(&ok_to_proceed);

  free(thread_handles);
  
  return 0;
}  /* main */


