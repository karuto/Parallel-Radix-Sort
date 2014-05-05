/****************************************************************************\
 * radix.c
 * Robert Blumofe
 * Copyright (c) Robert Blumofe, 1996.  All rights reserved.
\****************************************************************************/

#include <pthread.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

/* Bits of value to sort on. */
#define BITS 29

/****************************************************************************\
 * Array utilities.
\****************************************************************************/

/* Copy array. */
void copy_array (unsigned *dest, unsigned *src, int n)
{
  for ( ; n > 0; n-- )
    *dest++ = *src++;
}

/* Print array. */
void print_array (unsigned *val, int n)
{
  int i;
  for ( i = 0; i < n; i++ )
    printf ("%d ", val[i]);
  printf ("\n");
}

/* Fill array with random values. */
void random_array (unsigned *val, int n)
{
  int i;
  for ( i = 0; i < n; i++ ) {
  	val[i] = (unsigned)lrand48() & (unsigned)((1 << BITS) - 1);  	
  }
}

/* Check if array is sorted. */
int array_is_sorted (unsigned *val, int n)
{
  int i;
  for ( i = 1; i < n; i++ )
    if ( val[i-1] > val[i] )
      return 0;
  return 1;
}

/****************************************************************************\
 * Barrier.
\****************************************************************************/

/* Barrier. */
void barrier (void)
{
  /* CODE NEEDED HERE. */
  return;
}

/****************************************************************************\
 * Thread part of radix sort.
\****************************************************************************/

/* Individual thread part of radix sort. */
void radix_sort_thread (unsigned *val,           /* Array of values. */
			unsigned *tmp,           /* Temp array. */
			int start, int n,        /* Portion of array. */
			int *nzeros, int *nones, /* Counters. */
			int self,                /* My thread index. */
			int t)                   /* Number of theads. */
{
  /* THIS ROUTINE WILL REQUIRE SOME SYNCHRONIZATION. */
  /* MAYBE A CALL TO barrier() or TWO. */

  unsigned *src, *dest;
  int bit_pos;
  int index0, index1;
  int i;

  /* Initialize source and destination. */
  src = val;
  dest = tmp;

  /* For each bit... */
  for ( bit_pos = 0; bit_pos < BITS; bit_pos++ ) {

    /* Count elements with 0 in bit_pos. */
    nzeros[self] = 0;
    for ( i = start; i < start + n; i++ )
      if ( ((src[i] >> bit_pos) & 1) == 0 )
	nzeros[self]++;
    nones[self] = n - nzeros[self];

    /* Get starting indices. */
    index0 = 0;
    index1 = 0;
    for ( i = 0; i < self; i++ ) {
      index0 += nzeros[i];
      index1 += nones[i];
    }
    index1 += index0;
    for ( ; i < t; i++ )
      index1 += nzeros[i];

    /* Move values to correct position. */
    for ( i = start; i < start + n; i++ ) {
      if ( ((src[i] >> bit_pos) & 1) == 0 )
	dest[index0++] = src[i];
      else
	dest[index1++] = src[i];
    }

    /* Swap arrays. */
    tmp = src;
    src = dest;
    dest = tmp;
  }
}

/* Thread arguments for radix sort. */
struct rs_args {
  int id;         /* thread index. */
  unsigned *val;  /* array. */
  unsigned *tmp;  /* temporary array. */
  int n;          /* size of array. */
  int *nzeros;    /* array of zero counters. */
  int *nones;     /* array of one counters. */
  int t;          /* number of threads. */
};

/* Thread main routine. */
void thread_main (struct rs_args *args)
{
  int start;
  int n;

  /* Get portion of array to process. */
  n = args->n / args->t;
  start = args->id * n;

  /* Perform radix sort. */
  radix_sort_thread (args->val, args->tmp, start, n,
		     args->nzeros, args->nones, args->id, args->t);
}

/****************************************************************************\
 * Main part of radix sort.
\****************************************************************************/

/* Radix sort array. */
void radix_sort (unsigned *val, int n, int t)
{
  unsigned *tmp;
  int *nzeros, *nones;
  struct rs_args *args;
  int r, i;

  /* Allocate temporary array. */
  tmp = (unsigned *) malloc (n * sizeof(unsigned));
  if (!tmp) { fprintf (stderr, "Malloc failed.\n"); exit(1); }

  /* Allocate counter arrays. */
  nzeros = (int *) malloc (t * sizeof(int));
  if (!nzeros) { fprintf (stderr, "Malloc failed.\n"); exit(1); }
  nones = (int *) malloc (t * sizeof(int));
  if (!nones) { fprintf (stderr, "Malloc failed.\n"); exit(1); }

  /* Allocate thread arguments. */
  args = (struct rs_args *) malloc (t * sizeof(struct rs_args));
  if (!args) { fprintf (stderr, "Malloc failed.\n"); exit(1); }

  /* Initialize arguments. */
  for ( i = 0; i < t; i++ ) {
    args[i].id = i;
    args[i].val = val;
    args[i].tmp = tmp;
    args[i].n = n;
    args[i].nzeros = nzeros;
    args[i].nones = nones;
    args[i].t = t;
  }

  /* Create threads. */
  /* CODE NEEDED HERE. */
  thread_main (&args[0]);

  /* Wait for threads to terminate. */
  /* CODE NEEDED HERE. */

  /* Free thread arguments. */
  free (args);

  /* Copy array if necessary. */
  if ( BITS % 2 == 1 )
    copy_array (val, tmp, n);

  /* Free temporary array and couter arrays. */
  free (nzeros);
  free (nones);
  free (tmp);
}


void main (int argc, char *argv[]) {
  int n, t;
  unsigned *val;
  time_t start, end;
  int ok;

  /* User input: number of elements [n]. An array of size [n] will be then 
  	generated based on this user input. This input is not mandatory. */
  n = 1e6; /* By default one million, or 1 followed by 6 zeros */
  if ( argc > 1 )
    n = atoi (argv[1]);
  if ( n < 1 ) { fprintf (stderr, "Invalid number of elements.\n"); exit(1); }

  /* User input: number of threads [t]. Must be evenly divisible by the
  	number of elements [n]. This input is not mandatory. */
  t = 1;
  if ( argc > 2 )
    t = atoi (argv[2]);
  if ( t < 1 ) { fprintf (stderr, "Invalid number of threads.\n"); exit(1); }
  if ( (n / t) * t != n ) {
    fprintf (stderr, "Number of threads must divide number of elements.\n");
    exit(1);
  }

  /* Allocate array. */
  val = (unsigned *) malloc (n * sizeof(unsigned));
  if (!val) { fprintf (stderr, "Malloc failed.\n"); exit(1); }

  /* Initialize array. */
  printf ("Initializing array... "); fflush (stdout);
  random_array (val, n);
  printf ("Done.\n");

  /* Sort array. */
  printf ("Sorting array... "); fflush (stdout);
  start = time (0);
  radix_sort (val, n, t);
  end = time (0);
  printf ("Done.\n");
  printf ("Elapsed time = %.0f seconds.\n", difftime(end, start));

  /* Check result. */
  if ( n <= 30 ) print_array (val, n);
  printf ("Testing array... "); fflush (stdout);
  ok = array_is_sorted (val, n);
  printf ("Done.\n");
  if ( ok )
    printf ("Array is correctly sorted.\n");
  else
    printf ("Oops! Array is not correctly sorted.\n");

  /* Free array. */
  free (val);
}