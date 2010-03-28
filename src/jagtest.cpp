#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "plfs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// A simple pthread based test.

// Parameters of the test.
#define NUM_WRITERS 10
#define WRITES_PER_WRITER 10
#define BYTES_PER_WRITE 16
#define BYTES_PER_THREAD (WRITES_PER_WRITER*BYTES_PER_WRITE)
#define FILENAME "foo"

// Global for ease of use.
Plfs_fd *fd;

// Opens the file and writes to the appriopriate location.
void *writing_thread(void *arg) {
  int i;
  char mybuff[BYTES_PER_WRITE];
  int pid = (int) arg;
 
  // The 0th writer writes all As, the 1st all Bs, the 2nd Cs, etc.
  memset(mybuff, 'A'+pid, BYTES_PER_WRITE);
  if (plfs_open(&fd, FILENAME, O_WRONLY, pid, 0777)) {
    printf("Error opening " FILENAME "!\n");
    exit(1);
  }
  
  for (i = 0; i < WRITES_PER_WRITER; i++) {
    int offset = BYTES_PER_THREAD*pid + BYTES_PER_WRITE*i;
    plfs_write(fd, mybuff, BYTES_PER_WRITE, offset, pid);
  }

  plfs_close(fd, pid, O_WRONLY);
  
  return NULL;
}

int main(int argc, char** argv) {
  pthread_t threads[NUM_WRITERS];
  int i;

  // Set the global descriptor to null so the first thread fills it in.
  fd = NULL;
  printf("Creating container!\n");
  if (plfs_open(&fd, FILENAME, O_WRONLY | O_CREAT, NUM_WRITERS, 0777)) {
    printf("Error opening " FILENAME "!\n");
    exit(1);
  }

  printf("Making writers!\n");

  for(i = 0; i < NUM_WRITERS; i++) {
    if (pthread_create(&threads[i], NULL, writing_thread, (void*)i)) {
      printf("Error creating thread %u\n", i);
      exit (1);
    }
  }
  
  printf("Writers made! Joining!\n");

  for (i = 0; i < NUM_WRITERS; i++) {
    pthread_join(threads[i], NULL);
    printf("Joined %u\n", i);
  }

  plfs_close(fd, NUM_WRITERS, O_WRONLY | O_CREAT);
  return 0;
}
