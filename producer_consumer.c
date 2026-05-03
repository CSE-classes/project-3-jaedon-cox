#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define BUFFER_SIZE 5

/* --- Circular buffer (queue) --- */
char buffer[BUFFER_SIZE];
int  in  = 0;  /* next write position */
int  out = 0;  /* next read position  */
int  count = 0; /* number of items currently in buffer */

/* --- Synchronization --- */
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  not_full  = PTHREAD_COND_INITIALIZER; /* producer waits here */
pthread_cond_t  not_empty = PTHREAD_COND_INITIALIZER; /* consumer waits here */

/* --- Shared message state --- */
char *message = NULL;
int   msg_len  = 0;

void *producer(void *arg)
{
	int i;
	for (i = 0; i < msg_len; i++) {
		pthread_mutex_lock(&mutex);

		/* Wait while buffer is full */
		while (count == BUFFER_SIZE)
			pthread_cond_wait(&not_full, &mutex);

		/* Write character into circular buffer */
		buffer[in] = message[i];
		in = (in + 1) % BUFFER_SIZE;
		count++;

		/* Signal consumer that data is available */
		pthread_cond_signal(&not_empty);
		pthread_mutex_unlock(&mutex);
	}

	/* Write sentinel '\0' to tell consumer we are done */
	pthread_mutex_lock(&mutex);
	while (count == BUFFER_SIZE)
		pthread_cond_wait(&not_full, &mutex);
	buffer[in] = '\0';
	in = (in + 1) % BUFFER_SIZE;
	count++;
	pthread_cond_signal(&not_empty);
	pthread_mutex_unlock(&mutex);

	pthread_exit(NULL);
}

void *consumer(void *arg)
{
	char ch;
	while (1) {
		pthread_mutex_lock(&mutex);

		/* Wait while buffer is empty */
		while (count == 0)
			pthread_cond_wait(&not_empty, &mutex);

		/* Read character from circular buffer */
		ch = buffer[out];
		out = (out + 1) % BUFFER_SIZE;
		count--;

		/* Signal producer that space is available */
		pthread_cond_signal(&not_full);
		pthread_mutex_unlock(&mutex);

		if (ch == '\0')   /* sentinel: producer finished */
			break;

		printf("%c", ch);
		fflush(stdout);
	}
	printf("\n");
	pthread_exit(NULL);
}

int main(void)
{
	FILE *fp;
	char line[1024];

	/* Read message from file */
	if ((fp = fopen("message.txt", "r")) == NULL) {
		printf("ERROR: can't open message.txt!\n");
		exit(1);
	}
	if (fgets(line, sizeof(line), fp) == NULL) {
		printf("ERROR: message.txt is empty!\n");
		fclose(fp);
		exit(1);
	}
	fclose(fp);

	/* Strip trailing newline so it isn't printed */
	msg_len = strlen(line);
	if (msg_len > 0 && line[msg_len-1] == '\n') {
		line[msg_len-1] = '\0';
		msg_len--;
	}
	message = line;

	/* Create producer and consumer threads */
	pthread_t prod_tid, cons_tid;
	pthread_create(&prod_tid, NULL, producer, NULL);
	pthread_create(&cons_tid, NULL, consumer, NULL);

	pthread_join(prod_tid, NULL);
	pthread_join(cons_tid, NULL);

	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&not_full);
	pthread_cond_destroy(&not_empty);

	return 0;
}
