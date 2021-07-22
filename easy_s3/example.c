/**
 * Copyright (C) 2020 Bull S.A.S. - All rights reserved
 */



#include <stdio.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "easy_s3.h"


struct s3info myinfo = {
	"127.0.0.1:9000",
	"CECI_EST_MA_CLE_D_ACCES",
	"CECI_EST_MA_CLE_SECRETE"
};



static void sanity_check(void)
{
	if (getenv("http_proxy")) {
		fprintf(stderr, "Note: unsetting http_proxy\n");
		unsetenv("http_proxy");
	}
}


int main(void)
{
	sanity_check();

	S3Status s;
	s = S3_initialize(NULL, 0, NULL);
	assert(s == S3StatusOK);
	S3BucketContext *bucket = s3_open_bucket(&myinfo, "mybucket");
	if (!bucket) {
		fprintf(stderr, "S3 is not happy\n");
		exit(1);
	}

	char buf[1000];
	int64_t count = s3_get_range(bucket, "digits10", 25, 1000, buf);
	printf("Got %ld bytes\n", count);
	int i;
	for(i = 0; i < 10; i++) {
		printf("%c", buf[i]);
	}
	printf("\n");

	uint64_t sz;
	int64_t when;
	s3_head_object(bucket, "digits10", &sz, &when);
	printf("Size  = %lu\n", sz);


	/// STOP HERE
	//exit(0);

	char *upid;
	int rc;
	rc = s3_start_multi_upload(bucket, "newfile", &upid);
	if (rc == 0) {
		printf("Got upload ID %s\n", upid);
	} else {
		exit(1);
	}

	//char *a = "A la peche aux moules moules moules\n";
	//char *b = "Je ne veux plus y aller maman\n";
//#define L 10000000 // min is 5MB
#define L 10 // min is 5MB
	char *a = malloc(L);
	memset(a, 'A', L);
	//sprintf(a, "A la peche aux moules moules moules");
	char *b = malloc(L);
	memset(b, 'B', L);
	//sprintf(b, "Je ne veux plus y aller maman");

	char *etags[2];
	int seqids[] = { 1, 2 };
	rc = s3_upload_part(bucket, "newfile", upid, 1, &etags[0], L, a);
	assert(rc == L);
	//assert(rc == strlen(a));
	rc = s3_upload_part(bucket, "newfile", upid, 2, &etags[1], L, b);
	assert(rc == L);
	//assert(rc == strlen(b));

	rc = s3_complete_upload(bucket, "newfile", upid, 2, seqids, (const char**) etags);
	assert(rc == 0);

	char chk[2*L];
	count = s3_get_range(bucket, "newfile", 0, 2*L, chk);

	fwrite(chk, 1, 2 * L, stdout);
	printf("\n");

//	exit(0);


	char * upid2;
	char *other_etags[2];
	int seqids2[] = { 1000, 2000 };
	rc = s3_start_multi_upload(bucket, "newfile", &upid2);
	assert(rc == 0);
	rc = s3_copy_part(bucket, "newfile", upid2, 1000, &other_etags[0], "newfile", 0, L);
	assert(rc == 0);
	printf("Got copy etag %s\n", other_etags[0]);
	char *c = malloc(L);
	memset(c, 'C', L);
	rc = s3_upload_part(bucket, "newfile", upid2, 2000, &other_etags[1], L, c);
	assert(rc == L);
	rc = s3_complete_upload(bucket, "newfile", upid2, 2, seqids2, (const char**) other_etags);
	assert(rc == 0);

	count = s3_get_range(bucket, "newfile", 0, 2*L, chk);

	fwrite(chk, 1, 2 * L, stdout);
	printf("\n");


	free(other_etags[0]);
	free(other_etags[1]);
	free(upid);
	free(upid2);

	s3_free_bucket(bucket);

	return 0;
}
