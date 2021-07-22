/**
 * Copyright (C) 2020 Bull S.A.S. - All rights reserved
 */



#ifndef LIB_EASY_S3_H
#define LIB_EASY_S3_H

#include <libs3.h>

int s3_init(void);
void s3_set_tracer(void (*__tracefunc)(const char *function, const char *file, int line, const char *format, ...));

struct s3info {
	char *url;
	char *accesskey;
	char *secretkey;
};

S3BucketContext *s3_open_bucket(struct s3info *s3i, const char *bucketname);

void s3_free_bucket(S3BucketContext *bucket);

int s3_head_object(S3BucketContext *bucket, const char *objectname,
		uint64_t *size, time_t *lastmodified);

int64_t s3_get_range(S3BucketContext *bucket, const char *objectname,
		uint64_t startByte, uint64_t byteCount,
		void *buf);

int s3_start_multi_upload(S3BucketContext *bucket, const char *objectname, char **uploadID);

int64_t s3_upload_part(S3BucketContext *bucket, const char *objectname,
		const char *uploadID, int seq,
		char **etag,
		uint64_t byteCount,
		void *buf);

int64_t s3_complete_upload(S3BucketContext *bucket, const char *objectname,
		const char *uploadID, 
		int nr_parts,
		int *seq_ids,
		const char **etags);

int s3_copy_part(S3BucketContext *bucket, const char *dest_objectname,
		const char *uploadID, int seq,
		char **etag,
		const char *src_objectname,
		uint64_t startOffset,
		uint64_t byteCount);

#endif
