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

#define MAX_ETAG_LEN 1000 // cannot find that info anywhere ?

static void (*s3_tracefunc)(const char *function, const char *file, int line, const char *format, ...) __attribute__ ((format (printf, 4, 5)));

#define trace(format...) do { if (s3_tracefunc) s3_tracefunc(__FUNCTION__, __FILE__, __LINE__, "S3: " format); } while(0)

void s3_set_tracer(void (*__tracefunc)(const char *function, const char *file, int line, const char *format, ...))
{
	s3_tracefunc = __tracefunc;
}

/**
 * Generic properties callback which does nothing
 */
static S3Status prop_cb_nop(const S3ResponseProperties *properties, void *callbackData)
{
	return S3StatusOK;
}

/**
 * We use this simple callback to get the status for almost all requests.
 * The pointer passed as the last argument will point to a request-specific context
 * struct, so all context structs must begin with the status
 */
static void comp_cb_status(S3Status status,
			  const S3ErrorDetails *errorDetails,
			  void *cb_status)
{
	*(S3Status*) cb_status = status;
}

static const struct S3ResponseHandler simple_response_handler = {
	prop_cb_nop,
	comp_cb_status,
};

/********************* OPEN BUCKET ****************************/


/**
 * Open (Actually, check accessibility of) a S3 bucket
 *
 * @param[in] s3i           Connection and Auth information
 * @param[in] bucketname    Name of the (existing) bucket to "open"
 *
 * @return S3BucketContext* on success, NULL on failure
 *
 * Resources associated with this bucket can be released by calling s3_free_bucket()
 *
 */
S3BucketContext *s3_open_bucket(struct s3info *s3i, const char *bucketname)
{
	assert(s3i); assert(bucketname);
	char *dupname = strdup(bucketname);
	char *dupakey = strdup(s3i->accesskey);
	char *dupskey = strdup(s3i->secretkey);
	char *dupurl = strdup(s3i->url);
	S3BucketContext *bucket = calloc(1, sizeof(*bucket));
	if (!bucket || !dupname || !dupurl || !dupskey || !dupakey) {
		trace("bucket memory allocation failed");
		free(bucket);
		free(dupname);
		free(dupurl);
		free(dupskey);
		free(dupakey);
		return NULL;
	}
	*bucket = (S3BucketContext) {
		.hostName = dupurl,
		.bucketName = dupname,
		.protocol = S3ProtocolHTTP,
		.uriStyle = S3UriStylePath,
		.accessKeyId = dupakey,
		.secretAccessKey = dupskey,
	};
	S3Status s = S3StatusInternalError; // if ever callback is not called
	S3_test_bucket(bucket->protocol,
	               bucket->uriStyle,
	               bucket->accessKeyId,
	               bucket->secretAccessKey,
			NULL /* security token */,
			bucket->hostName,
			bucket->bucketName,
			NULL /* region */,
			0, // constraint len
			NULL, // constraint
			NULL, // context,
			0, // timeout
			&simple_response_handler,
			(void*) &s);

	trace("test bucket %s returned %s",
	      bucketname,
	      S3_get_status_name(s));

	if (s == S3StatusOK) {
		return bucket;
	} else {
		trace("Failed to access bucket %s: %s",
				bucketname,
				S3_get_status_name(s));
		s3_free_bucket(bucket);
		return NULL;
	}
}

/**
 * Free the resources of a bucket obtained from s3_open_bucket()
 *
 * @param[in] bucket   Bucket to free
 */
void s3_free_bucket(S3BucketContext *bucket)
{
	if (bucket) {
		free((char *) bucket->hostName);
		free((char *) bucket->bucketName);
		free((char *) bucket->accessKeyId);
		free((char *) bucket->secretAccessKey);
		free(bucket);
	}
}

/********************* HEAD ***************************/
struct head_cb_ctx {
	S3Status cb_status; // MUST be at the head of the struct
	uint64_t head_objsize;
	time_t   head_lastmodified;
};

static S3Status prop_cb_head(const S3ResponseProperties *properties, void *callbackData)
{
	struct head_cb_ctx *ctx = callbackData;
	ctx->head_objsize = properties->contentLength;
	ctx->head_lastmodified = properties->lastModified;
	return S3StatusOK;
}

/**
 * Do a HEAD operation on an object
 *
 * @param[in] bucket         Bucket
 * @param[in] object_name     Object name
 * @param[out] size          Size (in bytes) of the object
 * @param[out] lastmodified  Date of last modification
 *
 * @return 0 on success, -1 on error
 */
int s3_head_object(S3BucketContext *bucket, const char *object_name,
		uint64_t *size, time_t *lastmodified)
{
	assert(object_name); assert(size); assert(lastmodified);
	struct S3ResponseHandler my_response_handler = {
		prop_cb_head,
		comp_cb_status,
	};
	struct head_cb_ctx ctx = {
		.cb_status = S3StatusInternalError // if ever callback is not called
	};

	S3_head_object(bucket, object_name,
			NULL /* requestContext */,
			0 /* timeoutMs */,
			&my_response_handler,
			(void*) &ctx);

	trace("HEAD %s returned %s size %lu time %ld",
	      object_name,
	      S3_get_status_name(ctx.cb_status),
	      ctx.head_objsize,
	      ctx.head_lastmodified);

	if (ctx.cb_status == S3StatusOK) {
		*size = ctx.head_objsize;
		*lastmodified = ctx.head_lastmodified;
		return 0;
	} else {
		trace("Failed to head %s: %s",
				object_name,
				S3_get_status_name(ctx.cb_status));
		return -1;
	}
}



/********************* GET ****************************/

struct get_cb_ctx {
	S3Status cb_status; // MUST be at the head of the struct
	void *get_cb_buf;
	uint64_t get_cb_bytes;
	uint64_t get_cb_maxbytes;
};

/** getObjectDataCallback for get_range */
static S3Status get_object_data_cb_get(int bufferSize, const char *buffer, void *cbdata)
{
	struct get_cb_ctx *ctx = cbdata;
	if (ctx->get_cb_bytes + bufferSize > ctx->get_cb_maxbytes) {
		trace("S3 GET returned too much data");
		trace("Already got %lu/%lu, now +%d",
				ctx->get_cb_bytes, ctx->get_cb_maxbytes, bufferSize);
		return S3StatusAbortedByCallback;
	}
	memcpy(ctx->get_cb_buf + ctx->get_cb_bytes, buffer, bufferSize);
	ctx->get_cb_bytes += bufferSize;
	return S3StatusOK;
}

/**
 * Read data from a S3 object
 *
 * @param[in]    bucket       Bucket where the object is. Use s3_open_bucket() to get it.
 * @param[in]    object_name   Name (i.e key) of the object
 * @param[in]    startByte    Offset of the first byte to read
 * @param[in]    byteCount    Number of bytes to read
 * @param[out]   buf          Buffer to fill
 *
 * @return Number of bytes read, or <0 on error
 */
int64_t s3_get_range(S3BucketContext *bucket, const char *object_name,
		uint64_t startByte, uint64_t byteCount,
		void *buf)
{
	assert(object_name); assert(buf);
	const struct S3GetObjectHandler myhandler = {
		simple_response_handler,
		get_object_data_cb_get
	};
	struct get_cb_ctx ctx = {
		.cb_status = S3StatusInternalError, // if ever callback is not called
		.get_cb_buf = buf,
		.get_cb_bytes = 0,
		.get_cb_maxbytes = byteCount
	};
	S3_get_object(bucket, object_name, NULL, startByte, byteCount, NULL, 0,
		&myhandler, &ctx);
	if (ctx.cb_status == S3StatusErrorInvalidRange) // EOF ?
		return 0;
	if (ctx.cb_status != S3StatusOK) {
		trace("Get Failed: %s", S3_get_status_name(ctx.cb_status));
		return -1;
	} else {
		return ctx.get_cb_bytes;
	}
}


/*************************** UPLOAD (multi-part) ******************/
// start
struct start_multi_ctx {
	S3Status cb_status; // MUST be at the head of the struct
	char *multi_upload_id;
};


/** responseXmlCallback for s3_start_multi_upload */
static S3Status resp_xml_cb_start_multi(const char *upload_id, void *callbackData)
{
	struct start_multi_ctx *ctx = callbackData;
	ctx->multi_upload_id = strdup(upload_id); // in doubt, strdup
	if (!ctx->multi_upload_id)
		return S3StatusOutOfMemory;
	return S3StatusOK;
}

/**
 * Start a multi upload.
 *
 * @param[in]    bucket       Bucket where the object is. Use s3_open_bucket() to get it.
 * @param[in]    object_name   Name (i.e key) of the object
 * @param[out]   upload_id     upload ID (malloc'ed)
 *
 * @return 0 on success, or <0 on error
 */
int s3_start_multi_upload(S3BucketContext *bucket, const char *object_name, char **upload_id)
{
	struct S3MultipartInitialHandler myhandler = {
		simple_response_handler,
		resp_xml_cb_start_multi
	};
	struct start_multi_ctx ctx = {
		.cb_status = S3StatusInternalError // if ever callback is not called
	};
	S3_initiate_multipart(bucket, object_name, NULL, &myhandler, NULL, 0, &ctx);
	if (ctx.cb_status != S3StatusOK) {
		trace("Init Multi Failed: %s",
				S3_get_status_name(ctx.cb_status));
		return -1;
	} else {
		*upload_id = ctx.multi_upload_id;
		return 0;
	}
}

// upload a part
struct upload_cb_ctx {
	S3Status cb_status; // MUST be at the head of the struct
	char *upload_etag;
	void *upload_cb_buf;
	uint64_t upload_cb_bytes; // amount already uploaded
	uint64_t upload_cb_maxbytes; // total amount to upload
};

/** putObjectDataCallback for s3_upload_part */
static int put_object_data_cb_upload_part(int bufferSize, char *buffer, void *cbdata)
{
	struct upload_cb_ctx *ctx = cbdata;
	int avail = ctx->upload_cb_maxbytes - ctx->upload_cb_bytes;
	int count = avail < bufferSize ? avail : bufferSize;
	memcpy(buffer, ctx->upload_cb_buf + ctx->upload_cb_bytes, count);
	ctx->upload_cb_bytes += count;
	return count;
}

/** propertiesCallback for s3_upload_part */
static S3Status prop_cb_upload_part(const S3ResponseProperties *properties, void *callbackData)
{
	struct upload_cb_ctx *ctx = callbackData;
	ctx->upload_etag = strdup(properties->eTag);
	if (!ctx->upload_etag)
		return S3StatusOutOfMemory;
	trace("Got part etag %s", ctx->upload_etag);
	return S3StatusOK;
}

/**
 * @param[in]    bucket         Bucket where the object is. Use s3_open_bucket() to get it.
 * @param[in]    upload_id      Upload ID (see s3_start_multi_upload)
 *
 * @param[in]    seq            Part number. Part numbers must be in ascending order but do not need to be consecutive.
 * @param[out]   etag           Etag returned by the server for that part. (malloc'ed). Must be provided to s3_complete_upload()
 * @param[in]    byteCount      Number of bytes for that part. MINIMUM 5MB.
 * @param[in]    buf            Buffer with the data to upload
 *
 * @return Number of bytes uploaded, or <0 on error
 */
int64_t s3_upload_part(S3BucketContext *bucket, const char *object_name,
		const char *upload_id, int seq,
		char **etag,
		uint64_t byteCount,
		void *buf)
{
	assert(object_name); assert(etag); assert(buf);
	struct S3PutObjectHandler myhandler = {
		{
			prop_cb_upload_part,
			comp_cb_status,
		},
		put_object_data_cb_upload_part
	};
	struct upload_cb_ctx ctx = {
		.cb_status = S3StatusInternalError, // if ever callback is not called
		.upload_cb_buf = buf,
		.upload_cb_bytes = 0,
		.upload_cb_maxbytes = byteCount
	};
	S3_upload_part(bucket, object_name, NULL,
		&myhandler, seq, upload_id, byteCount, NULL, 0,
		&ctx);
	if (ctx.cb_status != S3StatusOK) {
		trace("Upload Part Failed: %s", S3_get_status_name(ctx.cb_status));
		return -1;
	} else {
		*etag = ctx.upload_etag;
		return ctx.upload_cb_bytes;
	}
}

//
// complete
//
struct complete_multi_upload_cb_ctx {
	S3Status cb_status; // MUST be at the head of the struct
	int upload_nr_parts;
	int *upload_seq_ids;
	const char **upload_etags;

	/**
	 * next part that put_cb_complete_upload()
	 * will have to print.
	 */
	int upload_current_part; // start at -1 for header
	/**
	 * If put_cb_complete_upload() has to truncate its output because the end of the buffer has
	 * been reached, it won't increase upload_current_part and will store the number of bytes
	 * already printed in upload_current_offset
	 */
	int upload_current_offset;
};

/**
 * putObjectDataCallback for s3_complete_upload
 * Generate XML for multi upload completion
 */
static int put_cb_complete_upload(int bufferSize, char *buffer, void *cbdata)
{
	int l;
	struct complete_multi_upload_cb_ctx *ctx = cbdata;
	/* We can't sprintf directly to the buffer because we don't
	 * want the null terminator.
	 */
	const int tsize = 1000;
	char tbuf[tsize];
	int n;

	/*
	 * This function will be called many times when a multi-upload completes.
	 * It will be called once to generate the XML header, once for each part,
	 * and once for the XML footer.
	 * We'll know what to do depending on the value of ctx->upload_current_part:
	 *                  -1       ---> add header
	 *     [0, nr_parts-1]       ---> add part
	 *            nr_parts       ---> add footer
	 */
	if (ctx->upload_current_part == -1) {
		n = snprintf(tbuf, tsize, "<CompleteMultipartUpload>");
	} else if (ctx->upload_current_part == ctx->upload_nr_parts) {
		n = snprintf(tbuf, tsize, "</CompleteMultipartUpload>");
	} else if (ctx->upload_current_part > ctx->upload_nr_parts) {
		n = 0;
	} else {
		n = snprintf(tbuf, tsize,
			"<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>",
			ctx->upload_seq_ids[ctx->upload_current_part],
			ctx->upload_etags[ctx->upload_current_part]);
	};

	if (n >= tsize) {
		trace("Error: cannot format S3 part number/etag");
		exit(1); // not sure how to handle that yet
	}

	/* Now that we have generated the XML for ctx->upload_current_part,
	 * Copy it the output buffer... if it fits. If it does not then this
	 * function will be called again with the same ctx->upload_current_part
	 */
	if (n - ctx->upload_current_offset <= bufferSize) {
		l = n - ctx->upload_current_offset;
		if (buffer)
			memcpy(buffer, tbuf + ctx->upload_current_offset, l);
		ctx->upload_current_offset = 0;
		ctx->upload_current_part++;
	} else {
		l = bufferSize;
		if (buffer)
			memcpy(buffer, tbuf + ctx->upload_current_offset, l);
		ctx->upload_current_offset += l;
	}
	return l;
}

/** responseXmlCallback for s3_complete_upload */
static S3Status resp_xml_cb_complete_upload(const char *location, const char *etag, void *cbdata)
{
	return S3StatusOK;
}

/**
 * Count the size of the XML list of the parts for multi upload complete
 */
static int count_complete_bytes(int nr_parts, int *seq_ids, const char **etags)
{
	struct complete_multi_upload_cb_ctx ctx = {
		.upload_nr_parts = nr_parts,
		.upload_seq_ids = seq_ids,
		.upload_etags = etags,
		.upload_current_part = -1,
	};
	int n = 0;
	for(;;) {
		int l = put_cb_complete_upload(INT_MAX, NULL, &ctx);
		if (l <= 0)
			break;
		n += l;
	}
	return n;
}

/**
 * Complete a multi upload.
 * Call this when all parts have been uploaded with s3_upload_part().
 *
 * @param[in]    bucket       Bucket where the object is. Use s3_open_bucket() to get it.
 * @param[in]    object_name   Name (i.e key) of the object
 * @param[in]    upload_id     upload ID
 * @param[in]    nr_parts     Number of parts
 * @param[in]    seq_ids      Array with part number of each part
 * @param[in]    etags        Array with the etag of each part
 *
 * @return 0 on success, or <0 on error
 */
int64_t s3_complete_upload(S3BucketContext *bucket, const char *object_name,
		const char *upload_id,
		int nr_parts,
		int *seq_ids,
		const char **etags)
{
	assert(object_name); assert(upload_id); assert(seq_ids); assert(etags);
	struct S3MultipartCommitHandler myhandler = {
		simple_response_handler,
		put_cb_complete_upload,
		resp_xml_cb_complete_upload,
	};

	int xml_bytes = count_complete_bytes(nr_parts, seq_ids, etags);

	struct complete_multi_upload_cb_ctx ctx = {
		.upload_nr_parts = nr_parts,
		.upload_seq_ids = seq_ids,
		.upload_etags = etags,
		.upload_current_part = -1,
	};
	S3_complete_multipart_upload(bucket, object_name,
		&myhandler,
		upload_id, xml_bytes, NULL, 0,
		&ctx);
	if (ctx.cb_status != S3StatusOK) {
		trace("Upload Complete Failed: %s", S3_get_status_name(ctx.cb_status));
		return -1;
	} else {
		return 0;
	}
}


/**
 * s3_copy_part: make a part from a data range in an object already stored in the bucket.
 *
 * @param[in]    bucket            Bucket where the object is. Use s3_open_bucket() to get it.
 * @param[in]    dest_object_name  Name of the destination object
 * @param[in]    upload_id         Upload ID (see s3_start_multi_upload)
 * @param[in]    seq               Part number. Part numbers must be in ascending order but do not need to be consecutive.
 * @param[out]   etag              Etag returned by the server for that part. (malloc'ed). Must be provided to s3_complete_upload()
 * @param[in]    src_object_name   Name of the source object
 * @param[in]    startOffset       Start offset of the data in the source object.
 * @param[in]    byteCount         Number of bytes for that part. MINIMUM 5MB.
 *
 * @return Number of bytes copied, or <0 on error
 */
int s3_copy_part(S3BucketContext *bucket, const char *dest_object_name,
		const char *upload_id, int seq,
		char **etag,
		const char *src_object_name,
		uint64_t startOffset,
		uint64_t byteCount)
{
	assert(dest_object_name); assert(etag); assert(src_object_name);
	S3Status s = S3StatusInternalError; // if ever callback is not called
	char etagbuf[MAX_ETAG_LEN + 1];
	S3_copy_object_range(bucket, src_object_name,
		NULL /* destinationBucket */,
		dest_object_name, seq,
		upload_id, startOffset, byteCount,
		NULL /* putProperties */,
		NULL /* lastModifiedReturn */,
		MAX_ETAG_LEN /* eTagReturnSize */,
		etagbuf /* eTagReturn */,
		NULL /* requestContext */,
		0 /* timeoutMs */,
		&simple_response_handler,
		(void*) &s);
	if (s != S3StatusOK) {
		trace("S3: Copy Part Failed: %s", S3_get_status_name(s));
		return -1;
	}
	*etag = strdup(etagbuf);
	if (!*etag) {
		trace("S3: strdup() failed");
		return -1;
	}
	return 0;
}


/***************************** INIT ************************/
int s3_init(void)
{
	S3Status s = S3_initialize(NULL, S3_INIT_ALL, NULL);
	if (s != S3StatusOK) {
		trace("S3 failed to initialize: %s", S3_get_status_name(s));
		return -1;
	}
	return 0;
}
