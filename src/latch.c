#include "strm.h"
#include "queue.h"
#include <pthread.h>

const char* strm_p(strm_value val);

struct recv_data {
  strm_stream* strm;
  strm_callback func;
};

struct latch_data {
  struct strm_queue* dq;        /* data queue */
  struct strm_queue* rq;        /* receiver queue */
  pthread_mutex_t mtx;
  int closed;
};

int
strm_latch_finish_p(strm_stream* latch)
{
  struct latch_data* c;

  if (latch->mode == strm_consumer) return 0;
  c = latch->data;
  return strm_queue_empty_p(c->dq);
}

static int
latch_push(strm_stream* strm, strm_value data)
{
  struct latch_data* d = strm->data;
  struct recv_data* r;

  if (strm->mode != strm_consumer) {
    return STRM_NG;
  }
  pthread_mutex_lock(&d->mtx);
  r = strm_queue_get(d->rq);
  if (!r) {
    strm_value* v = malloc(sizeof(strm_value));
    *v = data;
    strm_queue_add(d->dq, v);
  }
  pthread_mutex_unlock(&d->mtx);
  if (r) {
    (*r->func)(r->strm, data);
    free(r);
  }
  return STRM_OK;
}

void
strm_latch_receive(strm_stream* latch, strm_stream* strm, strm_callback func)
{
  struct latch_data* d;
  strm_value* v;
  int eos = FALSE;

  assert(latch->start_func == latch_push);
  d = latch->data;
  pthread_mutex_lock(&d->mtx);
  v = strm_queue_get(d->dq);
  if (!v) {
    if (d->closed) {
      eos = TRUE;
    }
    else {
      struct recv_data* r = malloc(sizeof(struct recv_data));
      r->strm = strm;
      r->func = func;
      strm_queue_add(d->rq, r);
    }
  }
  pthread_mutex_unlock(&d->mtx);
  if (v) {
    (*func)(strm, *v);
    free(v);
  }
  else if (eos) {
    (*func)(strm, strm_nil_value());
  }
}

static int
latch_close(strm_stream* strm, strm_value data)
{
  struct latch_data* d = strm->data;

  for (;;) {
    struct recv_data* r;

    pthread_mutex_lock(&d->mtx);
    d->closed = TRUE;
    r = strm_queue_get(d->rq);
    pthread_mutex_unlock(&d->mtx);
    if (!r) break;
    (*r->func)(r->strm, data);
    free(r);
  }
  return STRM_OK;
}

strm_stream*
strm_latch_new()
{
  struct latch_data* d = malloc(sizeof(struct latch_data));

  assert(d != NULL);
  d->dq = strm_queue_new();
  d->rq = strm_queue_new();
  d->closed = FALSE;
  pthread_mutex_init(&d->mtx, NULL);
  return strm_stream_new(strm_consumer, latch_push, latch_close, d);
}

struct zip_data {
  strm_array a;
  strm_int i;
  strm_int len;
  strm_stream* latch[0];
};

static int zip_start(strm_stream* strm, strm_value data);

static int
zip_iter(strm_stream* strm, strm_value data)
{
  struct zip_data* z = strm->data;

  if (!z) return STRM_OK;
  if (strm_nil_p(data)) {
    strm_int i;

    for (i=0; i<z->len; i++) {
      strm_stream_close(z->latch[i]);
    }
    strm_stream_close(strm);
    return STRM_OK;
  }
  strm_ary_ptr(z->a)[z->i++] = data;
  if (z->i < z->len) {
    strm_latch_receive(z->latch[z->i], strm, zip_iter);
  }
  else {
    strm_int i;
    strm_int done = 0;

    for (i=0; i<z->len; i++){
      if (strm_latch_finish_p(z->latch[i])) {
        done = 1;
        break;
      }
    }
    if (done) {
      strm_emit(strm, z->a, NULL);
      for (i=0; i<z->len; i++){
        strm_stream_close(z->latch[i]);
      }
      strm_stream_close(strm);
    }
    else {
      strm_emit(strm, z->a, zip_start);
    }
  }
  return STRM_OK;
}

static int
zip_start(strm_stream* strm, strm_value data)
{
  struct zip_data* z = strm->data;

  if (z) {
    z->i = 0;
    z->a = strm_ary_new(NULL, z->len);
    strm_latch_receive(z->latch[0], strm, zip_iter);
  }
  return STRM_OK;
}

static int
zip_close(strm_stream* strm, strm_value data)
{
  strm->data = NULL;
  return STRM_OK;
}

static int
exec_zip(strm_stream* strm, int argc, strm_value* args, strm_value* ret)
{
  struct zip_data* z = malloc(sizeof(struct zip_data)+sizeof(strm_stream*)*argc);
  strm_int i;
  strm_stream* s;

  z->i = 0;
  z->len = argc;
  for (i=0; i<argc; i++) {
    strm_value r;
    s = strm_latch_new();
    strm_connect(strm, args[i], strm_stream_value(s), &r);
    z->latch[i] = s;
  }
  *ret = strm_stream_value(strm_stream_new(strm_producer, zip_start, zip_close, z));
  return STRM_OK;
}

struct concat_data {
  strm_int i;
  strm_int len;
  strm_stream* latch[0];
};

static int
concat_iter(strm_stream* strm, strm_value data)
{
  struct concat_data* d = strm->data;

  if (!d) return STRM_OK;
  strm_emit(strm, data, NULL);
  if (strm_nil_p(data) || strm_latch_finish_p(d->latch[d->i])) {
    strm_stream_close(d->latch[d->i]);
    d->i++;
  }
  if (d->i < d->len) {
    strm_latch_receive(d->latch[d->i], strm, concat_iter);
  }
  else {
    strm_stream_close(strm);
  }
  return STRM_OK;
}

static int
concat_start(strm_stream* strm, strm_value data)
{
  struct concat_data* d = strm->data;

  if (d) {
    strm_latch_receive(d->latch[d->i], strm, concat_iter);
  }
  return STRM_OK;
}

static int
exec_concat(strm_stream* strm, int argc, strm_value* args, strm_value* ret)
{
  struct concat_data* d = malloc(sizeof(struct concat_data)+sizeof(strm_stream*)*argc);
  strm_int i;
  strm_stream* s;

  d->i = 0;
  d->len = argc;
  for (i=0; i<argc; i++) {
    strm_value r;
    s = strm_latch_new();
    strm_connect(strm, args[i], strm_stream_value(s), &r);
    d->latch[i] = s;
  }
  *ret = strm_stream_value(strm_stream_new(strm_producer, concat_start, NULL, d));
  return STRM_OK;
}

static int
chan_func(strm_stream* strm, strm_value data)
{
  strm_emit(strm, data, NULL);
  return STRM_OK;
}

static int
exec_chan(strm_stream* strm, int argc, strm_value* args, strm_value* ret)
{
  *ret = strm_stream_value(strm_stream_new(strm_filter, chan_func, NULL, NULL));
  return STRM_OK;
}

void
strm_latch_init(strm_state* state)
{
  strm_var_def(state, "&", strm_cfunc_value(exec_zip));
  strm_var_def(state, "zip", strm_cfunc_value(exec_zip));
  strm_var_def(state, "concat", strm_cfunc_value(exec_concat));
  strm_var_def(state, "chan", strm_cfunc_value(exec_chan));
}
