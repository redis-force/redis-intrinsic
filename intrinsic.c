#include "redismodule.h"
#include <string.h>

static int intrinsic_hpackall_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static int intrinsic_happend_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static int intrinsic_hmappend_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static int intrinsic_mexists_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static int intrinsic_hmpack_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static int intrinsic_hmexists_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

struct intrinsic_command {
  const char *name;
  RedisModuleCmdFunc entry;
  const char *flags;
  int first_key;
  int last_key;
  int key_step;
};

static struct intrinsic_command intrinsics[] = {
  {"hpackall", intrinsic_hpackall_command, "readonly", 1, 1, 1},
  {"happend", intrinsic_happend_command, "write deny-oom", 1, 1, 1},
  {"hmappend", intrinsic_hmappend_command, "write deny-oom", 1, 1, 1},
  {"mexists", intrinsic_mexists_command, "readonly", 1, 1, 1},
  {"hmpack", intrinsic_hmpack_command, "readonly", 1, 1, 1},
  {"hmexists", intrinsic_hmexists_command, "readonly", 1, 1, 1},
};

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  size_t idx;
  REDISMODULE_NOT_USED(argv);
  REDISMODULE_NOT_USED(argc);
  if (RedisModule_Init(ctx, "intrinsic", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }
  for (idx = 0; idx < sizeof(intrinsics) / sizeof(intrinsics[0]); ++idx) {
    struct intrinsic_command *command = intrinsics + idx;
    if (RedisModule_CreateCommand(ctx, command->name, command->entry, command->flags,
          command->first_key, command->last_key, command->key_step) == REDISMODULE_ERR) {
      RedisModule_Log(ctx, "warning", "could not register intrinsic command %s", command->name);
      return REDISMODULE_ERR;
    }
  }
  return REDISMODULE_OK;
}

struct hpack_context {
  char *buffer;
  int32_t size;
  int32_t capacity;
  int32_t count;
};

static inline char *intrinsic_hpack_check_buffer(struct hpack_context *ctx, int32_t required) {
  if (ctx->capacity - ctx->size < required) {
    ctx->capacity = 2 * (ctx->capacity + required);
    ctx->buffer = RedisModule_Realloc(ctx->buffer, ctx->capacity);
  }
  return ctx->buffer + ctx->size;
}

static void intrinsic_hpackall_sink(const char **key, int32_t *ksize, const char **value, int32_t *vsize, int32_t num, void *privdata) {
  struct hpack_context *ctx = privdata;
  char *buffer;
  int32_t idx, ks, vs, rs;
  for (idx = 0; idx < num; ++idx) {
    ks = ksize[idx];
    vs = vsize[idx];
    rs = ks + vs + 8;
    buffer = intrinsic_hpack_check_buffer(ctx, rs);
    ctx->size += rs;
    memcpy(buffer, &ks, 4);
    buffer += 4;
    memcpy(buffer, key[idx], ks);
    buffer += ks;
    memcpy(buffer, &vs, 4);
    buffer += 4;
    memcpy(buffer, value[idx], vs);
  }
  ctx->count += num;
}

static int intrinsic_hpackall_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  /* hpackall key */
  struct hpack_context hctx = {NULL, 4, 0, 0};
  RedisModuleKey *key;
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  if (key != NULL) {
    intrinsic_hpack_check_buffer(&hctx, 1024 * 1024);
    if (RedisModule_HashGetAll(key, intrinsic_hpackall_sink, &hctx) == REDISMODULE_OK) {
      memcpy(hctx.buffer, &hctx.count, sizeof(hctx.count));
      RedisModule_ReplyWithStringBuffer(ctx, hctx.buffer, hctx.size);
    } else {
      RedisModule_ReplyWithError(ctx, "ERR internal error");
    }
    RedisModule_CloseKey(key);
  } else {
    RedisModule_ReplyWithNull(ctx);
  }
  RedisModule_Free(hctx.buffer);
  return REDISMODULE_OK;
}

static int intrinsic_hash_append_command(RedisModuleCtx *ctx, RedisModuleString *key, RedisModuleString **kv, int kvnum, int mappend) {
  int idx, eidx, rc = REDISMODULE_OK;
  size_t len;
  const char *str;
  long long *reply = RedisModule_Alloc(sizeof(long long) * kvnum);
  RedisModuleKey *realkey = RedisModule_OpenKey(ctx, key, REDISMODULE_WRITE);
  RedisModuleString *value;

  for (idx = 0; idx < kvnum; ++idx) {
    eidx = 2 * idx;
    if ((rc = RedisModule_HashGet(realkey, REDISMODULE_HASH_NONE, kv[eidx], &value, NULL)) != REDISMODULE_OK) {
      goto cleanup_exit;
    }
    if (value != NULL) {
      RedisModuleString *newvalue = RedisModule_CreateStringFromString(ctx, value);
      RedisModule_FreeString(ctx, value);
      value = newvalue;
      str = RedisModule_StringPtrLen(kv[eidx + 1], &len);
      if ((rc = RedisModule_StringAppendBuffer(ctx, value, str, len)) != REDISMODULE_OK) {
        RedisModule_FreeString(ctx, value);
        goto cleanup_exit;
      }
    } else {
      value = kv[eidx + 1];
    }
    RedisModule_HashSet(realkey, REDISMODULE_HASH_NONE, kv[eidx], value, NULL);
    RedisModule_StringPtrLen(value, &len);
    reply[idx] = (long long) len;
    if (value != kv[eidx + 1]) {
      RedisModule_FreeString(ctx, value);
    }
  }

cleanup_exit:
  RedisModule_CloseKey(realkey);

  if (rc != REDISMODULE_OK) {
    RedisModule_ReplyWithError(ctx, "internal error");
  } else {
    if (mappend) {
      RedisModule_ReplyWithArray(ctx, kvnum);
    }
    for (idx = 0; idx < kvnum; ++idx) {
      RedisModule_ReplyWithLongLong(ctx, reply[idx]);
    }
  }

  RedisModule_Free(reply);
  return rc;
}

static int intrinsic_happend_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  /* happend key field value */
  if (argc != 4) {
    return RedisModule_WrongArity(ctx);
  }
  return intrinsic_hash_append_command(ctx, argv[1], argv + 2, 1, 0);
}

static int intrinsic_hmappend_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  /* hmappend key field value [field value ...] */
  if (argc < 4 || (argc - 2) % 2 != 0) {
    return RedisModule_WrongArity(ctx);
  }
  return intrinsic_hash_append_command(ctx, argv[1], argv + 2, (argc - 2) / 2, 1);
}

static int intrinsic_mexists_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  int idx;
  RedisModuleKey *key;
  const char *reply;
  /* mexists key [key ...] */
  if (argc < 2) {
    return RedisModule_WrongArity(ctx);
  }
  RedisModule_ReplyWithArray(ctx, argc - 1);
  for (idx = 1; idx < argc; ++idx) {
    key = RedisModule_OpenKey(ctx, argv[idx], REDISMODULE_READ);
    if (key != NULL) {
      reply = "1";
      RedisModule_CloseKey(key);
    } else {
      reply = "0";
    }
    RedisModule_ReplyWithStringBuffer(ctx, reply, 1);
  }
  return REDISMODULE_OK;
}

#define HASH_GET_BATCH_SIZE (32)
typedef void (*intrinsic_hash_get_batch_impl)(RedisModuleKey *key, int flags, RedisModuleString **fields, void **results);

static void intrinsic_hash_get_batch(RedisModuleKey *key, int flags, RedisModuleString **fields, void **results) {
  RedisModule_HashGet(key, flags, fields[0], results[0], fields[1], results[1], fields[2], results[2],
      fields[3], results[3], fields[4], results[4], fields[5], results[5], fields[6], results[6],
      fields[7], results[7], fields[8], results[8], fields[9], results[9], fields[10], results[10],
      fields[11], results[11], fields[12], results[12], fields[13], results[13], fields[14], results[14],
      fields[15], results[15], fields[16], results[16], fields[17], results[17], fields[18], results[18],
      fields[19], results[19], fields[20], results[20], fields[21], results[21], fields[22], results[22],
      fields[23], results[23], fields[24], results[24], fields[25], results[25], fields[26], results[26],
      fields[27], results[27], fields[28], results[28], fields[29], results[29], fields[30], results[30],
      fields[31], results[31], NULL);
}
static void intrinsic_hash_get_batch_half(RedisModuleKey *key, int flags, RedisModuleString **fields, void **results) {
  RedisModule_HashGet(key, flags, fields[0], results[0], fields[1], results[1], fields[2], results[2],
      fields[3], results[3], fields[4], results[4], fields[5], results[5], fields[6], results[6],
      fields[7], results[7], fields[8], results[8], fields[9], results[9], fields[10], results[10],
      fields[11], results[11], fields[12], results[12], fields[13], results[13], fields[14], results[14],
      fields[15], results[15], NULL);
}

static void intrinsic_hash_get_batch_quarter(RedisModuleKey *key, int flags, RedisModuleString **fields, void **results) {
  RedisModule_HashGet(key, flags, fields[0], results[0], fields[1], results[1], fields[2], results[2],
      fields[3], results[3], fields[4], results[4], fields[5], results[5], fields[6], results[6],
      fields[7], results[7], NULL);
}

typedef void (*intrinsic_hmget_sink)(RedisModuleCtx *ctx, void **results, int count, void *privdata);
typedef void (*intrinsic_hmget_nokey)(RedisModuleCtx *ctx, int count);

static int intrinsic_hmget(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int flags,
    void **results, intrinsic_hmget_sink sink, void *privdata, intrinsic_hmget_nokey nokey) {
  int batch_size;
  RedisModuleKey *key;
  intrinsic_hash_get_batch_impl hash_get;
  if (argc < 3) {
    return RedisModule_WrongArity(ctx);
  }
  key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
  if (key != NULL) {
    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_HASH) {
      return RedisModule_ReplyWithError(ctx, "ERR Wrong Type");
    }
    argv = argv + 2;
    argc -= 2;
    while (argc > 0) {
      if (argc > HASH_GET_BATCH_SIZE) {
        hash_get = intrinsic_hash_get_batch;
        batch_size = HASH_GET_BATCH_SIZE;
      } else if (argc > HASH_GET_BATCH_SIZE / 2) {
        hash_get = intrinsic_hash_get_batch_half;
        batch_size = HASH_GET_BATCH_SIZE / 2;
      } else if (argc > HASH_GET_BATCH_SIZE / 4) {
        hash_get = intrinsic_hash_get_batch_quarter;
        batch_size = HASH_GET_BATCH_SIZE / 4;
      } else {
        RedisModule_HashGet(key, flags, *(argv++), results[0], NULL);
        sink(ctx, results, 1, privdata);
        argc--;
        continue;
      }
      hash_get(key, flags, argv, results);
      sink(ctx, results, batch_size, privdata);
      argv += batch_size;
      argc -= batch_size;
    }
    RedisModule_CloseKey(key);
  } else {
    nokey(ctx, argc - 2);
  }
  return REDISMODULE_OK;
}

static void intrinsic_hmpack_sink(RedisModuleCtx *rctx, void **results, int count, void *privdata) {
  struct hpack_context *ctx = privdata;
  char *buffer;
  const char *val;
  size_t vsize;
  int32_t idx, vs, rs;
  RedisModuleString *str;
  REDISMODULE_NOT_USED(rctx);
  for (idx = 0; idx < count; ++idx) {
    if ((str = *(RedisModuleString **)results[idx]) != NULL) {
      val = RedisModule_StringPtrLen(str, &vsize);
      vs = (int32_t) vsize;
      rs = vs + 4;
      buffer = intrinsic_hpack_check_buffer(ctx, rs);
      ctx->size += rs;
      memcpy(buffer, &vs, 4);
      memcpy(buffer + 4, val, vs);
    } else {
      buffer = intrinsic_hpack_check_buffer(ctx, 4);
      ctx->size += 4;
      memset(buffer, 0, 4);
    }
  }
}

static void intrinsic_hmpack_nokey(RedisModuleCtx *ctx, int count) {
  REDISMODULE_NOT_USED(count);
  RedisModule_ReplyWithNull(ctx);
}

static int intrinsic_hmpack_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  /* hmpack key field [field ...] */
  struct hpack_context hctx = {NULL, 0, 0, 0};
  int idx, rc;
  RedisModuleString *results[HASH_GET_BATCH_SIZE];
  void *results_ptr[HASH_GET_BATCH_SIZE];
  for (idx = 0; idx < HASH_GET_BATCH_SIZE; ++idx) {
    results_ptr[idx] = results + idx;
  }
  intrinsic_hpack_check_buffer(&hctx, 1024 * 1024);
  rc = intrinsic_hmget(ctx, argv, argc, REDISMODULE_HASH_NONE, results_ptr, intrinsic_hmpack_sink, &hctx, intrinsic_hmpack_nokey);
  if (rc == REDISMODULE_OK) {
    /* send packed data */
    RedisModule_ReplyWithStringBuffer(ctx, hctx.buffer, hctx.size);
  }
  RedisModule_Free(hctx.buffer);
  return rc;
}

static void intrinsic_hmexists_sink(RedisModuleCtx *ctx, void **results, int count, void *privdata) {
  int *total = privdata;
  const char *ret;
  if (*total > 0) {
    RedisModule_ReplyWithArray(ctx, *total);
    *total = 0;
  }
  while (count-- > 0) {
    ret = **(int **)(results++) != 0 ? "1" : "0";
    RedisModule_ReplyWithStringBuffer(ctx, ret, 1);
  }
}

static void intrinsic_hmexists_nokey(RedisModuleCtx *ctx, int count) {
  RedisModule_ReplyWithArray(ctx, count);
  while (count-- > 0) {
    RedisModule_ReplyWithStringBuffer(ctx, "0", 1);
  }
}

static int intrinsic_hmexists_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  /* hmexists key field [field ...] */
  int total = argc - 2, idx;
  int results[HASH_GET_BATCH_SIZE];
  void *results_ptr[HASH_GET_BATCH_SIZE];
  for (idx = 0; idx < HASH_GET_BATCH_SIZE; ++idx) {
    results_ptr[idx] = results + idx;
  }
  return intrinsic_hmget(ctx, argv, argc, REDISMODULE_HASH_EXISTS, results_ptr, intrinsic_hmexists_sink, &total, intrinsic_hmexists_nokey);
}
