#include "redismodule.h"
#include <string.h>

static int intrinsic_hpackall_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static int intrinsic_happend_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
static int intrinsic_hmappend_command(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

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

struct hpackall_context {
  char *buffer;
  int32_t size;
  int32_t capacity;
  int32_t count;
};

static inline char *intrinsic_hpackall_check_buffer(struct hpackall_context *ctx, int32_t required) {
  if (ctx->capacity - ctx->size < required) {
    ctx->capacity = 2 * (ctx->capacity + required);
    ctx->buffer = RedisModule_Realloc(ctx->buffer, ctx->capacity);
  }
  return ctx->buffer + ctx->size;
}

static void intrinsic_hpackall_sink(const char **key, int32_t *ksize, const char **value, int32_t *vsize, int32_t num, void *privdata) {
  struct hpackall_context *ctx = privdata;
  char *buffer;
  int32_t idx, ks, vs, rs;
  for (idx = 0; idx < num; ++idx) {
    ks = ksize[idx];
    vs = vsize[idx];
    rs = ks + vs + 8;
    buffer = intrinsic_hpackall_check_buffer(ctx, rs);
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
  struct hpackall_context hctx = {NULL, 4, 0, 0};
  RedisModuleKey *key;
  if (argc != 2) {
    return RedisModule_WrongArity(ctx);
  }
  key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_WRITE);
  intrinsic_hpackall_check_buffer(&hctx, 1024 * 1024);
  if (RedisModule_HashGetAll(key, intrinsic_hpackall_sink, &hctx) == REDISMODULE_OK) {
    memcpy(hctx.buffer, &hctx.count, sizeof(hctx.count));
    RedisModule_ReplyWithStringBuffer(ctx, hctx.buffer, hctx.size);
  } else {
    RedisModule_ReplyWithError(ctx, "ERR internal error");
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
