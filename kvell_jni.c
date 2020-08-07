#include "kvell_jni.h"
#include "headers.h"
#include <pthread.h>
//
// Created by 이계원 on 2020/08/06.
//

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    open_native
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_open_1native
        (JNIEnv *env, jobject object) {
    // init workers. Please make sure that databases are deleted.
    slab_workers_init(1, 8);
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    close_native
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_close_1native
(JNIEnv *env, jobject object) {
    // Do nothing.
}

void do_nothing_callback(struct slab_callback *cb, void *item) {
    cb->is_finished = 1;
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    read_native
 * Signature: ([B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_read_1native
        (JNIEnv *env, jobject object, jbyteArray key) {
    int key_size = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, NULL);

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    cb->item = malloc(sizeof(*meta) + key_size);
    meta = (struct item_metadata *)item;
    cb->cb = do_nothing_callback();
    cb->payload = NULL;
    meta->key_size = key_size;
    memcpy(cb->item + sizeof(*meta), key_bytes, key_size);
    kv_read_async(cb);
    // busy waiting (could it be changed to conditional variables?)
    while(cb->is_finished != 1);

    if (item_metadata->value_size == 0) {
        return null;
    }
    // Copy to Java
    jbyte *item_value = cb->item + sizeof(*meta) + key_size;
    env->SetByteArrayRegion(javaBytes, 0, meta->value_size, item_value);
    free(cb->item);
    free(cb);
    return item_value;
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    write_native
 * Signature: ([B[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_write_1native
(JNIEnv *env, jobject object, jbyteArray key, jbyteArray value) {
    int key_size = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, NULL);
    int value_size = env->GetArrayLength(value);
    jbyte *value_bytes = env->GetByteArrayElements(value, NULL);

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    cb->item = malloc(sizeof(*meta) + key_size + value_size);
    meta = (struct item_metadata *)item;
    cb->cb = do_nothing_callback();
    cb->payload = NULL;
    meta->key_size = key_size;
    meta->value_size = value_size;
    char *item_key = cb->item + sizeof(*meta);
    memcpy(cb->item + sizeof(*meta), key_bytes, key_size);
    memcpy(cb->item + sizeof(*meta) + key_size, value_bytes, value_size);
    kv_add_or_update_async(cb);
    // busy waiting (could it be changed to conditional variables?)
    while(cb->is_finished != 1);
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    delete_native
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_delete_1native
(JNIEnv *env, jobject object, jbyteArray key) {
    int key_size = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, NULL);

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    cb->item = malloc(sizeof(*meta) + key_size);
    meta = (struct item_metadata *)item;
    cb->cb = do_nothing_callback();
    cb->payload = NULL;
    meta->key_size = key_size;
    char *item_key = cb->item + sizeof(*meta);
    memcpy(item_key, key_bytes, key_size);
    kv_remove_async(cb);
    while(cb->is_finished != 1);
    free(cb->item);
    free(cb);
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    append_native
 * Signature: ([B[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_append_1native
(JNIEnv *env, jobject object, jbyteArray key, jbyteArray item) {
    int key_size = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, nullptr);

    // Read first
    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    cb->item = malloc(sizeof(*meta) + key_size);
    meta = (struct item_metadata *)item;
    cb->cb = do_nothing_callback();
    cb->payload = NULL;
    meta->key_size = key_size;
    char *item_key = &item[sizeof(*meta)];
    memcpy(item_key, key_bytes, key_size);
    kv_read_async(cb);
    // busy waiting (could it be changed to conditional variables?)
    while(cb->is_finished != 1);

    jbyte *item_value = cb->item + sizeof(*meta) + key_size;
    // Then let's append.
    int item_size = env->GetArrayLength(item);
    jbyte *item_bytes = env->GetByteArrayElements(item, NULL);

    struct slab_callback *append_cb = malloc(sizeof(*cb));
    append_cb->item = malloc(sizeof(struct meta*) + cb->item->key_size + cb->item->value_size + item_size);
    struct item_metadata *append_meta = (struct item_metadata *)item;
    memcpy(append_cb->item + sizeof(struct meta*) + cb->item->key_size, item_value, cb->item->value_size);
    append_cb->cb = do_nothing_callback();
    append_cb->payload = NULL;
    append_meta->key_size = key_size;
    append_meta->value_size = cb->item->value_size + item_size;
    kv_add_or_update(append_cb);

    while(cb->is_finished != 1);

    free(cb->item);
    free(cb);
    free(append_cb->item);
    free(append_cb->item);
}