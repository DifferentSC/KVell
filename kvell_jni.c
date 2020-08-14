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
    return 0;
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

void pass_item_callback(struct slab_callback *cb, void *item) {
    cb->is_finished = 1;
    // Invalidate existing items and link it to cb, so that client context can fetch data.
    cb->result = item;
}

void no_pass_item_callback(struct slab_callback *cb, void *item) {
    cb->is_finished = 1;
}

void add_item_callback(struct slab_callback *cb, void *item) {
    memory_index_add(cb, item);
    cb->is_finished = 1;
}

void busy_wait_with_noop(struct slab_callback *cb) {
    while(cb->is_finished==0)
        NOP10();
}

void free_cb(struct slab_callback *cb) {
    if (cb->item != NULL)
        free(cb->item);
    free(cb);
}

/*
void initialize_cond(slab_callback *cb) {
    // Iniitialize mutex
    cb->m = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(cb->m, NULL);
    // Initialize conditional variable
    cb->c = malloc(sizeof(pthread_cond_t));
    pthread_cond_init(cb->c, NULL);
}

void destory_cond(slab_callback *cb) {
    pthread_mutex_destroy(cb->m);
    free(cb->m);
    pthread_cond_destroy(cb->c);
    free(cb->c);
}*/

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    read_native
 * Signature: ([B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_read_1native
        (JNIEnv *env, jobject object, jbyteArray key) {
    int key_size = (*env)->GetArrayLength(env, key);
    jbyte *key_bytes = (*env)->GetByteArrayElements(env, key, NULL);

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    char* item = malloc(sizeof(*meta) + key_size);
    meta = (struct item_metadata *)item;
    cb->cb = pass_item_callback;

    cb->payload = NULL;
    meta->key_size = key_size;
    memcpy(&item[sizeof(*meta)], key_bytes, key_size);
    cb->item = item;
    cb->is_finished = 0;
    kv_read_async(cb);
    // busy waiting with NOP.
    busy_wait_with_noop(cb);

    // Key does not exist, then return NULL.
    if (cb->result == NULL) {

        return NULL;
    }
    // Copy to Java
    jbyteArray javaBytes = (*env)->NewByteArray(env, meta->value_size);
    jbyte *item_value = cb->result + sizeof(*meta) + key_size;
    (*env)->SetByteArrayRegion(env, javaBytes, 0, meta->value_size, item_value);
    free_cb(cb);
    return javaBytes;
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    write_native
 * Signature: ([B[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_write_1native
(JNIEnv *env, jobject object, jbyteArray key, jbyteArray value) {
    int key_size = (*env)->GetArrayLength(env, key);
    jbyte *key_bytes = (*env)->GetByteArrayElements(env, key, NULL);
    int value_size = (*env)->GetArrayLength(env, value);
    jbyte *value_bytes = (*env)->GetByteArrayElements(env, value, NULL);

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    char* item = malloc(sizeof(*meta) + key_size + value_size);
    meta = (struct item_metadata *)item;
    meta->key_size = key_size;
    meta->value_size = value_size;
    memcpy(&item[sizeof(*meta)], key_bytes, key_size);
    memcpy(&item[sizeof(*meta) + key_size], value_bytes, value_size);
    cb->cb = no_pass_item_callback;
    cb->payload = NULL;
    cb->item = item;
    cb->is_finished = 0;
    kv_add_or_update_async(cb);
    busy_wait_with_noop(cb);
    free_cb(cb);
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    add_native
 * Signature: ([B[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_add_1native
(JNIEnv *env, jobject object, jbyteArray key, jbyteArray value) {
    int key_size = (*env)->GetArrayLength(env, key);
    jbyte *key_bytes = (*env)->GetByteArrayElements(env, key, NULL);
    int value_size = (*env)->GetArrayLength(env, value);
    jbyte *value_bytes = (*env)->GetByteArrayElements(env, value, NULL);

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    char* item = malloc(sizeof(*meta) + key_size + value_size);
    meta = (struct item_metadata *)item;
    cb->cb = add_item_callback;
    cb->payload = NULL;
    meta->key_size = key_size;
    meta->value_size = value_size;
    memcpy(&item[sizeof(*meta)], key_bytes, key_size);
    memcpy(&item[sizeof(*meta) + key_size], value_bytes, value_size);
    cb->item = item;
    cb->is_finished = 0;
    kv_add_async(cb);
    busy_wait_with_noop(cb);
    free_cb(cb);
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    delete_native
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_delete_1native
(JNIEnv *env, jobject object, jbyteArray key) {
    int key_size = (*env)->GetArrayLength(env, key);
    jbyte *key_bytes = (*env)->GetByteArrayElements(env, key, NULL);

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    cb->item = malloc(sizeof(*meta) + key_size);
    meta = (struct item_metadata *)(cb->item);
    cb->cb = no_pass_item_callback;
    cb->payload = NULL;
    meta->key_size = key_size;
    char *item_key = cb->item + sizeof(*meta);
    memcpy(item_key, key_bytes, key_size);
    kv_remove_async(cb);
    busy_wait_with_noop(cb);
    free_cb(cb);
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    append_native
 * Signature: ([B[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_append_1native
(JNIEnv *env, jobject object, jbyteArray key, jbyteArray item) {
    int key_size = (*env)->GetArrayLength(env, key);
    jbyte *key_bytes = (*env)->GetByteArrayElements(env, key, NULL);

    // Read first
    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    cb->item = malloc(sizeof(*meta) + key_size);
    meta = (struct item_metadata *)item;
    cb->cb = pass_item_callback;
    cb->payload = NULL;
    meta->key_size = key_size;
    char *item_key = cb->item + sizeof(*meta);
    memcpy(item_key, key_bytes, key_size);
    kv_read_async(cb);
    // busy waiting (could it be changed to conditional variables?)
    while(cb->is_finished != 1);

    jbyte *item_value = cb->item + sizeof(*meta) + key_size;
    // Then let's append.
    int item_size = (*env)->GetArrayLength(env, item);
    jbyte *item_bytes = (*env)->GetByteArrayElements(env, item, NULL);

    struct slab_callback *append_cb = malloc(sizeof(*cb));
    append_cb->item = malloc(sizeof(struct meta*) + meta->key_size + meta->value_size + item_size);
    struct item_metadata *append_meta = (struct item_metadata *)item;
    memcpy(append_cb->item + sizeof(struct meta*), key_bytes, meta->key_size);
    memcpy(append_cb->item + sizeof(struct meta*) + meta->key_size, item_value, meta->value_size);
    memcpy(append_cb->item + sizeof(struct meta*) + meta->key_size + meta->value_size, item_bytes, item_size);
    append_cb->cb = pass_item_callback;
    append_cb->payload = NULL;
    append_meta->key_size = key_size;
    append_meta->value_size = meta->value_size + item_size;
    kv_add_or_update_async(append_cb);

    while(cb->is_finished != 1)
        NOP10();

    free(cb->item);
    free(cb);
    free(append_cb->item);
    free(append_cb->item);
}