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
    slab_workers_init(1, 32);
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

// cb->result should be 
void pass_item_callback(struct slab_callback *cb, void *item) {
    cb->is_finished = 1;
    if (item != NULL && ((struct item_metadata*)item)->key_size > 0) {
        // Invalidate existing items and link it to cb, so that client context can fetch data.
        struct item_metadata *meta = (struct item_metadata*)item;
        // Copy the result because the page could be evicted after the callback. This needs to be freed.
        cb->result = malloc(sizeof(*meta) + meta->key_size + meta->value_size);
        memcpy(cb->result, item, sizeof(*meta) + meta->key_size + meta->value_size);
    } else {
        cb->result = NULL;
    }
}

void no_pass_item_callback(struct slab_callback *cb, void *item) {
    if (cb->is_new_item) {
        memory_index_add(cb, item); // Why should I do this on my own? :(
    }
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

void* read_internal(jbyte* key_bytes, int key_size) {

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
    void* result = cb->result;
    free_cb(cb);
    // This needs to be freed.
    return result;

}

void update_internal(jbyte* key_bytes, int key_size, jbyte* value_bytes, int value_size) {

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    char* item = malloc(sizeof(*meta) + key_size + value_size);
    meta = (struct item_metadata *)item;
    cb->cb = no_pass_item_callback;
    cb->payload = NULL;
    meta->key_size = key_size;
    meta->value_size = value_size;
    memcpy(&item[sizeof(*meta)], key_bytes, key_size);
    memcpy(&item[sizeof(*meta) + key_size], value_bytes, value_size);
    cb->item = item;
    cb->is_finished = 0;
    kv_add_or_update_async(cb);
    busy_wait_with_noop(cb);
    free_cb(cb);

}

void add_internal(jbyte* key_bytes, int key_size, jbyte* value_bytes, int value_size) {

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    char* item = malloc(sizeof(*meta) + key_size + value_size);
    meta = (struct item_metadata *)item;
    cb->cb = no_pass_item_callback;
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

void delete_internal(jbyte* key_bytes, int key_size) {

    struct slab_callback *cb = malloc(sizeof(*cb));
    struct item_metadata *meta;
    char* item = malloc(sizeof(*meta) + key_size);
    meta = (struct item_metadata *)item;
    cb->cb = no_pass_item_callback;

    cb->payload = NULL;
    meta->key_size = key_size;
    memcpy(&item[sizeof(*meta)], key_bytes, key_size);
    cb->item = item;
    cb->is_finished = 0;
    kv_remove_async(cb);
    // busy waiting with NOP.
    busy_wait_with_noop(cb);
    free_cb(cb);
    
}

/*
 * Class:     edu_useoul_streamix_kvell_flink_KVell
 * Method:    read_native
 * Signature: ([B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_kvell_1flink_KVell_read_1native
        (JNIEnv *env, jobject object, jbyteArray key) {
    int key_size = (*env)->GetArrayLength(env, key);
    jbyte *key_bytes = (*env)->GetByteArrayElements(env, key, NULL);

    void* result = read_internal(key_bytes, key_size);

    // Key does not exist, then return NULL.
    if (result == NULL) {
        return NULL;
    }
    // Retrieve item
    struct item_metadata* meta = (struct item_metadata*)result;
    jbyteArray javaBytes = (*env)->NewByteArray(env, meta->value_size);
    jbyte *item_value = result + sizeof(*meta) + key_size;
    (*env)->SetByteArrayRegion(env, javaBytes, 0, meta->value_size, item_value);
    free(result);
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

    update_internal(key_bytes, key_size, value_bytes, value_size);
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
    
    add_internal(key_bytes, key_size, value_bytes, value_size);
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

    delete_internal(key_bytes, key_size);
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
    int item_size = (*env)->GetArrayLength(env, item);
    jbyte *item_bytes = (*env)->GetByteArrayElements(env, item, NULL);

    // Read first to append.
    void* result = read_internal(key_bytes, key_size);

    if (result == NULL) {
        // Just add when there is no existing value.
        add_internal(key_bytes, key_size, item_bytes, item_size);
    } else {
        // Otherwise, add existing value after deleting it.
        struct item_metadata* meta = (struct item_metadata*)result;

        int old_key_size = meta->key_size;
        int old_value_size = meta->value_size;
        void* old_value = malloc(meta->value_size);
        memcpy(old_value, result + sizeof(*meta) + old_key_size, old_value_size);
        free(result);
        // We need to delete the data firstly, because KVell does not support updating values with growing length.
        delete_internal(key_bytes, key_size);
        // Let's add eappended data.
        jbyte* new_value_bytes = malloc(old_value_size + item_size);
        memcpy(new_value_bytes, old_value, old_value_size);
        memcpy(new_value_bytes + old_value_size, item_bytes, item_size);
        add_internal(key_bytes, key_size, new_value_bytes, old_value_size + item_size);       
        free(old_value);
        free(new_value_bytes);
    }
}