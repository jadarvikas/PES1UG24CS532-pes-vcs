// object.c — Content-addressable object store
//
// Every piece of data (file contents, directory listings, commits) is stored
// as an "object" named by its SHA-256 hash. Objects are stored under
// .pes/objects/XX/YYYYYY... where XX is the first two hex characters of the
// hash (directory sharding).
//
// PROVIDED functions: compute_hash, object_path, object_exists, hash_to_hex, hex_to_hash
// TODO functions:     object_write, object_read

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

// Get the filesystem path where an object should be stored.
// Format: .pes/objects/XX/YYYYYYYY...
// The first 2 hex chars form the shard directory; the rest is the filename.
void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Helper: return the type string for an ObjectType
static const char* object_type_string(ObjectType type) {
    switch (type) {
        case OBJ_BLOB:   return "blob";
        case OBJ_TREE:   return "tree";
        case OBJ_COMMIT: return "commit";
        default:         return NULL;
    }
}

static int parse_type_string(const char *s, ObjectType *type_out) {
    if (strcmp(s, "blob") == 0) {
        *type_out = OBJ_BLOB;
        return 0;
    } else if (strcmp(s, "tree") == 0) {
        *type_out = OBJ_TREE;
        return 0;
    } else if (strcmp(s, "commit") == 0) {
        *type_out = OBJ_COMMIT;
        return 0;
    }
    return -1;
}
// Write an object to the store.
//
// Object format on disk:
//   "<type> <size>\0<data>"
//   where <type> is "blob", "tree", or "commit"
//   and <size> is the decimal string of the data length
//
// Steps:
//   1. Build the full object: header ("blob 16\0") + data
//   2. Compute SHA-256 hash of the FULL object (header + data)
//   3. Check if object already exists (deduplication) — if so, just return success
//   4. Create shard directory (.pes/objects/XX/) if it doesn't exist
//   5. Write to a temporary file in the same shard directory
//   6. fsync() the temporary file to ensure data reaches disk
//   7. rename() the temp file to the final path (atomic on POSIX)
//   8. Open and fsync() the shard directory to persist the rename
//   9. Store the computed hash in *id_out
//
// Returns 0 on success, -1 on error.
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // 1. Build header: "<type> <size>\0"
    const char *type_str = object_type_string(type);
    if (!type_str) return -1;

    // Calculate number of decimal digits in len
    size_t digits = 0;
    size_t tmp = len;
    do { digits++; tmp /= 10; } while (tmp > 0);
    if (len == 0) digits = 1;  // "0" case

    // header length: type_str + space + digits + null terminator
    size_t header_len = strlen(type_str) + 1 + digits + 1;
    char *header = malloc(header_len);
    if (!header) return -1;
    snprintf(header, header_len, "%s %zu", type_str, len);
    // snprintf does not include the trailing null in the count? Actually it writes null.
    // But we need the header to be exactly "type size\0". The string ends with null.
    // So header_len is correct.

    // 2. Full object = header + data
    size_t full_len = header_len + len;
    void *full = malloc(full_len);
    if (!full) {
        free(header);
        return -1;
    }
    memcpy(full, header, header_len);
    memcpy((char*)full + header_len, data, len);
    free(header);

    // Compute hash of full object
    ObjectID id;
    compute_hash(full, full_len, &id);

    // 3. Check deduplication
    if (object_exists(&id)) {
        // Already exists, just copy hash and return success
        if (id_out) memcpy(id_out, &id, sizeof(ObjectID));
        free(full);
        return 0;
    }

    // 4. Create shard directory
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(&id, hex);
    char shard_path[512];
    snprintf(shard_path, sizeof(shard_path), "%s/%.2s", OBJECTS_DIR, hex);
    mkdir(shard_path, 0755);  // ignore error if already exists

    // 5. Prepare final path and temp path
    char final_path[512];
    object_path(&id, final_path, sizeof(final_path));

    char temp_path[1024];  // larger buffer
    snprintf(temp_path, sizeof(temp_path), "%s/tmp_XXXXXX", shard_path);
    int fd = mkstemp(temp_path);
    if (fd == -1) {
        free(full);
        return -1;
    }

    // Write full object to temp file
    ssize_t written = write(fd, full, full_len);
    if (written != (ssize_t)full_len) {
        close(fd);
        unlink(temp_path);
        free(full);
        return -1;
    }

    // 6. fsync the temp file
    if (fsync(fd) != 0) {
        close(fd);
        unlink(temp_path);
        free(full);
        return -1;
    }
    close(fd);

    // 7. rename temp to final (atomic)
    if (rename(temp_path, final_path) != 0) {
        unlink(temp_path);
        free(full);
        return -1;
    }

    // 8. fsync the shard directory to persist rename
    int dir_fd = open(shard_path, O_RDONLY);
    if (dir_fd != -1) {
        fsync(dir_fd);
        close(dir_fd);
    }

    free(full);

    // 9. Store hash
    if (id_out) memcpy(id_out, &id, sizeof(ObjectID));
    return 0;
}

// Read an object from the store.
//
// Steps:
//   1. Build the file path from the hash using object_path()
//   2. Open and read the entire file
//   3. Parse the header to extract the type string and size
//   4. Verify integrity: recompute the SHA-256 of the file contents
//      and compare to the expected hash (from *id). Return -1 if mismatch.
//   5. Set *type_out to the parsed ObjectType
//   6. Allocate a buffer, copy the data portion (after the \0), set *data_out and *len_out
//
// The caller is responsible for calling free(*data_out).
// Returns 0 on success, -1 on error (file not found, corrupt, etc.).
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    // 1. Get file path
    char path[512];
    object_path(id, path, sizeof(path));

    // 2. Open and read entire file
    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // Get file size
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (file_size < 0) {
        fclose(f);
        return -1;
    }

    void *file_data = malloc(file_size);
    if (!file_data) {
        fclose(f);
        return -1;
    }
    size_t bytes_read = fread(file_data, 1, file_size, f);
    fclose(f);
    if (bytes_read != (size_t)file_size) {
        free(file_data);
        return -1;
    }

    // 3. Parse header: find the first '\0' byte
    const char *data_ptr = (const char*)file_data;
    const void *null_pos = memchr(data_ptr, '\0', file_size);
    if (!null_pos) {
        free(file_data);
        return -1;
    }
    size_t header_len = (const char*)null_pos - data_ptr + 1; // include null
    // The header string is from data_ptr to null_pos (excluding null? Actually we have null at end)
    // We'll copy header string (without null) for parsing
    char *header_str = malloc(header_len); // including null
    if (!header_str) {
        free(file_data);
        return -1;
    }
    memcpy(header_str, data_ptr, header_len);
    header_str[header_len - 1] = '\0'; // ensure null termination

    // Parse type and size from header_str which is "type size"
    char type_buf[32];
    size_t parsed_len;
    if (sscanf(header_str, "%31s %zu", type_buf, &parsed_len) != 2) {
        free(header_str);
        free(file_data);
        return -1;
    }
    free(header_str);

    // Verify that parsed_len matches the data length
    size_t actual_data_len = file_size - header_len;
    if (parsed_len != actual_data_len) {
        free(file_data);
        return -1;
    }

    // 4. Verify integrity: recompute hash of entire file content
    ObjectID computed_id;
    compute_hash(file_data, file_size, &computed_id);
    if (memcmp(computed_id.hash, id->hash, HASH_SIZE) != 0) {
        free(file_data);
        return -1;
    }

    // 5. Set type_out
    if (parse_type_string(type_buf, type_out) != 0) {
        free(file_data);
        return -1;
    }

    // 6. Extract data portion (after the null)
    void *data = malloc(actual_data_len);
    if (!data) {
        free(file_data);
        return -1;
    }
    memcpy(data, (char*)file_data + header_len, actual_data_len);
    free(file_data);

    *data_out = data;
    *len_out = actual_data_len;
    return 0;
}
