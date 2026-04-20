// index.c — Staging area implementation
//
// Text format of .pes/index (one entry per line, sorted by path):
//
//   <mode-octal> <64-char-hex-hash> <mtime-seconds> <size> <path>
//
// Example:
//   100644 a1b2c3d4e5f6...  1699900000 42 README.md
//   100644 f7e8d9c0b1a2...  1699900100 128 src/main.c
//
// This is intentionally a simple text format. No magic numbers, no
// binary parsing. The focus is on the staging area CONCEPT (tracking
// what will go into the next commit) and ATOMIC WRITES (temp+rename).
//
// PROVIDED functions: index_find, index_remove, index_status
// TODO functions:     index_load, index_save, index_add

#include "index.h"
#include "tree.h"      // for get_file_mode
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <time.h>

// Forward declaration for object_write (defined in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

// Find an index entry by path (linear scan).
IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

// Remove a file from the index.
// Returns 0 on success, -1 if path not in index.
int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    fprintf(stderr, "error: '%s' is not in the index\n", path);
    return -1;
}

// Print the status of the working directory.
int index_status(const Index *index) {
    printf("Staged changes:\n");
    int staged_count = 0;
    for (int i = 0; i < index->count; i++) {
        printf("  staged:     %s\n", index->entries[i].path);
        staged_count++;
    }
    if (staged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n");
    int unstaged_count = 0;
    for (int i = 0; i < index->count; i++) {
        struct stat st;
        if (stat(index->entries[i].path, &st) != 0) {
            printf("  deleted:    %s\n", index->entries[i].path);
            unstaged_count++;
        } else {
            if (st.st_mtime != (time_t)index->entries[i].mtime_sec || st.st_size != (off_t)index->entries[i].size) {
                printf("  modified:   %s\n", index->entries[i].path);
                unstaged_count++;
            }
        }
    }
    if (unstaged_count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Untracked files:\n");
    int untracked_count = 0;
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
            if (strcmp(ent->d_name, ".pes") == 0) continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;

            int is_tracked = 0;
            for (int i = 0; i < index->count; i++) {
                if (strcmp(index->entries[i].path, ent->d_name) == 0) {
                    is_tracked = 1;
                    break;
                }
            }
            if (!is_tracked) {
                struct stat st;
                stat(ent->d_name, &st);
                if (S_ISREG(st.st_mode)) {
                    printf("  untracked:  %s\n", ent->d_name);
                    untracked_count++;
                }
            }
        }
        closedir(dir);
    }
    if (untracked_count == 0) printf("  (nothing to show)\n");
    printf("\n");
    return 0;
}

// ─── TODO: Implement these ───────────────────────────────────────────────────

static int compare_index_entries(const void *a, const void *b) {
    const IndexEntry *ea = *(const IndexEntry **)a;
    const IndexEntry *eb = *(const IndexEntry **)b;
    return strcmp(ea->path, eb->path);
}

// Load the index from .pes/index.
int index_load(Index *index) {
    if (!index) return -1;
    index->count = 0;

    FILE *f = fopen(INDEX_FILE, "r");
    if (!f) return 0;  // no index file -> empty index

    char line[1024];
    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        if (len > 0 && line[len-1] == '\n')
            line[len-1] = '\0';

        unsigned int mode;
        char hash_hex[HASH_HEX_SIZE + 1];
        long mtime, size;
        char path[PATH_MAX];

        if (sscanf(line, "%o %64s %ld %ld %[^\n]",
                   &mode, hash_hex, &mtime, &size, path) != 5) {
            fclose(f);
            return -1;
        }
        if (index->count >= MAX_INDEX_ENTRIES) {
            fclose(f);
            return -1;
        }
        IndexEntry *entry = &index->entries[index->count];
        entry->mode = mode;
        if (hex_to_hash(hash_hex, &entry->hash) != 0) {
            fclose(f);
            return -1;
        }
        entry->mtime_sec = mtime;
        entry->size = size;
        // Safe path copy
        if (strlen(path) >= sizeof(entry->path)) {
            fclose(f);
            return -1;
        }
        strcpy(entry->path, path);
        index->count++;
    }
    fclose(f);
    return 0;
}

// Save the index atomically.
int index_save(const Index *index) {
    if (!index) return -1;

    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/index.tmp", PES_DIR);

    FILE *f = fopen(temp_path, "w");
    if (!f) return -1;

    const IndexEntry *sorted_entries[MAX_INDEX_ENTRIES];
    for (int i = 0; i < index->count; i++)
        sorted_entries[i] = &index->entries[i];
    qsort(sorted_entries, index->count, sizeof(IndexEntry *), compare_index_entries);

    for (int i = 0; i < index->count; i++) {
        const IndexEntry *entry = sorted_entries[i];
        char hash_hex[HASH_HEX_SIZE + 1];
        hash_to_hex(&entry->hash, hash_hex);
        fprintf(f, "%o %s %ld %ld %s\n",
                entry->mode, hash_hex,
                (long)entry->mtime_sec, (long)entry->size,
                entry->path);
    }

    fflush(f);
    int fd = fileno(f);
    if (fd == -1 || fsync(fd) != 0) {
        fclose(f);
        unlink(temp_path);
        return -1;
    }
    fclose(f);

    if (rename(temp_path, INDEX_FILE) != 0) {
        unlink(temp_path);
        return -1;
    }

    int dir_fd = open(PES_DIR, O_RDONLY);
    if (dir_fd != -1) {
        fsync(dir_fd);
        close(dir_fd);
    }
    return 0;
}

// Stage a file.
int index_add(Index *index, const char *path) {
    if (!index || !path) return -1;

    struct stat st;
    if (stat(path, &st) != 0) {
        fprintf(stderr, "error: cannot stat '%s'\n", path);
        return -1;
    }
    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "error: '%s' is not a regular file\n", path);
        return -1;
    }

    FILE *f = fopen(path, "rb");
    if (!f) {
        fprintf(stderr, "error: cannot open '%s'\n", path);
        return -1;
    }
    fseek(f, 0, SEEK_END);
    long file_size = ftell(f);
    fseek(f, 0, SEEK_SET);
    void *data = malloc(file_size);
    if (!data) {
        fclose(f);
        return -1;
    }
    size_t bytes_read = fread(data, 1, file_size, f);
    fclose(f);
    if (bytes_read != (size_t)file_size) {
        free(data);
        return -1;
    }

    ObjectID blob_hash;
    if (object_write(OBJ_BLOB, data, file_size, &blob_hash) != 0) {
        free(data);
        return -1;
    }
    free(data);

    uint32_t mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;

    IndexEntry *existing = index_find(index, path);
    if (existing) {
        existing->mode = mode;
        existing->hash = blob_hash;
        existing->mtime_sec = st.st_mtime;
        existing->size = st.st_size;
    } else {
        if (index->count >= MAX_INDEX_ENTRIES) {
            fprintf(stderr, "error: index is full\n");
            return -1;
        }
        IndexEntry *new_entry = &index->entries[index->count];
        new_entry->mode = mode;
        new_entry->hash = blob_hash;
        new_entry->mtime_sec = st.st_mtime;
        new_entry->size = st.st_size;
        if (strlen(path) >= sizeof(new_entry->path)) {
            fprintf(stderr, "error: path too long\n");
            return -1;
        }
        strcpy(new_entry->path, path);
        index->count++;
    }
    return index_save(index);
}
