// tree.c — Tree object serialization and construction
//
// PROVIDED functions: get_file_mode, tree_parse, tree_serialize
// TODO functions:     tree_from_index
//
// Binary tree format (per entry, concatenated with no separators):
//   "<mode-as-ascii-octal> <name>\0<32-byte-binary-hash>"
//
// Example single entry (conceptual):
//   "100644 hello.txt\0" followed by 32 raw bytes of SHA-256

#include "tree.h"
#include "index.h"   // for Index, IndexEntry, index_load
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>

// Forward declaration for object_write (defined in object.c)
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── Mode Constants ─────────────────────────────────────────────────────────

#define MODE_FILE      0100644
#define MODE_EXEC      0100755
#define MODE_DIR       0040000

// ─── PROVIDED ───────────────────────────────────────────────────────────────

// Determine the object mode for a filesystem path.
uint32_t get_file_mode(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return 0;

    if (S_ISDIR(st.st_mode))  return MODE_DIR;
    if (st.st_mode & S_IXUSR) return MODE_EXEC;
    return MODE_FILE;
}

// Parse binary tree data into a Tree struct safely.
// Returns 0 on success, -1 on parse error.
int tree_parse(const void *data, size_t len, Tree *tree_out) {
    tree_out->count = 0;
    const uint8_t *ptr = (const uint8_t *)data;
    const uint8_t *end = ptr + len;

    while (ptr < end && tree_out->count < MAX_TREE_ENTRIES) {
        TreeEntry *entry = &tree_out->entries[tree_out->count];

        // 1. Safely find the space character for the mode
        const uint8_t *space = memchr(ptr, ' ', end - ptr);
        if (!space) return -1; // Malformed data

        // Parse mode into an isolated buffer
        char mode_str[16] = {0};
        size_t mode_len = space - ptr;
        if (mode_len >= sizeof(mode_str)) return -1;
        memcpy(mode_str, ptr, mode_len);
        entry->mode = strtol(mode_str, NULL, 8);

        ptr = space + 1; // Skip space

        // 2. Safely find the null terminator for the name
        const uint8_t *null_byte = memchr(ptr, '\0', end - ptr);
        if (!null_byte) return -1; // Malformed data

        size_t name_len = null_byte - ptr;
        if (name_len >= sizeof(entry->name)) return -1;
        memcpy(entry->name, ptr, name_len);
        entry->name[name_len] = '\0'; // Ensure null-terminated

        ptr = null_byte + 1; // Skip null byte

        // 3. Read the 32-byte binary hash
        if (ptr + HASH_SIZE > end) return -1; 
        memcpy(entry->hash.hash, ptr, HASH_SIZE);
        ptr += HASH_SIZE;

        tree_out->count++;
    }
    return 0;
}

// Helper for qsort to ensure consistent tree hashing
static int compare_tree_entries(const void *a, const void *b) {
    return strcmp(((const TreeEntry *)a)->name, ((const TreeEntry *)b)->name);
}

// Serialize a Tree struct into binary format for storage.
// Caller must free(*data_out).
// Returns 0 on success, -1 on error.
int tree_serialize(const Tree *tree, void **data_out, size_t *len_out) {
    // Estimate max size: (6 bytes mode + 1 byte space + 256 bytes name + 1 byte null + 32 bytes hash) per entry
    size_t max_size = tree->count * 296; 
    uint8_t *buffer = malloc(max_size);
    if (!buffer) return -1;

    // Create a mutable copy to sort entries (Git requirement)
    Tree sorted_tree = *tree;
    qsort(sorted_tree.entries, sorted_tree.count, sizeof(TreeEntry), compare_tree_entries);

    size_t offset = 0;
    for (int i = 0; i < sorted_tree.count; i++) {
        const TreeEntry *entry = &sorted_tree.entries[i];
        
        // Write mode and name (%o writes octal correctly for Git standards)
        int written = sprintf((char *)buffer + offset, "%o %s", entry->mode, entry->name);
        offset += written + 1; // +1 to step over the null terminator written by sprintf
        
        // Write binary hash
        memcpy(buffer + offset, entry->hash.hash, HASH_SIZE);
        offset += HASH_SIZE;
    }

    *data_out = buffer;
    *len_out = offset;
    return 0;
}

// ─── TODO: Implement these ──────────────────────────────────────────────────

// Helper function to compare two index entries by path (for sorting)
static int compare_index_entries_by_path(const void *a, const void *b) {
    const IndexEntry *ea = (const IndexEntry *)a;
    const IndexEntry *eb = (const IndexEntry *)b;
    return strcmp(ea->path, eb->path);
}

// Recursive helper: build a tree object for the directory represented by
// the slice of index entries from start (inclusive) to end (exclusive).
// All entries in this slice have paths that start with `base_path`.
// base_path is the directory path (relative to repo root) that this tree represents.
// It may be an empty string for the root directory.
// Returns 0 and sets `tree_hash_out` on success, -1 on error.
static int build_tree_from_index_entries(const IndexEntry *entries, int start, int end,
                                          const char *base_path, ObjectID *tree_hash_out) {
    if (start >= end) {
        // Empty directory: create an empty tree object
        Tree empty_tree = { .count = 0 };
        void *data;
        size_t len;
        if (tree_serialize(&empty_tree, &data, &len) != 0)
            return -1;
        int ret = object_write(OBJ_TREE, data, len, tree_hash_out);
        free(data);
        return ret;
    }

    // We'll collect TreeEntry objects for this directory.
    Tree tree = { .count = 0 };
    size_t base_len = strlen(base_path);

    int i = start;
    while (i < end) {
        const IndexEntry *entry = &entries[i];
        const char *rel_path = entry->path + base_len; // skip base_path
        // Find the next slash after base_path
        const char *slash = strchr(rel_path, '/');
        if (slash == NULL) {
            // This entry is a file directly inside this directory.
            TreeEntry *te = &tree.entries[tree.count];
            te->mode = entry->mode;
            memcpy(&te->hash, &entry->hash, sizeof(ObjectID));
            snprintf(te->name, sizeof(te->name), "%s", rel_path);
            tree.count++;
            i++;
        } else {
            // This entry belongs to a subdirectory. Find the subdirectory name.
            size_t subdir_len = slash - rel_path;
            char subdir_name[256];
            if (subdir_len >= sizeof(subdir_name)) return -1;
            memcpy(subdir_name, rel_path, subdir_len);
            subdir_name[subdir_len] = '\0';

            // Find the range of entries that belong to this subdirectory.
            // All entries with the same prefix "base_path/subdir_name/"
            char subdir_prefix[512];
            snprintf(subdir_prefix, sizeof(subdir_prefix), "%s%s/", base_path, subdir_name);
            int sub_start = i;
            int sub_end = i;
            while (sub_end < end) {
                const char *next_path = entries[sub_end].path;
                if (strncmp(next_path, subdir_prefix, strlen(subdir_prefix)) == 0)
                    sub_end++;
                else
                    break;
            }

            // Recursively build the tree for this subdirectory.
            ObjectID sub_tree_hash;
            if (build_tree_from_index_entries(entries, sub_start, sub_end, subdir_prefix, &sub_tree_hash) != 0)
                return -1;

            // Add an entry for the subdirectory in the current tree.
            TreeEntry *te = &tree.entries[tree.count];
            te->mode = MODE_DIR;
            memcpy(&te->hash, &sub_tree_hash, sizeof(ObjectID));
            snprintf(te->name, sizeof(te->name), "%s", subdir_name);
            tree.count++;

            i = sub_end; // move to next group
        }
    }

    // Serialize the tree and write it to the object store.
    void *data;
    size_t len;
    if (tree_serialize(&tree, &data, &len) != 0)
        return -1;
    int ret = object_write(OBJ_TREE, data, len, tree_hash_out);
    free(data);
    return ret;
}

// Build a tree hierarchy from the current index and write all tree
// objects to the object store.
//
// HINTS - Useful functions and concepts for this phase:
//   - index_load      : load the staged files into memory
//   - strchr          : find the first '/' in a path to separate directories from files
//   - strncmp         : compare prefixes to group files belonging to the same subdirectory
//   - Recursion       : you will likely want to create a recursive helper function 
//                       (e.g., `write_tree_level(entries, count, depth)`) to handle nested dirs.
//   - tree_serialize  : convert your populated Tree struct into a binary buffer
//   - object_write    : save that binary buffer to the store as OBJ_TREE
//
// Returns 0 on success, -1 on error.
int tree_from_index(ObjectID *id_out) {
    // Load the index
    Index index;
    if (index_load(&index) != 0) {
        // If index doesn't exist, treat as empty (no staged files)
        // Create an empty tree object
        Tree empty_tree = { .count = 0 };
        void *data;
        size_t len;
        if (tree_serialize(&empty_tree, &data, &len) != 0)
            return -1;
        int ret = object_write(OBJ_TREE, data, len, id_out);
        free(data);
        return ret;
    }

    if (index.count == 0) {
        // No entries: empty tree
        Tree empty_tree = { .count = 0 };
        void *data;
        size_t len;
        if (tree_serialize(&empty_tree, &data, &len) != 0)
            return -1;
        int ret = object_write(OBJ_TREE, data, len, id_out);
        free(data);
        return ret;
    }

    // Sort index entries by path (necessary for grouping)
    qsort(index.entries, index.count, sizeof(IndexEntry), compare_index_entries_by_path);

    // Build the root tree (base_path = "")
    int ret = build_tree_from_index_entries(index.entries, 0, index.count, "", id_out);

    return ret;
}
