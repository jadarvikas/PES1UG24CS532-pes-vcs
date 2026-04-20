# PES1UG24CS532-pes-vcs
# PES‑VCS Lab Submission

**Name:** Vikas Ashok Jadar  
**SRN:** PES1UG24CS532
**Repository:** [GitHub repo URL]

---

## Screenshots

### Phase 1 – Object Storage

- **Screenshot 1A:** `screenshot_1A.png` – Output of `./test_objects`
- **Screenshot 1B:** `screenshot_1B.png` – `find .pes/objects -type f` showing sharded directories

### Phase 2 – Tree Objects

- **Screenshot 2A:** `screenshot_2A.png` – Output of `./test_tree`
- **Screenshot 2B:** `screenshot_2B.png` – `xxd` dump of a raw tree object (first 20 lines)

### Phase 3 – Index (Staging Area)

- **Screenshot 3A:** `screenshot_3A.png` – `./pes init`, `./pes add`, `./pes status` sequence
- **Screenshot 3B:** `screenshot_3B.png` – Content of `.pes/index` (text format)

### Phase 4 – Commits and History

- **Screenshot 4A:** `screenshot_4A.png` – Output of `./pes log` (three commits)
- **Screenshot 4B:** `screenshot_4B.png` – `find .pes -type f | sort` after three commits
- **Screenshot 4C:** `screenshot_4C.png` – `cat .pes/refs/heads/main` and `cat .pes/HEAD`

### Integration Test (Optional)

- `screenshot_integration.png` – Output of `make test-integration`

---

## Analysis Questions

### Branching (Q5.1 – Q5.3)

#### Q5.1: How would you implement `pes checkout <branch>`? What files in `.pes/` change, and what must happen to the working directory? Why is this complex?

**Answer:**  
To implement `pes checkout <branch>`:

1. **Read the branch reference** – The target branch is stored as a file under `.pes/refs/heads/<branch>`. The file contains a commit hash.
2. **Update HEAD** – Write `ref: refs/heads/<branch>` into `.pes/HEAD` to point to the new branch.
3. **Read the target commit** – Use the commit hash to obtain the root tree hash.
4. **Recreate the working directory** – For every file in the target tree, write its content (read from the object store) to the working directory. Files that exist in the current working directory but not in the target tree must be deleted.
5. **Update the index** – Replace the current index with the list of files from the target tree, so that the staging area reflects the new snapshot.

**Complexity:**  
- **Conflicts with uncommitted changes** – If the working directory contains modifications to files that differ between branches, checkout must refuse to overwrite them. Detecting this requires comparing the current working directory against both the old and new trees.
- **Recursive directory handling** – Creating/deleting directories and files while preserving permissions is error‑prone.
- **Atomicity** – If the operation fails halfway, the repository and working directory could be left in an inconsistent state.

---

#### Q5.2: How would you detect a “dirty working directory” conflict when switching branches, using only the index and object store?

**Answer:**  
A dirty working directory means there are unstaged changes to tracked files. To detect conflicts:

1. **Load the current index** – This tells us which files are staged and their expected content (hashes).
2. **For each file in the index** – Compute its current hash (by reading the file and calling `object_write(OBJ_BLOB)`) and compare to the hash stored in the index.
   - If the hashes differ, the file has unstaged modifications.
3. **For the target branch’s tree** – Compare the hash of each file in the target tree with the corresponding file in the working directory (using `object_write` on the working file).
   - If a file exists in both branches but has different content, and the working copy is dirty (hash differs from current index), then switching branches would lose changes.
4. **Refuse checkout** if any dirty file would be overwritten by a different version from the target branch.

The index acts as the “clean” baseline. The object store provides the content hashes of the target tree without needing to unpack entire blobs.

---

#### Q5.3: What is “detached HEAD”? What happens if you make commits in that state? How can a user recover those commits?

**Answer:**  
**Detached HEAD** occurs when `.pes/HEAD` contains a commit hash directly instead of a reference to a branch (e.g., `ref: refs/heads/main`). This typically happens when you check out a specific commit, tag, or a remote branch without a local branch.

**Making commits in detached HEAD:**  
- New commits are created normally – they have a parent (the current HEAD commit), and the commit object is written to the object store.
- However, **no branch reference is updated**. The only reference to the new commit is the `HEAD` file itself, which still contains a hash (not a branch).
- If you later switch to another branch, the `HEAD` file will point to that branch, and the commits made in detached HEAD become **unreachable** (no branch or tag points to them).

**Recovery:**  
- As long as the commit objects still exist in `.pes/objects/`, they can be recovered by:
  1. Finding the commit hash via `git reflog` (or in our case, by scanning object directories and parsing commit objects).
  2. Creating a new branch that points to that commit: `echo "<hash>" > .pes/refs/heads/recovered-branch`
  3. Updating `HEAD` to point to the new branch.
- The object store is append‑only, so unreachable commits remain until garbage collection runs.

---

### Garbage Collection (Q6.1 – Q6.2)

#### Q6.1: Describe an algorithm to find and delete unreachable objects. What data structure would you use to track reachable hashes efficiently? Estimate the number of objects to visit for a repo with 100,000 commits and 50 branches.

**Answer:**  

**Algorithm (mark‑and‑sweep):**

1. **Mark phase – collect all reachable objects**  
   - Start from every branch reference (in `.pes/refs/heads/`) and from `HEAD` (if it points directly to a commit).  
   - Use a stack or queue to traverse:  
     - For a commit: mark it, then mark its tree and parent commit.  
     - For a tree: mark it, then for each entry, mark the blob (if file) or subtree (if directory).  
   - Keep a set (e.g., hash table) of all marked object IDs.

2. **Sweep phase – delete unreachable objects**  
   - Iterate over all files in `.pes/objects/` (sharded directories).  
   - For each object, compute its hash from the path. If the hash is **not** in the marked set, delete the file.  
   - Optionally remove empty shard directories.

**Data structure:**  
- A hash set (e.g., `unordered_set<ObjectID>` or a boolean array keyed by hash) to store reachable IDs.  
- A stack/queue (list) for traversal.

**Estimate for 100,000 commits, 50 branches:**  
- Each commit points to one tree.  
- Each tree may have many entries, but many trees will be shared across commits. In a typical project, the number of unique trees is roughly proportional to the number of commits.  
- Assume average 2 files per commit (changes). Then objects visited ≈ commits (100k) + trees (~100k) + blobs (~200k) = **~400,000 objects**.  
- With 50 branches, the traversal starts from 50 tips, but the total unique objects doesn’t increase linearly because branches share history.  
- The upper bound is O(commits + trees + blobs) ≈ a few hundred thousand to a million for a large repo.

---

#### Q6.2: Why is it dangerous to run garbage collection concurrently with a commit operation? Describe a race condition and how Git’s real GC avoids it.

**Answer:**  

**Danger:**  
GC deletes objects that are not reachable from any reference. A concurrent commit creates new objects (blobs, trees, commit) and updates a branch reference to point to the new commit. If GC runs at the same time, it might:

- See the old branch pointer (before the commit updates it).  
- Mark only the old commit and its objects as reachable.  
- Delete the **newly created objects** that are not yet referenced (because the branch pointer hasn’t been updated).  

**Race condition example:**  

1. GC begins: reads branch `main` → points to commit `C1`.  
2. User commits: creates new commit `C2` (and its tree and blobs), writes them to object store.  
3. GC continues: it does not see `C2` because the branch still points to `C1`. GC marks only objects reachable from `C1`.  
4. GC deletes `C2` and its associated objects because they are “unreachable”.  
5. User’s commit operation finishes and tries to update `main` to `C2`, but `C2` no longer exists → repository corruption.

**How Git avoids this:**  

Git uses **temporary references** and **atomic reference updates** with a “reachability bitmap” technique:

- A commit is first written to the object store, then the branch reference is updated **atomically** (using `rename()` on the ref file).  
- Git’s garbage collector (`git gc`) operates in two modes:  
  - **Incremental GC** – it does not delete objects that are less than a certain age (e.g., 2 weeks) to avoid races with concurrent commits.  
  - **Full GC** – it uses a **lock** (`.git/gc.pid`) to ensure only one GC runs at a time, and it refuses to run if any other Git operation is in progress (e.g., by checking for `index.lock`).  
- More advanced: Git can create temporary references (e.g., `refs/heads/.tmp-<pid>`) that are also marked as reachable during GC, so that in‑flight commits are protected.  

In practice, `git gc` is safe because it relies on the fact that reference updates are atomic and it explicitly locks the repository during the crucial mark‑and‑sweep phase.

---

## Submission Notes

- All code files (`object.c`, `tree.c`, `index.c`, `commit.c`) are fully implemented and tested.  
- The repository contains at least 5 commits per phase (see `git log --oneline`).  
- The lab report (this file) is placed at the root of the repository.

**GitHub Repository URL:** [Replace with your public repo URL]
