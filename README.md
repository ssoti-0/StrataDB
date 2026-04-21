# StrataDB

**Author:** Slavi Sotirov
**Course:** COS 4091A — Senior Project (AUBG, Spring 2026)

## Description

A from-scratch disk-based relational database engine implemented in C++20. StrataDB implements all core layers of a database management system: page-based storage, B+ tree indexing with full rebalancing, recursive-descent SQL parsing, and query execution with cross-table joins.

---

## Features

- Page-based storage with fixed-size 4096-byte disk pages
- Self-balancing B+ tree index with search, insertion (with node splitting), and deletion (with redistribution, merging, and root shrinking)
- Minimal SQL subset: CREATE TABLE, INSERT, SELECT (by primary key and full table scan), DELETE
- Cross-table JOIN support (nested-loop equi-join)
- INT and TEXT column types
- Schema persistence — tables survive application restarts
- Built-in diagnostic commands: STATS, VERBOSE ON/OFF, BENCHMARK
- Interactive SQL shell

---

## Architecture

```
┌─────────────────────────────────────────┐
│     Execution Layer                     │
├─────────────────────────────────────────┤
│     Query Processing Layer              │
├─────────────────────────────────────────┤
│     Indexing Layer (B+ Tree)            │
├─────────────────────────────────────────┤
│     Storage Layer (DiskManager)         │
└─────────────────────────────────────────┘
```

### Layer 1: Storage Layer

- **Component:** `DiskManager`
- **Location:** `src/storage/disk_manager.h`, `src/storage/disk_manager.cpp`
- Manages page-based disk I/O (fixed 4096-byte pages)
- Two-phase file opening handles both new and existing databases
- Tracks cumulative read/write counts for diagnostics
- Non-copyable — one manager per file to prevent corruption

### Layer 2: Indexing Layer (B+ Tree)

- **Components:** `BPlusTree`, `Node`, `LeafNode`, `InternalNode`
- **Location:** `src/index/btree.h`, `src/index/btree.cpp`, `src/index/node.h`, `src/index/node.cpp`
- Self-balancing B+ tree (ORDER=4) with O(log n) search, insert, and delete
- Node splitting on overflow, redistribution and merging on underflow, root shrinking
- Leaf chain for efficient sequential scans
- Polymorphic node hierarchy (abstract Node base class with LeafNode and InternalNode)

### Layer 3: Query Processing Layer

- **Components:** `Parser`, `tokenize()`, AST types
- **Location:** `src/query/parser.h`, `src/query/parser.cpp`, `src/query/ast.h`
- Hand-written recursive-descent parser with case-insensitive keyword matching
- Tokenizer handles keywords, identifiers, integers, single-quoted strings, and symbols
- Produces `std::variant`-based AST (9 statement types)

### Layer 4: Execution Layer

- **Components:** `Executor`, `Schema`, `TableInfo`
- **Location:** `src/execution/executor.h`, `src/execution/executor.cpp`
- Dispatches parsed statements to the appropriate table's B+ tree
- Manages table lifecycle, schema persistence, and automatic table loading on startup
- Verbose trace mode, per-table statistics, and built-in benchmarking

---

## Supported SQL

```sql
CREATE TABLE students (id INT PRIMARY KEY, name TEXT);
INSERT INTO students VALUES (1, 'Alice');
SELECT * FROM students;
SELECT * FROM students WHERE id = 1;
DELETE FROM students WHERE id = 1;
SELECT * FROM students JOIN grades ON students.id = grades.sid;
```

### Diagnostic Commands

- `STATS` — per-table I/O counters and B+ tree operation counts
- `VERBOSE ON` / `VERBOSE OFF` — step-by-step engine trace for each command
- `BENCHMARK` — timed insert/search/scan/delete at 25, 100, and 1000 keys

---

## Project Structure

```
StrataDB/
├── src/
│   ├── execution/
│   │   ├── executor.cpp
│   │   └── executor.h
│   ├── index/
│   │   ├── btree.cpp
│   │   ├── btree.h
│   │   ├── node.cpp
│   │   └── node.h
│   ├── query/
│   │   ├── ast.h
│   │   ├── parser.cpp
│   │   └── parser.h
│   ├── storage/
│   │   ├── disk_manager.cpp
│   │   └── disk_manager.h
│   └── main.cpp
├── tests/
│   └── tests.cpp
├── testdata/       (created at runtime for test fixtures)
├── data/           (created at runtime for live databases)
├── build/          (CMake output)
└── CMakeLists.txt
```

---

## Testing

**Test Suite:** `tests/tests.cpp`

**Coverage:**

- **Storage layer** — page I/O, allocation, boundary validation, zero-filling
- **Node operations** — sorted insertion, serialization round-trips, child index lookup, key and value access
- **B+ Tree** — search, insert, cascading splits (25 keys), delete, full rebalancing (forward, reverse, partial delete with re-insert, delete to empty)
- **Parser** — statement parsing (CREATE, INSERT, SELECT, DELETE), case insensitivity, error handling for unknown and incomplete statements
- **Executor** — full CRUD workflow (CREATE, INSERT, SELECT, DELETE)
- **Join** — cross-table inner join with string values

**Results:** 55 passed, 0 failed

---

## Build (macOS / Linux)

```bash
mkdir -p build && cd build
cmake ..
cmake --build .
```

**Run the interactive shell:**

```bash
cd ..
./build/stratadb
```

**Run the test suite:**

```bash
./build/stratadb_tests
```

---

## Known Limitations

StrataDB deliberately does not implement:

- Concurrency control
- Transactions / ACID guarantees
- Query optimization
- Secondary indexes
- Write-ahead logging / crash recovery
- Network protocol

These are intentional scope decisions for a teaching-focused implementation.

---

## Dependencies

None. The entire project uses only the C++ standard library.

---

## Commit Naming

- `wip:` — working on file
- `done:` — finished code
- `fix:` — bug fixed
- `rework:` — changing an approach
- `docs:` — updated documentation or README
- `update:` — general update