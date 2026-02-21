# StrataDB

Slavi Sotirov

COS 4091A - Senior Project (AUBG 2026 Spring Semester) 

## Description of project:

Lightweight Relational Database engine implemented in C++.

---

## Scope of project:

- Page-based storage with fixed-size disk pages.
- Disk-based B+ tree storage/indexing.
- Minimal SQL subset: CREATE, INSERT, SELECT by primary key, DELETE by primary key

---

## Current Out of Scope:

- Joins
- Concurrency Control
- Transactions

---

## Architecture

**Currently Implemented**

### **Layer 1: Storage Layer** 

- **Component** 'DiskManager'
- **Location** 'include/storage/disk_manager.h' , 'src/storage/disk_manager.cpp'

**Responsibilities**
- Management of page-based disk I/O (fixed 4096-byte pages)
- Abstract file operations from upper layers
- Provide persistence for database data

**Key Operations**
- 'allocate_page()' - Allocates a new zero-filled page
- 'read_page(page_id, buffer)' - Reads a page from disk
- 'write_page(page_id, buffer)' - Write page to disk
- 'num_pages()' - Query total page count 

**Important Design Choices**
- **Two-phase file openings** - Handles new and existing databases
- **Cached page count** - Avoids repeated disk seeks for better performance
- **Boundary Validation** - Prevents the reading of nonexistent pages or the creation of gaps
- **Non-copyable** - Only one manager per file to prevent corruption

---

### **Upcoming Layers** (Future Implementation)

- **Layer 2: Index LAyer (B+ Tree)**
- **Layer 3: Query Processing**
- **Layer 4: Execution**

**(Storage Layer --> B+ Tree Index --> Query Parser --> SQL Executor)**

---

### Testing

**Test Suite:** 'src/main.cpp'

**Current Coverage**
- File creation and initialization
- Page allocation and zero-filling
- Read/write operations
- Boundary validation (out-of-range errors)
- Persistence across database reopening

**Results:** All tests pass

---

## Build for MacOS

```shell
mkdir -p build
cd build
cmake ..
cmake --build .
./stratadb 
```

---

## Commit Naming - for better clarity

This project is going to use the following commit prefixes: 
- `wip:` - working on file
- `done:` - finished code
- `fix:` - bug fixed
- `rework:` - chaning an approach to a code
- `docs:` - updated documentation or Read.me
- `update` - just a general update



