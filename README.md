# StrataDB

Slavi Sotirov

COS 4091A - Senior Project (AUBG 2026 Spring Semester) 

##Description of project:
Lightweight Relational Database engine implemented in C++.

##Scope of project:
- Page-based storage with fixed-size disk pages.
- Disk-based B+ tree storage/indexing.
- Minimal SQL subset: CREATE, INSERT, SELECT by primary key, DELETE by primary key

##Current Out of Scope:
- Joins
- Concurrency Control
- Transactions

##Build for MacOS

```shell
mkdir -p build
cd build
cmake ..
cmake --build .
./stratadb 
```
## Commit Naming - for better clarity

This project is going to use the following commit prefixes: 
- `wip:` - working on file
- `done:` - finished code
- `fix:` - bug fixed
- `rework:` - chaning an approach to a code
- `docs:` - updated documentation or Read.me
