# B+ Tree Design

## Why B+ Tree?

It is a standard indexing structure in databases (MySQL, SQLite, PostgreSQL)

- **O(log n)** search, insert, delete
- **Very disk-friendly** - each node = one page
- **Range queries** - linked leaf nodes
- **Self-balancing** - it maintains height automatically

## Order 

Decided to do ORDER = 4 as it gives 4 keys and 5 childeren, which would be better fro the demonstration of the branching structure of the B+ tree.

## Things to reconsider
