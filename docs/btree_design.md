# B+ Tree Design

## Why B+ Tree?

It is a standard indexing structure in databases (MySQL, SQLite, PostgreSQL)

- **O(log n)** search, insert, delete
- **Very disk-friendly** - each node = one page
- **Range queries** - linked leaf nodes
- **Self-balancing** - it maintains height automatically

## THings to reconsider

ORDER (max keys per node) - for now 3 but splits midpoint results in odd numbers (so i think i would change to 4)