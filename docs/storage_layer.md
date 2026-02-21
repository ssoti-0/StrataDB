### Design of Storage Layer (DiskManager)

### Responsibilities of DiskManager
- Manage page-based I/O
- Fixed page size: 4096 bytes
- Map page_id -> byte offset on disk
- Abstract operations from upper layers

### Key operations
- read_page(page_id, buffer) - Reads page into buffer
- write_page(page_id, buffer) - Write buffer to page
- Constructor - Opens/Creates database file 

### Some issues that have to be resolved (Resolved)
- File format - **Binary** - Due to fixed-size pages, has predictable seeking, and  raw integer storage
- Error Handling - **Exceptions** - std::runtime_error with descriptive messaging
- Page allocation - **Simple append-to-end strategy** - allocate_page() writes a zero-filled page and returns its id.
-File I/O API - **std::fstream** 

### Implementation Notes
