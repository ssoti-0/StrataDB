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

**Public Interface**

| Method | Description |

| DiskManager(const std::string& file_path) | Opens or creates database file |
| read_page(page_id, buffer) | Reads a page into a 'Page' (std::array<char, 4096>) |
| write_page(page_id, buffer ) | Writes a 'Page' to disk; allows appending one new page |
| allocate_page() | Appends a zero-filled page, returns its page_id |
| num_pages() | Returns cached page count |
| file_path() | Returns file path |


### Key Design Decisions

1. **Two-phase file open** - 'std::fstream' with 'ios::in' | **Why** - first time you the databse is run, the file doesn't exist, so I think it would be useful to first probe whether the file exists and if not to just create it.

2. **Cached page count** - 'num_pages_' is computed once from file constructor ('file_size / PAGE_SIZE') and updated incrementally | **Why** - computing page count requires a disk seek, which os much slower than reading from memory.

3. **mutable std::fstream** - seekg()mutates the stream position | **Why** - mutable lets me keep read_page() a const.

4. **Non-copyable** - copy constructor and assignment operator are deleted | **Why** - two managers on the same file would cause data corruption.

5. **Validation bounds** - 'read_page': throws if 'page_id >= num_pages_' (can't read unallocated pages) ; 'write_page': throws if 'page_id > num_pages_' (no gaps; '==' is allowed for append) | **Why** - 'read_page' - could read garbage from disk, could crash - 'write_page' - file sizes wouldn't match and pages must be contiguous.
