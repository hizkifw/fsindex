# FSIndex

A small Python utility for indexing files. Currently only works in Windows and Python 2.7. It also hogs up quite a lot of RAM, so that bit might need some improvements.

Right now, it loads the whole database into memory when searching and indexing. More efficient methods will be implemented in the future.

Indexed items:

- File name and path
- File size
- MD5 hash
- Date modified

Features:

- Multithreaded file indexing
- Regex search
- Duplicate file finder