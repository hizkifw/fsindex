# FSIndex

A small Python utility for indexing files. Currently only works in Windows and Python 2.7. It also hogs up quite a lot of RAM, so that bit might need some improvements.

Indexed items:

- File name and path
- File size
- MD5 hash
- Date modified

Some test results:
- ~~Searched through 149736 filenames in 5.83 seconds~~
- Did some optimizations, can now go through 1 million items in 0.8 secs