# MyStore

MyStore is a simple key:value store, where data is stored in a tree of files, and
the file format can be selected among several options, such as `dbm`, `json`,
`leveldb`, plain files. The way to store existing data can be changed without
changing the interface to access it.

## Rationale
This is a small library that I use in my hobby projects. Originally, I had a small
package that I called `dbmdb` where data was stored in a tree of `dbm` files. I used
a tree of files rather than a single instance to concurrently read/write bulks of
data from different processes and for it to scale better. As the code base slowly grew
and became chaotic, a need to create something more structured arose.

## Structure
The storage is represented by the *DB* class. Storage behaviour is
configured via 3 structural parts:

- a base storing *unit* - a minimal block containing a bunch of key:value pairs;
- a *router*, which maps each key to the base *unit* where it is to be stored;
- a *converter*, which transforms each incoming value to the format expected
  by the storing *unit* and back.
