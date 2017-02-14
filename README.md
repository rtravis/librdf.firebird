
# [Firebird](http://firebirdsql.org/) RDF triple [storage module](http://librdf.org/docs/api/redland-storage-modules.html) for [librdf](http://librdf.org/).

## Overview

librdf.firebird is a storage module for the [Redland RDF Library (librdf)](http://librdf.org/)
that stores and retrieves [RDF](http://www.w3.org/RDF/) data from a Firebird
database. Its purpose is to provide reliable and fast access to very large RDF
data sets. Unlike most other persistent librdf storage modules, librdf.firebird
doesn't use hashes as a handle to the actual data, a method that - at least in
theory - could yield wrong results due to hash collisions.

librdf.firebird was inspired by the [official sqlite store](https://github.com/dajobe/librdf/blob/master/src/rdf_storage_sqlite.c)
and the [improved sqlite store](https://github.com/mro/librdf.sqlite).

## Requirements

To use librdf.firebird in your program you need: a C++11 capable compiler, the
Redland libraries (librdf) and the Firebird libraries. The
[DbWrap++FB](https://github.com/rtravis/DbWrap-FB) C++ wrapper library for the
Firebird C API is included as a git submodule.

## Usage

For sample usage: RDF data importing and running SPARQL queries you can refer
to the rdf_firebird_tester.cpp file in the sources directory.

```cpp

    #include <rdf_storage_firebird.h>
    // ....
    librdf_world *world = ...;
    // ....
    librdf_init_storage_firebird(world); // register the storage factory
    // ....
    const char *options ="new='yes', host='localhost', "
                         "user='sysdba', password='masterkey'";

    librdf_storage *newStorage = librdf_new_storage(
                                    world, LIBRDF_STORAGE_FIREBIRD,
                                    file_path, options);
```

## License

librdf.firebird is an open source free software project.
Copyright (c) 2015 Robert Zavalczki, distributed under the terms
and conditions of the Lesser GNU General Public License version
2.1.
