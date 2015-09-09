/*
 * rdf_storage_firebird.h - the public header of the library
 *
 * This is part of the "librdf.firebird" storage module for the
 * "Redland RDF Library" (http://librdf.org/) that stores and
 * retrieves RDF data from a Firebird database.
 *
 * @created: Jul 18, 2015
 *
 * @copyright: Copyright (c) 2015 Robert Zavalczki, distributed
 * under the terms and conditions of the Lesser GNU General
 * Public License version 2.1
 */
#ifndef REDLAND_RDF_STORAGE_FIREBIRD_H
#define REDLAND_RDF_STORAGE_FIREBIRD_H

#include <librdf.h>

/**
 * Factory name
 */
extern const char * const LIBRDF_STORAGE_FIREBIRD;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Register the Firebird storage factory with the librdf library
 *
 * After registration clients can create instances of the
 * librdf.firebird storage module by calling:
 *     librdf_new_storage(world, LIBRDF_STORAGE_FIREBIRD,
 *                        database_path, options);
 */
void librdf_init_storage_firebird(librdf_world *world);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // ! REDLAND_RDF_STORAGE_FIREBIRD_H
