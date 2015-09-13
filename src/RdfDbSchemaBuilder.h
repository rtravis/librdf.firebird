/*
 * RdfDbSchemaBuilder.h - create schema objects in the database
 *
 * This is part of the "librdf.firebird" storage module for the
 * "Redland RDF Library" (http://librdf.org/) that stores and
 * retrieves RDF data from a Firebird database.
 *
 * @created: Sep 6, 2015
 *
 * @copyright: Copyright (c) 2015 Robert Zavalczki, distributed
 * under the terms and conditions of the Lesser GNU General
 * Public License version 2.1
 */
#ifndef RDFDBSCHEMABUILDER_H_
#define RDFDBSCHEMABUILDER_H_

namespace rdf
{

void create_firebird_rdf_db(const char *dbName);
void update_index_statistics(const char *dbName);

} /* namespace rdf */

#endif /* RDFDBSCHEMABUILDER_H_ */
