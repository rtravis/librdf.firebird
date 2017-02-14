/*
 * rdf_firebird_tester.cpp - tester program
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
#include <memory>
#include <unistd.h>
#include <map>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string.h>
#include "rdf_storage_firebird.h"

using namespace std;

static shared_ptr<librdf_world> make_rdf_world()
{
    shared_ptr<librdf_world> world(librdf_new_world(),
                                   &librdf_free_world);
    librdf_world_open(world.get());

    // register the storage factory
    librdf_init_storage_firebird(world.get());

    return world;
}

static shared_ptr<librdf_storage> make_rdf_storage(librdf_world *world,
                                                const char *db_name,
                                                const char *server,
                                                const char *user,
                                                const char *password,
                                                bool is_new = false,
                                                bool update_index_stats = false)
{
    char options[2048];
    // "new='yes',host='localhost',user='sysdba',password='masterkey'"
    snprintf(options, sizeof(options),
            "host='%s', user='%s', password='%s', new='%s', update_index_stats='%s'",
            server, user, password, is_new ? "yes" : "no",
            update_index_stats ? "yes" : "no");

    shared_ptr<librdf_storage> store(
                                librdf_new_storage(world,
                                                   LIBRDF_STORAGE_FIREBIRD,
                                                   db_name,
                                                   options),
                                &librdf_free_storage);
    return store;
}

static shared_ptr<librdf_model> make_rdf_model(librdf_world *world,
                                               librdf_storage *store)
{
    shared_ptr<librdf_model> model(
                                librdf_new_model(world, store, NULL),
                                &librdf_free_model);
    return model;
}

static shared_ptr<librdf_parser> make_rdf_parser(librdf_world *world,
                                            const char *parser_name = nullptr)
{
    shared_ptr<librdf_parser> parser(
                            librdf_new_parser(world, parser_name, NULL, NULL),
                            &librdf_free_parser);
    return parser;
}

static shared_ptr<librdf_uri> make_rdf_uri(librdf_world *world,
                                           const char *uri_str)
{
    shared_ptr<librdf_uri> uri(
                            librdf_new_uri(world, (const unsigned char*) uri_str),
                            &librdf_free_uri);
    return uri;
}

/**
 * \param file_name file containing RDF data
 * \param parser_name the format of the RDF data in the file. If not set, the
 * format will be guessed based on the file extension.
 * \param base_uri the base URI to be considered for the document
 * \param context_uri what context to use for the statements. If the special
 * value "" is used, the file name will be used as the context URI.
 */
static bool import_file(librdf_world *world,
                        librdf_model *model,
                        const char *file_name,
                        const char *parser_name = nullptr,
                        const char *base_uri = nullptr,
                        const char *context_uri = nullptr)
{
    map<string, string> parsers{
        { "nt", "ntriples" },
        { "nq", "ntriples" },
        { "xml", "rdfxml" },
        { "ttl", "turtle" },
        { "rdf", "rdfxml" }
    };

    if (!parser_name) {
        string param1 = file_name;
        string::size_type pos = param1.find_last_of("./");
        string file_ext(param1, pos != string::npos ? pos + 1 : 0);
        if (!parsers[file_ext].empty()) {
            parser_name = parsers[file_ext].c_str();
        }
    }

    shared_ptr<librdf_parser> parser = make_rdf_parser(world, parser_name);
    if (!parser) {
        return false;
    }

    string file_uri = file_name;
    if (file_uri.compare(0, 5, "file:") != 0) {
        file_uri.insert(0, "file:");
    }

    shared_ptr<librdf_uri> uri = make_rdf_uri(world, file_uri.c_str());
    if (!uri) {
        return false;
    }

    shared_ptr<librdf_uri> base;
    if (base_uri) {
        base = make_rdf_uri(world, base_uri);
    } else {
        base = uri;
    }

    librdf_stream *stream = librdf_parser_parse_as_stream(
                                        parser.get(), uri.get(), base.get());

    librdf_node *context_node = nullptr;
    shared_ptr<librdf_uri> curi;
    if (context_uri && *context_uri == '\0') {
        context_node = librdf_new_node_from_uri(world, uri.get());
    } else if (context_uri) {
        curi = make_rdf_uri(world, context_uri);
        context_node = librdf_new_node_from_uri(world, curi.get());
    }

    librdf_model_context_add_statements(model, context_node, stream);

    librdf_free_node(context_node);

    librdf_free_stream(stream);
    return true;
}

void run_query(librdf_world *world, librdf_model *model,
               const char *query_string, const char *lang = "sparql")
{
    shared_ptr<librdf_query> rdf_query(
                                librdf_new_query(world, lang, nullptr,
                                            (const unsigned char*) query_string,
                                            nullptr),
                                &librdf_free_query);

    librdf_query_results *res = librdf_query_execute(rdf_query.get(), model);
    if (!res) {
        return;
    }

    librdf_query_results_to_file_handle2(res, stdout, "csv",
                                         nullptr, nullptr, nullptr);

    librdf_free_query_results(res);
}

static int usage(int /* argc */, char *argv[])
{
    cout << "Synopsys:\n"
         << "    " << argv[0] << " <db_connection> [-i <import_rdf_file>] | [-q <sparql_query_file>]\n"
         << "\n"
         << "db_connection:\n"
         << "    -d <db_name> [-new] [-s <server>] [-u <user>] [-p <password>]\n"
         << "\n";
    return 1;
}


int main(int argc, char *argv[])
{
    string dbName;
    string server = "localhost";
    string userName = "sysdba";
    string password = "masterkey";
    string importFile;
    string queryFile;
    bool is_new = false;

    for (int i = 0; i < argc; ++i) {

        if (strcmp(argv[i], "-d") == 0 && (i + 1) < argc) {
            // database file
            dbName = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "-s") == 0 && (i + 1) < argc) {
            // server
            server = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "-u") == 0 && (i + 1) < argc) {
            // user
            userName = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "-p") == 0 && (i + 1) < argc) {
            // password
            password = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "-i") == 0 && (i + 1) < argc) {
            // import file
            importFile = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "-q") == 0 && (i + 1) < argc) {
            // query file
            queryFile = argv[i + 1];
            i++;
        } else if (strcmp(argv[i], "-new") == 0) {
            is_new = true;
        } else if (strcmp(argv[i], "-h") == 0) {
            // database connection options
            usage(argc, argv);
            return 0;
        }
    }

    if (dbName.empty()) {
        cout << "Database name (-d switch) is required!\n";
        return usage(argc, argv);
    }

    if (importFile.empty() && queryFile.empty()) {
        return usage(argc, argv);
    }

    shared_ptr<librdf_world> world = make_rdf_world();
    if (!world) {
        return 1;
    }

    shared_ptr<librdf_storage> store = make_rdf_storage(world.get(),
                                            dbName.c_str(), server.c_str(),
                                            userName.c_str(), password.c_str(),
                                            is_new);
    if (!store) {
        return 1;
    }

    shared_ptr<librdf_model> model = make_rdf_model(world.get(), store.get());
    if (!model) {
        return 1;
    }

    if (!importFile.empty()) {
        return import_file(world.get(), model.get(), importFile.c_str()) ? 0 : 1;
    }

    string query;
    if (!queryFile.empty()) {
        ifstream ifs(queryFile.c_str());
        stringstream ss;
        ss << ifs.rdbuf();
        query = ss.str();
    }

    run_query(world.get(), model.get(), query.c_str());
    return 0;
}
