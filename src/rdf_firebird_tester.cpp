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
												const char *file_path,
												bool is_new = false,
												bool use_contexts = true)
{
	char options[64];
	snprintf(options, sizeof(options),
			"new='%s', contexts='%s', synchronous='off'",
			is_new ? "yes" : "no", use_contexts ? "yes" : "no");
	shared_ptr<librdf_storage> store(
								librdf_new_storage(world,
											LIBRDF_STORAGE_FIREBIRD,
										file_path,
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

static bool import_file(librdf_world *world,
						librdf_model *model,
						const char *file_name,
						const char *parser_name = nullptr,
						bool set_context = true)
{
	map<string, string> parsers{
		{ "nt", "ntriples" },
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

	librdf_stream *stream = librdf_parser_parse_as_stream(
										parser.get(), uri.get(), uri.get());

	librdf_node *context_node = librdf_new_node_from_uri(world, uri.get());

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


int main(int argc, char *argv[])
{
	string importFile;
	string queryFile;

	for (int i = 0; i < argc; ++i) {

		if (strcmp(argv[i], "-i") == 0 && (i + 1) < argc) {
			// import file
			importFile = argv[i + 1];
			i++;
		} else if (strcmp(argv[i], "-q") == 0 && (i + 1) < argc) {
			// query file
			queryFile = argv[i + 1];
			i++;
		}
	}

	if (importFile.empty() && queryFile.empty()) {
		cout << "Usage: " << argv[0] << " [-i <import_rdf_file>] | [-q <sparql_query_file> ]\n";
		return 1;
	}

	shared_ptr<librdf_world> world = make_rdf_world();
	if (!world) {
		return 1;
	}

	const char *file_path =  "/tmp/rdf_store_fb2.fdb";

	bool is_new = (access(file_path, F_OK) < 0);

	shared_ptr<librdf_storage> store = make_rdf_storage(world.get(),
														file_path, is_new);
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

	/*
	const char *query = R"(

    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX dc: <http://purl.org/dc/elements/1.1/>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX dcterms: <http://purl.org/dc/terms/>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>

    SELECT ?s ?p 
    WHERE { ?s ?p "bear" . # ^^<http://www.w3.org/2001/XMLSchema#integer> .  
          }

    )";
    */

	run_query(world.get(), model.get(), query.c_str());
}
