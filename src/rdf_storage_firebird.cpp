/*
 * rdf_storage_firebird.cpp - the storage API implementation
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
#include "rdf_storage_firebird.h"

#include "fb/DbConnection.h"
#include "fb/DbRowProxy.h"
#include "fb/DbStatement.h"
#include "fb/DbTransaction.h"
#include "GenericCache.h"
#include "PreparedStatements.h"
#include "RdfDbSchemaBuilder.h"

#include <cassert>
#include <cstring>
#include <string>


const char * const LIBRDF_STORAGE_FIREBIRD = "http://librdf.org/docs/api/redland-storage-module-firebird.html";

// all macros should go here
#define RET_ERROR 1
#define RET_OK 0

#ifndef LIBRDF_MALLOC
#define LIBRDF_MALLOC(type, size) (type) malloc(size)
#define LIBRDF_CALLOC(type, size, count) (type) calloc(count, size)
#define LIBRDF_FREE(type, ptr) free((type) ptr)
#endif


namespace rdf {
namespace impl {

// types and constants
using fb::DbConnection;
using fb::DbTransaction;
using fb::DbStatement;
using std::string;


struct Instance
{
    // prepared statements, lazy initialisation
    PreparedStatements statements_;
    MatchPreparedStatements matchStatements_;

    DbStatement *getPrepStatement(PrepStatementIndex psi)
    {
        PreparedStatement &s = statements_.get(psi);
        if (!s.st) {
            s.st = new DbStatement(std::move(db_.createStatement(s.sql, &tr_)));
        } else {
            s.st->reset();
        }
        return s.st;
    }

    struct GetResourceId
    {
        GetResourceId(Instance &inst) : inst_(inst)
        {
        }

        int64_t operator()(const string &s)
        {
            DbStatement *st = inst_.getPrepStatement(GET_RESOURCE_ID);
            st->setText(1, s.c_str());
            return st->uniqueResult().getInt64(0);
        }
        private:
            Instance &inst_;
    };

    DbStatement *acquireMatchStatement(unsigned int index, void *userTag,
                                       const string *sql = nullptr)
    {
        MatchPreparedStatement &mps = matchStatements_.get(index);
        MatchPreparedStatement::StatementTagPair *p = nullptr;
        for (MatchPreparedStatement::StatementTagPair &i : mps.statements) {
            if (!i.second) {
                // use statement if available
                p = &i;
            } else if (i.second == userTag) {
                // but prefer the statement of this user
                p = &i;
                break;
            }
        }

        if (!p) {
            if (!sql) {
                // no SQL to create a new prepared statement
                assert(sql);
                return nullptr;
            }

            DbStatement *st = new DbStatement(std::move(db_.createStatement(
                                                       sql->c_str(), &tr_)));
            mps.statements.emplace_back(st, userTag);
            p = &mps.statements.back();
            if (mps.sql.empty()) {
                mps.sql = *sql;
            } else {
                assert(mps.sql == *sql);
            }
        } else {
            assert(p->first);
            // mark this prepared statement as in use by current user tag
            p->second = userTag;
            p->first->reset();
        }

        return p->first;
    }

    void releaseMatchStatement(unsigned int index, void *userTag)
    {
        MatchPreparedStatement &mps = matchStatements_.get(index);
        for (MatchPreparedStatement::StatementTagPair &i : mps.statements) {
            if (i.second == userTag) {
                i.second = nullptr;
                return;
            }
        }
        assert(false && "No such user tag for prepared statement!");
    }


    DbConnection db_;
    DbTransaction tr_;
    GetResourceId getResId_;
    cache::GenericCache<string, int64_t, GetResourceId> resCache;

    Instance(const char *dbName, const char *server,
             const char *userName, const char *userPassword)
            : db_{dbName, server, userName, userPassword},
              tr_{db_.nativeHandle(), 1},
              getResId_(*this),
              resCache(getResId_)
    {
    }

    ~Instance()
    {
        for (unsigned int i = 0; i != LAST_PREP_STATEMENT_IDX; ++i) {
            PreparedStatement &s = statements_.get((PrepStatementIndex) i);
            delete s.st;
        }

        for (MatchPreparedStatement &mps : matchStatements_.statements_) {
            for (MatchPreparedStatement::StatementTagPair &i : mps.statements) {
                assert(i.second == nullptr);
                delete i.first;
            }
        }
    }

    int64_t getResourceId(const char *res)
    {
        return resCache.getValue(res);
    }
};

static inline Instance *get_instance(librdf_storage *storage)
{
    return static_cast<Instance*>(librdf_storage_get_instance(storage));
}

static inline librdf_world *get_world(librdf_storage *storage)
{
    return librdf_storage_get_world(storage);
}

static inline void free_hash(librdf_hash *hash)
{
    if (hash) {
        librdf_free_hash(hash);
    }
}

static inline librdf_node_type node_type(librdf_node *node)
{
    return node ? librdf_node_get_type(node) : LIBRDF_NODE_TYPE_UNKNOWN;
}

/** 0 based index into the STATEMENTS database view */
enum idx_triple_column_t : int
{
    IDX_STATEMENT_ID = 0,
    IDX_S_URI,
    IDX_S_BLANK,
    IDX_P_URI,
    IDX_O_URI,
    IDX_O_BLANK,
    IDX_O_TEXT,
    IDX_O_LANGUAGE,
    IDX_O_DATATYPE,
    IDX_C_URI,
    IDX_STATEMENT_COUNT
};

/** return 0 if resource does not exist */
static int64_t get_resource_id(librdf_storage *storage, const char *uri)
{
    Instance *ctx = get_instance(storage);
    return ctx->getResourceId(uri);
}

static int64_t add_resource(librdf_storage *storage, const char *uri)
{
    Instance *ctx = get_instance(storage);
    DbStatement *st = ctx->getPrepStatement(INSERT_RESOURCE);
    st->setText(1, uri);
    return st->uniqueResult().getInt64(0);
}

static int64_t get_context_id(librdf_storage *storage, const char *uri)
{
    Instance *ctx = get_instance(storage);
    DbStatement *st = ctx->getPrepStatement(GET_CONTEXT_ID);
    st->setText(1, uri);
    return st->uniqueResult().getInt64(0);
}

static int64_t add_context(librdf_storage *storage, const char *uri)
{
    Instance *ctx = get_instance(storage);
    DbStatement *st = ctx->getPrepStatement(INSERT_CONTEXT);
    st->setText(1, uri);
    return st->uniqueResult().getInt64(0);
}

static int64_t get_blank_id(librdf_storage *storage, const char *blank)
{
    Instance *ctx = get_instance(storage);
    DbStatement *st = ctx->getPrepStatement(GET_BNODE_ID);
    st->setText(1, blank);
    return st->uniqueResult().getInt64(0);
}

static int64_t add_blank(librdf_storage *storage, const char *blank)
{
    Instance *ctx = get_instance(storage);
    DbStatement *st = ctx->getPrepStatement(INSERT_BNODE);
    st->setText(1, blank);
    return st->uniqueResult().getInt64(0);
}

static int64_t get_literal_id(librdf_storage *storage, const char *literal,
                            const char *lang, const char *datatype)
{
    assert(literal);
    assert((!lang && !datatype) || (lang != nullptr) != (datatype != nullptr));
    Instance *ctx = get_instance(storage);

    if (!lang && !datatype) {
        DbStatement *st = ctx->getPrepStatement(GET_LITERAL_ID_1);
        st->setText(1, literal);
        return st->uniqueResult().getInt64(0);
    }

    if (lang) {
        DbStatement *st = ctx->getPrepStatement(GET_LITERAL_ID_2_LANG);
        st->setText(1, literal);
        st->setText(2, lang);
        return st->uniqueResult().getInt64(0);
    }

    if (!datatype) {
        assert(false);
        return 0;
    }

    int64_t dtId = get_resource_id(storage, datatype);
    if (dtId == 0) {
        return 0;
    }

    DbStatement *st = ctx->getPrepStatement(GET_LITERAL_ID_3_DT);
    st->setText(1, literal);
    st->setInt(2, dtId);
    return st->uniqueResult().getInt64(0);
}

static int64_t add_literal(librdf_storage *storage, const char *literal,
                            const char *lang, const char *datatype)
{
    assert(literal);
    assert((!lang && !datatype) || (lang != nullptr) != (datatype != nullptr));

    int64_t dtId = 0;
    if (datatype) {
        dtId = get_resource_id(storage, datatype);
        if (dtId == 0) {
            dtId = add_resource(storage, datatype);
        }
    }

    Instance *ctx = get_instance(storage);
    DbStatement *st = ctx->getPrepStatement(INSERT_LITERAL);
    st->setText(1, literal);
    st->setText(2, lang);
    if (dtId) {
        st->setInt(3, dtId);
    } else {
        st->setNull(3);
    }
    return st->uniqueResult().getInt64(0);
}

/**
 * Return 0 if !create and context node does not exist, return the id of the
 * newly created node.
 */
static int64_t get_context_node_id(librdf_storage *storage,
                                    librdf_node *context_node,
                                    bool create)
{
    assert(node_type(context_node) == LIBRDF_NODE_TYPE_RESOURCE);

    size_t len = 0;
    const unsigned char *str = librdf_uri_as_counted_string(
                                    librdf_node_get_uri(context_node), &len);

    int64_t node_id = get_context_id(storage, (const char*) str);
    if (node_id == 0) {
        if (!create) {
            return 0;
        } else {
            node_id = add_context(storage, (const char*) str);
        }
    }
    return node_id;
}

/**
 * \return 0 if not found or the statement ID otherwise
 * \param context_node_id should be != 0 if a context node exists
 */
static int64_t find_statement(librdf_storage *storage,
                              int64_t context_node_id,
                              librdf_statement *statement,
                              bool create)
{
    librdf_node *s = librdf_statement_get_subject(statement);
    librdf_node *p = librdf_statement_get_predicate(statement);
    librdf_node *o = librdf_statement_get_object(statement);

    int64_t sUri = 0;
    int64_t sBlank = 0;
    int64_t pUri = 0;
    int64_t oUri = 0;
    int64_t oBlank = 0;
    int64_t oLiteral = 0;
    int64_t cUri = 0;
    int insertedNodes = 0;

    const unsigned char *str;
    size_t len;

    if (node_type(s) == LIBRDF_NODE_TYPE_RESOURCE) {
        len = 0;
        str = librdf_uri_as_counted_string(librdf_node_get_uri(s), &len);

        sUri = get_resource_id(storage, (const char*) str);
        if (sUri == 0) {
            if (!create) {
                return 0;
            } else {
                sUri = add_resource(storage, (const char*) str);
                insertedNodes++;
            }
        }
    } else if (node_type(s) == LIBRDF_NODE_TYPE_BLANK) {
        len = 0;
        str = librdf_node_get_counted_blank_identifier(s, &len);
        sBlank = get_blank_id(storage, (const char*) str);
        if (sBlank == 0) {
            if (!create) {
                return 0;
            } else {
                sBlank = add_blank(storage, (const char*) str);
                insertedNodes++;
            }
        }
    } else {
        assert(false);
        return 0;
    }

    if (node_type(p) == LIBRDF_NODE_TYPE_RESOURCE) {
        len = 0;
        str = librdf_uri_as_counted_string(librdf_node_get_uri(p), &len);

        pUri = get_resource_id(storage, (const char*) str);
        if (pUri == 0) {
            if (!create) {
                return 0;
            } else {
                pUri = add_resource(storage, (const char*) str);
                insertedNodes++;
            }
        }
    } else {
        assert(false);
        return 0;
    }

    if (node_type(o) == LIBRDF_NODE_TYPE_RESOURCE) {
        len = 0;
        str = librdf_uri_as_counted_string(librdf_node_get_uri(o), &len);

        oUri = get_resource_id(storage, (const char*) str);
        if (oUri == 0) {
            if (!create) {
                return 0;
            } else {
                oUri = add_resource(storage, (const char*) str);
                insertedNodes++;
            }
        }
    } else if (node_type(o) == LIBRDF_NODE_TYPE_BLANK) {
        len = 0;
        str = librdf_node_get_counted_blank_identifier(o, &len);
        oBlank = get_blank_id(storage, (const char*) str);
        if (oBlank == 0) {
            if (!create) {
                return 0;
            } else {
                oBlank = add_blank(storage, (const char*) str);
                insertedNodes++;
            }
        }
    } else if (node_type(o) == LIBRDF_NODE_TYPE_LITERAL) {
        len = 0;
        str = librdf_node_get_literal_value_as_counted_string(o, &len);
        const unsigned char *dtUri = nullptr;
        librdf_uri *uri = librdf_node_get_literal_value_datatype_uri(o);
        if (uri) {
            len = 0;
            dtUri = librdf_uri_as_counted_string(uri, &len);
        }
        const char *l = librdf_node_get_literal_value_language(o);
        oLiteral = get_literal_id(storage, (const char*) str, l,
                                 (const char*) dtUri);
        if (oLiteral == 0) {
            if (!create) {
                return 0;
            } else {
                oLiteral = add_literal(storage, (const char*) str, l,
                                       (const char*) dtUri);
                insertedNodes++;
            }
        }
    } else {
        assert(false);
        return 0;
    }

    if (context_node_id != 0) {
        cUri = context_node_id;
    }

    /*
     There are 12 possible queries:
    +------+--------+------+--------+----------+------+
    | sUri | sBlank | oUri | oBlank | oLiteral | cUri |
    +------+--------+------+--------+----------+------+
    |    1 |      0 |    1 |      0 |        0 |    1 |
    |    1 |      0 |    1 |      0 |        0 |    0 |
    |    1 |      0 |    0 |      1 |        0 |    1 |
    |    1 |      0 |    0 |      1 |        0 |    0 |
    |    1 |      0 |    0 |      0 |        1 |    1 |
    |    1 |      0 |    0 |      0 |        1 |    0 |
    |    0 |      1 |    1 |      0 |        0 |    1 |
    |    0 |      1 |    1 |      0 |        0 |    0 |
    |    0 |      1 |    0 |      1 |        0 |    1 |
    |    0 |      1 |    0 |      1 |        0 |    0 |
    |    0 |      1 |    0 |      0 |        1 |    1 |
    |    0 |      1 |    0 |      0 |        1 |    0 |
    +------+--------+------+--------+----------+------+

    "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI=?"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI IS NULL"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI=?"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI IS NULL"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI=?"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI IS NULL"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI=?"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI IS NULL"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI=?"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI IS NULL"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI=?"
    "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI IS NULL"
    */

    Instance *ctx = get_instance(storage);
    DbStatement *st;

    if (insertedNodes == 0) {

        int64_t sId = 0;
        int64_t oId = 0;

        // compute query index from a 2 * 3 * 2 branching
        int qindex = 0;
        int range = 12; // 12 = 2 * 3 * 2
        // string query = "SELECT r.ID FROM TRIPLE r WHERE ";

        range /= 2; // account for two fold branching
        if (sUri) {
            // query += "r.S_URI=? ";
            sId = sUri;
            qindex += (0 * range);
        } else {
            // query += "r.S_BLANK=? ";
            sId = sBlank;
            qindex += (1 * range);
        }

        // query += "AND r.P_URI=? ";

        range /= 3; // account for three fold branching
        if (oUri) {
            // query += "AND r.O_URI=? ";
            oId = oUri;
            qindex += (0 * range);
        } else if (oBlank) {
            // query += "AND r.O_BLANK=? ";
            oId = oBlank;
            qindex += (1 * range);
        } else {
            // query += "AND r.O_LITERAL=? ";
            oId = oLiteral;
            qindex += (2 * range);
        }

        range /= 2; // account for two fold branching
        assert(range == 1);
        if (cUri) {
            // query += "AND r.C_URI=? ";
            qindex += (0 * range);
        } else {
            // query += "AND r.C_URI IS NULL ";
            qindex += (1 * range);
        }

        assert(0 <= qindex && qindex < 12);
        st = ctx->getPrepStatement(
                            (PrepStatementIndex) (SELECT_TRIPLE_0 + qindex));

        st->setInt(1, sId);
        st->setInt(2, pUri);
        st->setInt(3, oId);
        if (cUri) {
            st->setInt(4, cUri);
        }

        int64_t stId = st->uniqueResult().getInt64(0);
        if (stId != 0) {
            // statement already exists
            return stId;
        }
    } else {
        assert(create);
    }

    st = ctx->getPrepStatement(INSERT_TRIPLE);
    if (sUri) {
        st->setInt(1, sUri);
    } else {
        st->setNull(1);
    }

    if (sBlank) {
        st->setInt(2, sBlank);
    } else {
        st->setNull(2);
    }

    st->setInt(3, pUri);

    if (oUri) {
        st->setInt(4, oUri);
    } else {
        st->setNull(4);
    }

    if (oBlank) {
        st->setInt(5, oBlank);
    } else {
        st->setNull(5);
    }

    if (oLiteral) {
        st->setInt(6, oLiteral);
    } else {
        st->setNull(6);
    }

    if (cUri) {
        st->setInt(7, cUri);
    } else {
        st->setNull(7);
    }

    return st->uniqueResult().getInt64(0);
}

} // namespace impl

/*
 * Public interface implementation starts here
 */

using namespace impl;

/**
 * Create a new storage.
 *
 * Setup connection instance and connect to the database.
 */
static int pub_init(librdf_storage *storage, const char *name,
                    librdf_hash *options)
{
    if(!name) {
        free_hash(options);
        return RET_ERROR;
    }

    bool is_new = false; /* default is NOT NEW */
    if(librdf_hash_get_as_boolean(options, "new") == 1) {
        is_new = true;
    }

    bool update_index_stats = false;
    if(librdf_hash_get_as_boolean(options, "update_index_stats") == 1) {
        update_index_stats = true;
    }

    // "new='yes',host='localhost',database='red',user='foo','password='bar'"
    std::unique_ptr<char, decltype(&free)> server(librdf_hash_get(options, "host"), &free);
    std::unique_ptr<char, decltype(&free)> user(librdf_hash_get(options, "user"), &free);
    std::unique_ptr<char, decltype(&free)> password(librdf_hash_get(options, "password"), &free);

    int rc = RET_OK;
    try {

        if (is_new) {
            create_firebird_rdf_db(name, server.get(), user.get(), password.get());
        }

        if (update_index_stats) {
            // optimize queries
            update_index_statistics(name, server.get(), user.get(), password.get());
        }

        Instance *inst = new Instance(name, server.get(), user.get(), password.get());

        librdf_storage_set_instance(storage, inst);

    } catch (std::exception &e) {
        librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE,
                   NULL, "Database initialization error: %s", e.what());
        rc = RET_ERROR;
    }

    free_hash(options);
    return rc;
}


static void pub_terminate(librdf_storage *storage)
{
    Instance *inst = get_instance(storage);
    delete inst;
}


static int pub_close(librdf_storage *storage)
{
    int rc = RET_OK;
    Instance *inst = get_instance(storage);

    try {
        inst->tr_.commitRetain();
    } catch (std::exception &e) {
        rc = RET_ERROR;
        librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE,
                   NULL, "Failed to commit transaction: %s", e.what());
    }
    return rc;
}

static int pub_open(librdf_storage * /* storage */, librdf_model */* model */)
{
    // By this time pub_init will have already dealt with initialisation:
    // database connection and setup. We have nothing to do.
    return RET_OK;
}


/**
 * librdf_storage_firebird_get_feature:
 * @storage: #librdf_storage object
 * @feature: #librdf_uri feature property
 *
 * Get the value of a storage feature.
 *
 * Return value: #librdf_node feature value or NULL if no such feature
 * exists or the value is empty.
 **/
static librdf_node *pub_get_feature(librdf_storage *storage, librdf_uri *feature)
{
    if(!feature) {
        return NULL;
    }
    const unsigned char *uri_string = librdf_uri_as_string(feature);
    if(!uri_string) {
        return NULL;
    }
    if(!strncmp((const char*) uri_string, LIBRDF_MODEL_FEATURE_CONTEXTS,
                sizeof(LIBRDF_MODEL_FEATURE_CONTEXTS) + 1 )) {
        return librdf_new_node_from_typed_literal(get_world(storage),
                                                  (const unsigned char*) "1",
                                                  NULL, NULL);
    }
    return NULL;
}

static int pub_transaction_start(librdf_storage * /* storage */)
{
    // We already are using a transaction.
    return 0;
}


static int pub_transaction_commit(librdf_storage *storage)
{
    int rc = RET_OK;
    Instance *inst = get_instance(storage);
    try {
        inst->tr_.commitRetain();
    } catch (std::exception &e) {
        rc = RET_ERROR;
        librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE,
                   NULL, "Failed to commit transaction: %s", e.what());
    }
    return rc;
}

static int pub_transaction_rollback(librdf_storage *storage)
{
    int rc = RET_OK;
    Instance *inst = get_instance(storage);
    try {
        inst->tr_.rollbackRetain();
    } catch (std::exception &e) {
        rc = RET_ERROR;
        librdf_log(get_world(storage), 0, LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE,
                   NULL, "Failed to rollback transaction: %s", e.what());
    }
    return rc;
}

// iterators implementation

namespace impl {

namespace empty_stream
{

static int null_iter_end_of_stream(void*)
{
    return 1;
}

static int null_iter_next_statement(void*)
{
    return RET_ERROR;
}

static void *null_iter_get_statement(void*, const int)
{
    return nullptr;
}

static void null_iter_finished(void*)
{
}

librdf_stream *make_empty_stream(librdf_world *w)
{
    return librdf_new_stream(w, nullptr,
                             &null_iter_end_of_stream,
                             &null_iter_next_statement,
                             &null_iter_get_statement,
                             &null_iter_finished);
}

} // namespace empty_stream

namespace statement_stream {

struct StatementIterator
{
    librdf_storage *storage;
    librdf_statement *pattern;
    librdf_statement *statement;
    librdf_node *context;

    DbStatement *stmt;
    DbStatement::Iterator *it;
    unsigned int prepStatementIndex;
    bool dirty;
};

static int statement_iter_end_of_stream(void *ctx)
{
    assert(ctx && "context mustn't be NULL");
    StatementIterator *iter = (StatementIterator*) ctx;
    return !iter->it || !(*iter->it != iter->stmt->end());
}

static int statement_iter_next_statement(void *ctx)
{
    assert(ctx && "context mustn't be NULL");
    StatementIterator *iter = (StatementIterator*) ctx;
    if (statement_iter_end_of_stream(iter)) {
        return RET_ERROR;
    }

    iter->dirty = true;

    // move to the next item
    ++(*iter->it);

    if (statement_iter_end_of_stream(iter)) {
        return RET_ERROR;
    }

    return RET_OK;
}

static void *statement_iter_get_statement(void *ctx, const int getMethod)
{
    assert(ctx && "context mustn't be NULL");
    const librdf_iterator_get_method_flags flags = (librdf_iterator_get_method_flags) getMethod;
    StatementIterator *iter = (StatementIterator*) ctx;

    switch (flags) {
    case LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT:
        break;
    case LIBRDF_ITERATOR_GET_METHOD_GET_CONTEXT:
        return iter->context;
    default:
        librdf_log(get_world(iter->storage), 0,
                   LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
                   "Unknown iterator method flag %d", flags);
        return NULL;
    }

    assert(flags == LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT);

    if (!iter->dirty || statement_iter_end_of_stream(ctx)) {
        return iter->statement;
    }

    assert(iter->statement && "statement mustn't be NULL");
    librdf_world *w = get_world(iter->storage);
    librdf_statement *st = iter->statement;

    // row columns refer to find_triples_sql
    fb::DbRowProxy row = *(*iter->it);

    // string field;
    librdf_statement_clear(st);
    librdf_node *node = NULL;

    /* subject */
    if (!row.fieldIsNull(IDX_S_URI)) {
        node = librdf_new_node_from_uri_string(
                w,
                (const unsigned char*) row.getText(IDX_S_URI).c_str());
    }

    if(!node && !row.fieldIsNull(IDX_S_BLANK)) {
        node = librdf_new_node_from_blank_identifier(
                w,
                (const unsigned char*) row.getText(IDX_S_BLANK).c_str());
    }

    if(!node) {
        return NULL;
    }

    librdf_statement_set_subject(st, node);

    /* predicate */
    node = NULL;
    if (!row.fieldIsNull(IDX_P_URI)) {
        node = librdf_new_node_from_uri_string(
                w,
                (const unsigned char*) row.getText(IDX_P_URI).c_str());
    }

    if(!node) {
        return NULL;
    }

    librdf_statement_set_predicate(st, node);

    /* object */
    node = NULL;
    string field = row.getText(IDX_O_URI);
    if (!field.empty()) {
        node = librdf_new_node_from_uri_string(
                        w, (const unsigned char*) field.c_str());
    }

    if (!node) {
        field = row.getText(IDX_O_BLANK);
        if (!field.empty()) {
            node = librdf_new_node_from_blank_identifier(
                        w, (const unsigned char*) field.c_str());
        }
    }

    if (!node) {
        field = row.getText(IDX_O_TEXT);
        if (!field.empty()) { // TODO: should we better check for null?
            string lang = row.getText(IDX_O_LANGUAGE);
            string dataType = row.getText(IDX_O_DATATYPE);
            librdf_uri *t = !dataType.empty() ?
                                librdf_new_uri(
                                    w,
                                    (const unsigned char*) dataType.c_str()) :
                                NULL;
            node = librdf_new_node_from_typed_literal(w,
                    (const unsigned char*) field.c_str(),
                    lang.empty() ? NULL : lang.c_str(),
                    t);
            librdf_free_uri(t);
        }
    }

    if (!node) {
        return NULL;
    }

    librdf_statement_set_object(st, node);

    assert(librdf_statement_is_complete(st) && "incomplete statement?");
    assert(librdf_statement_match(iter->statement, iter->pattern) &&
            "match candidate doesn't match.");

    iter->dirty = false;
    return iter->statement;
}

static void statement_iter_finished(void *ctx)
{
    assert(ctx && "context mustn't be NULL");
    StatementIterator *iter = (StatementIterator*) ctx;

    if (iter->pattern) {
        librdf_free_statement(iter->pattern);
    }

    if (iter->statement) {
        librdf_free_statement(iter->statement);
    }

    librdf_storage_remove_reference(iter->storage);

    delete iter->it;
    Instance *inst = get_instance(iter->storage);
    inst->releaseMatchStatement(iter->prepStatementIndex, (void*) iter);
    LIBRDF_FREE(Iterator*, iter);
}

} // namespace statement_stream

namespace context_stream {

struct ContextIterator
{
    librdf_storage *storage;
    librdf_node *current;
    DbStatement *stmt;
    DbStatement::Iterator *it;
    bool dirty;
};

static int context_iter_is_end(void *ctx)
{
    assert(ctx && "context mustn't be NULL");
    ContextIterator *iter = (ContextIterator*) ctx;
    return !iter->it || !(*iter->it != iter->stmt->end());
}

static int context_iter_next(void *ctx)
{
    assert(ctx && "context mustn't be NULL");
    ContextIterator *iter = (ContextIterator*) ctx;
    if (context_iter_is_end(iter)) {
        return RET_ERROR;
    }

    iter->dirty = true;

    // move to the next item
    ++(*iter->it);

    if (context_iter_is_end(iter)) {
        return RET_ERROR;
    }

    return RET_OK;
}

static void *context_iter_get_current(void *ctx, const int getMethod)
{
    assert(ctx && "context mustn't be NULL");
    const librdf_iterator_get_method_flags flags = (librdf_iterator_get_method_flags) getMethod;
    ContextIterator *iter = (ContextIterator*) ctx;

    switch (flags) {
    case LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT:
        break;
    case LIBRDF_ITERATOR_GET_METHOD_GET_CONTEXT:
        return nullptr;
    default:
        librdf_log(get_world(iter->storage), 0,
                   LIBRDF_LOG_ERROR, LIBRDF_FROM_STORAGE, NULL,
                   "Unknown iterator method flag %d", flags);
        return NULL;
    }

    assert(flags == LIBRDF_ITERATOR_GET_METHOD_GET_OBJECT);

    if (!iter->dirty || context_iter_is_end(ctx)) {
        return iter->current;
    }

    // row columns refer to find_triples_sql
    fb::DbRowProxy row = *(*iter->it);

    librdf_node *node = nullptr;
    librdf_world *w = get_world(iter->storage);

    assert(!row.fieldIsNull(0));
    if (row.fieldIsNull(0)) {
        return nullptr;
    }

    node = librdf_new_node_from_uri_string(w,
            (const unsigned char*) row.getText(0).c_str());

    if (!node) {
        return nullptr;
    }

    if (iter->current) {
        librdf_free_node(iter->current);
    }

    iter->current = node;
    iter->dirty = false;
    return iter->current;
}

static void context_iter_finished(void *ctx)
{
    assert(ctx && "context mustn't be NULL");
    ContextIterator *iter = (ContextIterator*) ctx;

    if (iter->current) {
        librdf_free_node(iter->current);
    }

    delete iter->it;
    librdf_storage_remove_reference(iter->storage);
    LIBRDF_FREE(ContextIterator*, iter);
}

} // namespace context_stream

} // namespace impl

static int pub_size(librdf_storage *storage)
{
    Instance *ctx = get_instance(storage);
    DbStatement *st = ctx->getPrepStatement(GET_TRIPLE_COUNT);
    return (int) st->uniqueResult().getInt64(0);
}

static librdf_iterator *pub_get_contexts(librdf_storage *storage)
{
#if 0
    (void) storage;
    return NULL;
#else
    // TODO: this is very inefficient, the database schema needs to change to
    // implement this properly!
    using namespace context_stream;
    ContextIterator *iter = LIBRDF_CALLOC(ContextIterator*, sizeof(ContextIterator), 1);
    iter->storage = storage;
    iter->stmt = get_instance(storage)->getPrepStatement(GET_CONTEXTS);
    iter->it = new DbStatement::Iterator(std::move(iter->stmt->iterate()));
    iter->dirty = true;
    librdf_storage_add_reference(iter->storage);

    librdf_iterator *iterator = librdf_new_iterator(get_world(storage), iter,
            &context_iter_is_end, &context_iter_next, &context_iter_get_current,
            &context_iter_finished);

    if (!iterator) {
        context_iter_finished(iter);
    }
    return iterator;
#endif
}

static int pub_contains_statement(librdf_storage *storage,
                                  librdf_statement *statement)
{
    return (find_statement(storage, 0, statement, false) != 0);
}

static librdf_stream *pub_context_find_statements(librdf_storage *storage,
                                                  librdf_statement *statement,
                                                  librdf_node *context_node)
{
    Instance *db_ctx = get_instance(storage);
    librdf_world *w = get_world(storage);

    /*
    SELECT r.ID as statement_id,
           rs.URI as s_uri,
           bs.NAME as s_blank,
           rp.URI as predicate,
           ro.URI as o_uri,
           bo.NAME as o_blank,
           lo.VAL as o_literal,
           lo.LANGUAGE as o_lit_lang,
           ldt.URI as o_lit_dt,
           c.URI as context
    FROM TRIPLE r
    JOIN RESOURCE rp ON r.P_URI = rp.id
    LEFT JOIN RESOURCE rs ON r.S_URI = rs.ID
    LEFT JOIN BNODE bs ON r.S_BLANK = bs.ID
    LEFT JOIN RESOURCE ro ON r.O_URI = ro.ID
    LEFT JOIN BNODE bo ON r.O_BLANK = bo.ID
    LEFT JOIN LITERAL lo ON r.O_LITERAL = lo.ID
    LEFT JOIN RESOURCE ldt ON lo.DATATYPE = ldt.ID
    LEFT JOIN CONTEXT c ON r.C_URI = c.ID
    */

    using std::vector;

    vector<string> selectFields = {
        "r.ID as statement_id",
        "null as s_uri",
        "null as s_blank",
        "rp.URI as predicate",
        "null as o_uri",
        "null as o_blank",
        "null as o_literal",
        "null as o_lit_lang",
        "null as o_lit_dt",
        "c.URI as context"
    };
    assert(selectFields.size() == IDX_STATEMENT_COUNT);

    vector<string> innerJoins;
    vector<string> outerJoins;
    vector<string> whereCond;


    librdf_node *s = librdf_statement_get_subject(statement);
    librdf_node *p = librdf_statement_get_predicate(statement);
    librdf_node *o = librdf_statement_get_object(statement);
    size_t len;

    const unsigned char *parameters[12] = {};
    size_t idx = 0;

    // compute query index from a 3 * 2 * 6 * 2 branching
    int qindex = 0;
    int range = 72; // 72 = 3 * 2 * (1 + 1 + 3 + 1) * 2

    range /= 3; // account for three fold branching
    if (node_type(s) == LIBRDF_NODE_TYPE_RESOURCE) {
        selectFields[IDX_S_URI] = "rs.URI as s_uri";
        innerJoins.emplace_back("JOIN RESOURCE rs ON r.S_URI = rs.ID");
        whereCond.emplace_back("rs.URI=?");
        len = 0;
        parameters[idx++] = librdf_uri_as_counted_string(librdf_node_get_uri(s), &len);
        qindex += (0 * range);
    } else if (node_type(s) == LIBRDF_NODE_TYPE_BLANK) {
        selectFields[IDX_S_BLANK] = "bs.NAME as s_blank";
        innerJoins.emplace_back("JOIN BNODE bs ON r.S_BLANK = bs.ID");
        whereCond.emplace_back("bs.NAME=?");
        len = 0;
        parameters[idx++] = librdf_node_get_counted_blank_identifier(s, &len);
        qindex += (1 * range);
    } else if (node_type(s) == LIBRDF_NODE_TYPE_LITERAL) {
        // ain't no literal a subject
        return impl::empty_stream::make_empty_stream(w);
    } else {
        selectFields[IDX_S_URI] = "rs.URI as s_uri";
        selectFields[IDX_S_BLANK] = "bs.NAME as s_blank";
        outerJoins.emplace_back("LEFT JOIN RESOURCE rs ON r.S_URI = rs.ID");
        outerJoins.emplace_back("LEFT JOIN BNODE bs ON r.S_BLANK = bs.ID");
        qindex += (2 * range);
    }

    innerJoins.emplace_back("JOIN RESOURCE rp ON r.P_URI = rp.id");

    range /= 2;
    if (node_type(p) == LIBRDF_NODE_TYPE_RESOURCE) {
        whereCond.emplace_back("rp.URI=?");
        len = 0;
        parameters[idx++] = librdf_uri_as_counted_string(librdf_node_get_uri(p),
                                                         &len);
        qindex += (0 * range);
    } else if (node_type(p) != LIBRDF_NODE_TYPE_UNKNOWN) {
        assert(!"invalid predicate type in query");
        return impl::empty_stream::make_empty_stream(w);
    } else {
        qindex += (1 * range);
    }

    range /= 6;
    if (node_type(o) == LIBRDF_NODE_TYPE_RESOURCE) {
        selectFields[IDX_O_URI] = "ro.URI as o_uri";
        innerJoins.emplace_back("JOIN RESOURCE ro ON r.O_URI = ro.ID");
        whereCond.emplace_back("ro.URI=?");
        len = 0;
        parameters[idx++] = librdf_uri_as_counted_string(librdf_node_get_uri(o),
                                                         &len);
        qindex += (0 * range);
    } else if (node_type(o) == LIBRDF_NODE_TYPE_BLANK) {
        selectFields[IDX_O_BLANK] = "bo.NAME as o_blank";
        innerJoins.emplace_back("JOIN BNODE bo ON r.O_BLANK = bo.ID");
        whereCond.emplace_back("bo.NAME=?");
        len = 0;
        parameters[idx++] = librdf_node_get_counted_blank_identifier(o, &len);
        qindex += (1 * range);
    } else if (node_type(o) == LIBRDF_NODE_TYPE_LITERAL) {
        selectFields[IDX_O_TEXT] = "lo.VAL as o_literal";
        selectFields[IDX_O_LANGUAGE] = "lo.LANGUAGE as o_lit_lang";
        innerJoins.emplace_back("JOIN LITERAL lo ON r.O_LITERAL = lo.ID");
        whereCond.emplace_back("lo.VAL=?");
        len = 0;
        parameters[idx++] = librdf_node_get_literal_value_as_counted_string(
                                                                       o, &len);

        librdf_uri *uri = librdf_node_get_literal_value_datatype_uri(o);
        const char *l = librdf_node_get_literal_value_language(o);

        if (!l && !uri) {
            whereCond.emplace_back("lo.LANGUAGE IS NULL");
            whereCond.emplace_back("lo.DATATYPE IS NULL");
            qindex += (4 * range);
        } else if (l) {
            whereCond.emplace_back("lo.LANGUAGE=?");
            whereCond.emplace_back("lo.DATATYPE IS NULL");
            parameters[idx++] = (const unsigned char*) l;
            qindex += (3 * range);
        }

        if (uri && !l) {
            selectFields[IDX_O_DATATYPE] = "ldt.URI as o_lit_dt";
            innerJoins.emplace_back(
                    "JOIN RESOURCE ldt ON lo.DATATYPE = ldt.ID");
            whereCond.emplace_back("lo.LANGUAGE IS NULL");
            whereCond.emplace_back("ldt.URI=?");
            len = 0;
            parameters[idx++] = librdf_uri_as_counted_string(uri, &len);
            qindex += (2 * range);
        }
    } else {
        selectFields[IDX_O_URI] = "ro.URI as o_uri";
        selectFields[IDX_O_BLANK] = "bo.NAME as o_blank";
        selectFields[IDX_O_TEXT] = "lo.VAL as o_literal";
        selectFields[IDX_O_LANGUAGE] = "lo.LANGUAGE as o_lit_lang";
        selectFields[IDX_O_DATATYPE] = "ldt.URI as o_lit_dt";

        outerJoins.emplace_back("LEFT JOIN RESOURCE ro ON r.O_URI = ro.ID");
        outerJoins.emplace_back("LEFT JOIN BNODE bo ON r.O_BLANK = bo.ID");
        outerJoins.emplace_back("LEFT JOIN LITERAL lo ON r.O_LITERAL = lo.ID");
        outerJoins.emplace_back(
                "LEFT JOIN RESOURCE ldt ON lo.DATATYPE = ldt.ID");
        qindex += (5 * range);
    }

    range /= 2;
    assert(range == 1);
    if (context_node) {
        innerJoins.emplace_back("JOIN CONTEXT c ON r.C_URI = c.ID");
        whereCond.emplace_back("c.URI=?");
        len = 0;
        parameters[idx++] = librdf_uri_as_counted_string(
                librdf_node_get_uri(context_node), &len);
        qindex += (0 * range);
    } else {
        outerJoins.emplace_back("LEFT JOIN CONTEXT c ON r.C_URI = c.ID");
        qindex += (1 * range);
    }

    // build query
    string query = "SELECT\n";
    for (const string &s : selectFields) {
        query += s;
        query += ",\n";
    };

    // erase last ",\n"
    query.erase(query.length() - 2, 2);

    query += "\nFROM TRIPLE r\n";

    // put inner joins first
    // the Firebird query optimiser seems to mind the order
    for (const string &s : innerJoins) {
        query += s;
        query += "\n";
    }

    for (const string &s : outerJoins) {
        query += s;
        query += "\n";
    }

    if (!whereCond.empty()) {
        query += "WHERE\n";
    }

    for (vector<string>::const_iterator i = whereCond.begin();
            i != whereCond.end(); ++i) {
        if (i != whereCond.begin()) {
            query += " AND ";
        }
        query += *i;
    }

    // create iterator
    using namespace statement_stream;
    StatementIterator *iter = LIBRDF_CALLOC(
            StatementIterator *, sizeof(StatementIterator), 1);
    iter->storage = storage;
    iter->pattern = librdf_new_statement_from_statement(statement);
    iter->statement = librdf_new_statement(w);
    iter->context = context_node;
    iter->prepStatementIndex = (unsigned int) qindex;
    iter->stmt = db_ctx->acquireMatchStatement(iter->prepStatementIndex,
                                               (void*) iter, &query);
    iter->it = nullptr;
    iter->dirty = false;

    // bind query parameters, before creating an iterator
    for (unsigned int i = 0; i != idx; ++i) {
        iter->stmt->setText(i + 1, (const char*) parameters[i]);
    }

    iter->it = new DbStatement::Iterator(std::move(iter->stmt->iterate()));
    iter->dirty = true;

    librdf_storage_add_reference(iter->storage);
    librdf_stream *stream = librdf_new_stream(w, iter, &statement_iter_end_of_stream,
            &statement_iter_next_statement, &statement_iter_get_statement,
            &statement_iter_finished);

    return stream;
}

static librdf_stream *pub_find_statements(librdf_storage *storage,
                                          librdf_statement *statement)
{
    return pub_context_find_statements(storage, statement, NULL);
}

static librdf_stream *pub_context_serialise(librdf_storage *storage,
                                            librdf_node *context_node)
{
    return pub_context_find_statements(storage, NULL, context_node);
}

static librdf_stream *pub_serialise(librdf_storage *storage)
{
    return pub_context_serialise(storage, NULL);
}

static inline int priv_context_add_statement(librdf_storage *storage,
                                             int64_t context_id,
                                             librdf_statement *statement)
{
    assert(storage);
    assert(statement);
    return find_statement(storage, context_id, statement, true) ? RET_OK :
                                                                  RET_ERROR;
}

static int pub_context_add_statement(librdf_storage *storage,
                                     librdf_node *context_node,
                                     librdf_statement *statement)
{
    if(!storage) {
        return RET_ERROR;
    }

    if(!statement) {
        return RET_OK;
    }

    int64_t context_id = 0;
    if (context_node) {
        context_id = get_context_node_id(storage, context_node, true);
    }

    return priv_context_add_statement(storage, context_id, statement);
}

static int pub_context_add_statements(librdf_storage *storage,
                                      librdf_node *context_node,
                                      librdf_stream *statement_stream)
{
    pub_transaction_start(storage);

    int64_t context_id = 0;
    if (context_node) {
        context_id = get_context_node_id(storage, context_node, true);
    }

    for(; !librdf_stream_end(statement_stream);
           librdf_stream_next(statement_stream)) {
        librdf_statement *stmt = librdf_stream_get_object(statement_stream);
        const int rc = priv_context_add_statement(storage, context_id, stmt);
        if(RET_OK != rc) {
            pub_transaction_rollback(storage);
            return rc;
        }
    }

    return pub_transaction_commit(storage);
}


static int pub_add_statement(librdf_storage *storage,
                             librdf_statement *statement)
{
    return pub_context_add_statement(storage, NULL, statement);
}


static int pub_add_statements(librdf_storage *storage,
                              librdf_stream *statement_stream)
{
    return pub_context_add_statements(storage, NULL, statement_stream);
}

static int pub_context_remove_statement(librdf_storage *storage,
                                        librdf_node *context_node,
                                        librdf_statement *statement)
{
    int64_t context_id = 0;
    if (context_node) {
        context_id = get_context_node_id(storage, context_node, false);
        if (!context_id) {
            return RET_ERROR;
        }
    }

    int64_t statementId = find_statement(storage, context_id, statement, false);

    if (!statementId) {
        return RET_ERROR;
    }

    Instance *inst = get_instance(storage);
    DbStatement *st = inst->getPrepStatement(DELETE_TRIPLE);
    st->setInt(1, statementId);
    st->execute();

    return RET_OK;
}


static int pub_remove_statement(librdf_storage *storage, librdf_statement *statement)
{
    return pub_context_remove_statement(storage, NULL, statement);
}

static int pub_context_remove_statements(librdf_storage * /* storage */,
                                         librdf_node * /* context_node */)
{
    assert(!"Not implemented");
    return RET_ERROR; // TODO: implement this
//    n = transaction_start(storage);
//    for(librdf_statement *stmt = librdf_stream_get_object(statement_stream);
//            !librdf_stream_end(statement_stream);
//            librdf_stream_next(statement_stream) ) {
//        const int rc = pub_context_remove_statement(storage, context_node, stmt);
//        if( RET_OK != rc ) {
//            transaction_rollback(storage, txn);
//            return rc;
//        }
//    }
//    return transaction_commit(storage, txn);
}


// Register Storage Factory


static void register_factory(librdf_storage_factory *factory)
{
    assert(!strcmp(factory->name, LIBRDF_STORAGE_FIREBIRD));

    factory->version                    = LIBRDF_STORAGE_INTERFACE_VERSION;
    factory->init                       = pub_init;
    factory->terminate                  = pub_terminate;
    factory->open                       = pub_open;
    factory->close                      = pub_close;
    factory->size                       = pub_size;
    factory->add_statement              = pub_add_statement;
    factory->add_statements             = pub_add_statements;
    factory->remove_statement           = pub_remove_statement;
    factory->contains_statement         = pub_contains_statement;
    factory->serialise                  = pub_serialise;
    factory->find_statements            = pub_find_statements;
    factory->context_add_statement      = pub_context_add_statement;
    factory->context_add_statements     = pub_context_add_statements;
    factory->context_remove_statement   = pub_context_remove_statement;
    factory->context_remove_statements  = pub_context_remove_statements;
    factory->context_serialise          = pub_context_serialise;
    factory->find_statements_in_context = pub_context_find_statements;
    factory->get_contexts               = pub_get_contexts;
    factory->get_feature                = pub_get_feature;
    factory->transaction_start          = pub_transaction_start;
    factory->transaction_commit         = pub_transaction_commit;
    factory->transaction_rollback       = pub_transaction_rollback;
}

} // namespace rdf

void librdf_init_storage_firebird(librdf_world *world)
{
    librdf_storage_register_factory(world, LIBRDF_STORAGE_FIREBIRD,
                                    "Firebird", &rdf::register_factory);
}
