/*
 * PreparedStatements.h
 *
 *  Created on: Dec 5, 2015
 *      Author: toby
 */

#ifndef PREPAREDSTATEMENTS_H_
#define PREPAREDSTATEMENTS_H_

#include <string>
#include <vector>
#include <cassert>

// forward declarations
namespace fb {
class DbStatement;
}

namespace rdf {
namespace impl {

// types and constants
using fb::DbStatement;
using std::string;
using std::vector;

struct PreparedStatement
{
    const char * const sql;
    DbStatement *st;
    const int inParams;
    const int outParams;
};

enum PrepStatementIndex : unsigned int
{
    GET_RESOURCE_ID,
    INSERT_RESOURCE,
    GET_BNODE_ID,
    INSERT_BNODE,
    GET_LITERAL_ID_1,
    GET_LITERAL_ID_2_LANG,
    GET_LITERAL_ID_3_DT,
    INSERT_LITERAL,
    SELECT_TRIPLE_0,
    SELECT_TRIPLE_1,
    SELECT_TRIPLE_2,
    SELECT_TRIPLE_3,
    SELECT_TRIPLE_4,
    SELECT_TRIPLE_5,
    SELECT_TRIPLE_6,
    SELECT_TRIPLE_7,
    SELECT_TRIPLE_8,
    SELECT_TRIPLE_9,
    SELECT_TRIPLE_10,
    SELECT_TRIPLE_11,
    INSERT_TRIPLE,
    GET_TRIPLE_COUNT,
    DELETE_TRIPLE,
    LAST_PREP_STATEMENT_IDX
};

struct PreparedStatements
{
    PreparedStatement statements_[LAST_PREP_STATEMENT_IDX];

    PreparedStatements();
    PreparedStatement &get(PrepStatementIndex psi);
};

inline PreparedStatement &PreparedStatements::get(PrepStatementIndex psi)
{
    assert((int) psi < (int) LAST_PREP_STATEMENT_IDX);
    return statements_[psi];
}

struct MatchPreparedStatement
{
    typedef void* UserTag;
    using StatementTagPair = std::pair<DbStatement*, UserTag>;

    vector<StatementTagPair> statements;
    string sql;
};

struct MatchPreparedStatements
{
    static constexpr unsigned int MATCH_STATEMENTS_COUNT = 72;
    MatchPreparedStatement statements_[MATCH_STATEMENTS_COUNT];

    MatchPreparedStatement &get(unsigned int index);
};

inline MatchPreparedStatement &MatchPreparedStatements::get(unsigned int index)
{
    assert(index < MATCH_STATEMENTS_COUNT);
    return statements_[index];
}

} /* namespace impl */
} /* namespace rdf */

#endif /* PREPAREDSTATEMENTS_H_ */
