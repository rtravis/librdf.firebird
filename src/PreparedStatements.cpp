/*
 * PreparedStatements.cpp - manage the prepared statements cache
 *
 * This is part of the "librdf.firebird" storage module for the
 * "Redland RDF Library" (http://librdf.org/) that stores and
 * retrieves RDF data from a Firebird database.
 *
 * @created: Dec 5, 2015
 *
 * @copyright: Copyright (c) 2015 Robert Zavalczki, distributed
 * under the terms and conditions of the Lesser GNU General
 * Public License version 2.1
 */

#include "PreparedStatements.h"

namespace rdf {
namespace impl {


PreparedStatements::PreparedStatements() : statements_{
        {
            // GET_RESOURCE_ID
            "SELECT ID FROM RESOURCE WHERE URI=?",
            nullptr,
            1, 1
        },
        {
            // INSERT_RESOURCE
            "INSERT INTO RESOURCE (ID, URI) "
            "VALUES (NEXT VALUE FOR SEQ_RESOURCE, ?) "
            "RETURNING ID",
            nullptr,
            2, 1
        },
        {
            // GET_CONTEXT_ID
            "SELECT ID FROM CONTEXT WHERE URI=?",
            nullptr,
            1, 1
        },
        {
            // INSERT_CONTEXT
            "INSERT INTO CONTEXT (ID, URI) "
            "VALUES (NEXT VALUE FOR SEQ_CONTEXT, ?) "
            "RETURNING ID",
            nullptr,
            2, 1
        },
        {
            // GET_BNODE_ID
            "SELECT ID FROM BNODE WHERE NAME=?",
            nullptr,
            1, 1
        },
        {
            // INSERT_BNODE
            "INSERT INTO BNODE (ID, NAME) "
            "VALUES (NEXT VALUE FOR SEQ_BNODE, ?) "
            "RETURNING ID",
            nullptr,
            2, 1
        },
        {
            // GET_LITERAL_ID_1
            "SELECT ID FROM LITERAL WHERE VAL=? "
            "AND LANGUAGE IS NULL AND DATATYPE IS NULL",
            nullptr,
            1, 1
        },
        {
            // GET_LITERAL_ID_2_LANG
            "SELECT ID FROM LITERAL WHERE VAL=? AND LANGUAGE=? AND DATATYPE IS NULL",
            nullptr,
            2, 1
        },
        {
            // GET_LITERAL_ID_3_DT
            "SELECT ID FROM LITERAL WHERE VAL=? AND LANGUAGE IS NULL AND DATATYPE=?",
            nullptr,
            2, 1
        },
        {
            // INSERT_LITERAL
            "INSERT INTO LITERAL (ID, VAL, LANGUAGE, DATATYPE) "
            "VALUES (NEXT VALUE FOR SEQ_LITERAL, ?, ?, ?) "
            "RETURNING ID",
            nullptr,
            3, 1
        },
        {
            // SELECT_TRIPLE_0
            "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI=?"
            " PLAN (r INDEX (IDX_TRIPLE_S_URI))",
            nullptr,
            4, 1
        },
        {
            // SELECT_TRIPLE_1
            "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI IS NULL"
            " PLAN (r INDEX (IDX_TRIPLE_S_URI))",
            nullptr,
            3, 1
        },
        {
            // SELECT_TRIPLE_2
            "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI=?"
            " PLAN (r INDEX (IDX_TRIPLE_S_URI))",
            nullptr,
            4, 1
        },
        {
            // SELECT_TRIPLE_3
            "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI IS NULL"
            " PLAN (r INDEX (IDX_TRIPLE_S_URI))",
            nullptr,
            3, 1
        },
        {
            // SELECT_TRIPLE_4
            "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI=?"
            " PLAN (r INDEX (IDX_TRIPLE_S_URI))",
            nullptr,
            4, 1
        },
        {
            // SELECT_TRIPLE_5
            "SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI IS NULL"
            " PLAN (r INDEX (IDX_TRIPLE_S_URI))",
            nullptr,
            3, 1
        },
        {
            // SELECT_TRIPLE_6
            "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI=?"
            " PLAN (r INDEX (IDX_TRIPLE_S_BLANK, IDX_TRIPLE_O_URI))",
            nullptr,
            4, 1
        },
        {
            // SELECT_TRIPLE_7
            "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI IS NULL"
            " PLAN (r INDEX (IDX_TRIPLE_S_BLANK, IDX_TRIPLE_O_URI))",
            nullptr,
            3, 1
        },
        {
            // SELECT_TRIPLE_8
            "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI=?"
            " PLAN (r INDEX (IDX_TRIPLE_S_BLANK, IDX_TRIPLE_O_BLANK))",
            nullptr,
            4, 1
        },
        {
            // SELECT_TRIPLE_9
            "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI IS NULL"
            " PLAN (r INDEX (IDX_TRIPLE_S_BLANK, IDX_TRIPLE_O_BLANK))",
            nullptr,
            3, 1
        },
        {
            // SELECT_TRIPLE_10
            "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI=?"
            " PLAN (r INDEX (IDX_TRIPLE_S_BLANK, IDX_TRIPLE_O_LITERAL))",
            nullptr,
            4, 1
        },
        {
            // SELECT_TRIPLE_11
            "SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI IS NULL"
            " PLAN (r INDEX (IDX_TRIPLE_S_BLANK, IDX_TRIPLE_O_LITERAL))",
            nullptr,
            3, 1
        },
        {
            // INSERT_TRIPLE
            "INSERT INTO TRIPLE (ID, S_URI, S_BLANK, P_URI, O_URI, O_BLANK, "
            "O_LITERAL, C_URI) "
            "VALUES (NEXT VALUE FOR SEQ_TRIPLE, ?, ?, ?, ?, ?, ?, ?) "
            "RETURNING ID",
            nullptr,
            7, 1
        },
        {
            // GET_TRIPLE_COUNT
            "SELECT COUNT(*) FROM TRIPLE",
            nullptr,
            0, 1
        },
        {
            // DELETE_TRIPLE
            "DELETE FROM TRIPLE WHERE ID=?",
            nullptr,
            1, 0
        },
        {
            // GET_CONTEXTS
            "SELECT URI "
            "FROM CONTEXT",
            nullptr,
            0, 1
        },
    }
{
}

} /* namespace impl */
} /* namespace rdf */
