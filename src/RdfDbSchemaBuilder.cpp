/*
 * RdfDbSchemaBuilder.cpp - create schema objects in the database
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

#include "RdfDbSchemaBuilder.h"
#include "fb/DbConnection.h"
#include "fb/DbTransaction.h"
#include "fb/DbStatement.h"
#include "fb/DbRowProxy.h"
#include <string>
#include <string.h>
#include <cassert>

namespace rdf
{

struct DbObject
{
	const char *name;
	const char *type;
	const char *sql;
};

static DbObject db_schema[] = {
{
	"SEQ_RESOURCE",
	"SEQUENCE",
	R"(CREATE SEQUENCE SEQ_RESOURCE)"
},
{
	"SEQ_LITERAL",
	"SEQUENCE",
	R"(CREATE SEQUENCE SEQ_LITERAL)"
},
{
	"SEQ_BNODE",
	"SEQUENCE",
	R"(CREATE SEQUENCE SEQ_BNODE)"
},
{
	"SEQ_TRIPLE",
	"SEQUENCE",
	R"(CREATE SEQUENCE SEQ_TRIPLE)"
},
{
	"Resource",
	"TABLE",
	R"(CREATE TABLE Resource
(
	ID bigint NOT NULL,
	URI varchar(255) NOT NULL,
	CONSTRAINT PK_RESOURCE PRIMARY KEY(ID),
	CONSTRAINT UQ_RESOURCE_URI UNIQUE (URI)
))"
},
{
	"Literal",
	"TABLE",
	R"(CREATE TABLE Literal
(
	ID bigint NOT NULL,
	VAL varchar(1000) NOT NULL,
	LANGUAGE varchar(8) DEFAULT NULL,
	DATATYPE BIGINT DEFAULT NULL,
	CONSTRAINT PK_LITERAL PRIMARY KEY(ID),
	CONSTRAINT FK_LITERAL_DATATYPE 
		FOREIGN KEY (DATATYPE) REFERENCES Resource (ID),
	CONSTRAINT UQ_LITERAL_VAL UNIQUE (VAL, LANGUAGE, DATATYPE),
	CONSTRAINT CK_LITERAL_LANG_DT
		CHECK ((LANGUAGE IS NULL AND DATATYPE IS NULL) OR 
			   (LANGUAGE IS NOT NULL AND DATATYPE IS NULL) OR
			   (LANGUAGE IS NULL AND DATATYPE IS NOT NULL))
))"
},
{
	"Bnode",
	"TABLE",
	R"(CREATE TABLE Bnode
(
	ID bigint NOT NULL,
	NAME varchar(40) NOT NULL,
	CONSTRAINT PK_BNODE PRIMARY KEY(ID),
	CONSTRAINT UQ_BNODE_NAME UNIQUE (NAME)
))"
},
{
	"Triple",
	"TABLE",
	R"(CREATE TABLE Triple
(
	ID bigint NOT NULL,
	S_URI bigint,
	S_BLANK bigint,
	P_URI bigint NOT NULL,
	O_URI bigint,
	O_BLANK bigint,
	O_LITERAL bigint,
	C_URI bigint DEFAULT NULL,
	CONSTRAINT PK_TRIPLE PRIMARY KEY(ID),
	CONSTRAINT FK_TRIPLE_S_URI 
		FOREIGN KEY (S_URI) REFERENCES Resource (ID),
	CONSTRAINT FK_TRIPLE_S_BLANK
		FOREIGN KEY (S_BLANK) REFERENCES Bnode (ID),
	CONSTRAINT FK_TRIPLE_P_URI
		FOREIGN KEY (P_URI) REFERENCES Resource (ID),
	CONSTRAINT FK_TRIPLE_O_URI
		FOREIGN KEY (O_URI) REFERENCES Resource (ID),
	CONSTRAINT FK_TRIPLE_O_BLANK
		FOREIGN KEY (O_BLANK) REFERENCES Bnode (ID),
	CONSTRAINT FK_TRIPLE_O_LITERAL
		FOREIGN KEY (O_LITERAL) REFERENCES Literal (ID),
	CONSTRAINT FK_TRIPLE_C_URI
		FOREIGN KEY (C_URI) REFERENCES Resource (ID),
	CONSTRAINT UQ_TRIPLE
		UNIQUE (S_URI, S_BLANK, P_URI, O_URI, O_BLANK, O_LITERAL, C_URI),
	CONSTRAINT CK_TRIPLE_SUBJ
		CHECK ((S_URI IS NOT NULL AND S_BLANK IS NULL) OR 
			   (S_URI IS NULL AND S_BLANK IS NOT NULL)),
	CONSTRAINT CK_TRIPLE_OBJ
		CHECK ((O_URI IS NOT NULL AND O_BLANK IS NULL AND O_LITERAL IS NULL) OR 
			   (O_URI IS NULL AND O_BLANK IS NOT NULL AND O_LITERAL IS NULL) OR 
			   (O_URI IS NULL AND O_BLANK IS NULL AND O_LITERAL IS NOT NULL))
))"
},
{
	"IDX_TRIPLE_S_URI",
	"INDEX",
	R"(CREATE INDEX IDX_TRIPLE_S_URI ON TRIPLE (S_URI))"
},
{
	"IDX_TRIPLE_S_BLANK",
	"INDEX",
	R"(CREATE INDEX IDX_TRIPLE_S_BLANK ON TRIPLE (S_BLANK))"
},
{
	"IDX_TRIPLE_P_URI",
	"INDEX",
	R"(CREATE INDEX IDX_TRIPLE_P_URI ON TRIPLE (P_URI))"
},
{
	"IDX_TRIPLE_O_URI",
	"INDEX",
	R"(CREATE INDEX IDX_TRIPLE_O_URI ON TRIPLE (O_URI))"
},
{
	"IDX_TRIPLE_O_BLANK",
	"INDEX",
	R"(CREATE INDEX IDX_TRIPLE_O_BLANK ON TRIPLE (O_BLANK))"
},
{
	"IDX_TRIPLE_O_LITERAL",
	"INDEX",
	R"(CREATE INDEX IDX_TRIPLE_O_LITERAL ON TRIPLE (O_LITERAL))"
},
{
	"IDX_TRIPLE_C_URI",
	"INDEX",
	R"(CREATE INDEX IDX_TRIPLE_C_URI ON TRIPLE (C_URI))"
},
{
	"STATEMENTS_N3",
	"VIEW",
	R"(CREATE VIEW STATEMENTS_N3 as
SELECT r.ID as statement_id,
	   coalesce('<' || rs.URI || '>', '_:' || bs.NAME) as subject,
	   '<' || rp.URI || '>' as predicate,
	   coalesce('<' || ro.URI || '>', '_:' || bo.NAME, 
	   '"' || lo.VAL || '"' || 
		   coalesce('@' || lo.LANGUAGE, '') ||
		   coalesce('^^' || '<' || ldt.URI || '>', '')) as object,
	   '<' || c.URI || '>' as context
FROM TRIPLE r
LEFT JOIN RESOURCE rs ON r.S_URI = rs.ID
LEFT JOIN BNODE bs ON r.S_BLANK = bs.ID
JOIN RESOURCE rp ON r.P_URI = rp.id
LEFT JOIN RESOURCE ro ON r.O_URI = ro.ID
LEFT JOIN BNODE bo ON r.O_BLANK = bo.ID
LEFT JOIN LITERAL lo ON r.O_LITERAL = lo.ID
LEFT JOIN RESOURCE ldt ON lo.DATATYPE = ldt.ID
LEFT JOIN RESOURCE c ON r.C_URI = c.ID)"
},
{
	"STATEMENTS",
	"VIEW",
	R"(CREATE VIEW STATEMENTS as
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
LEFT JOIN RESOURCE rs ON r.S_URI = rs.ID
LEFT JOIN BNODE bs ON r.S_BLANK = bs.ID
JOIN RESOURCE rp ON r.P_URI = rp.id
LEFT JOIN RESOURCE ro ON r.O_URI = ro.ID
LEFT JOIN BNODE bo ON r.O_BLANK = bo.ID
LEFT JOIN LITERAL lo ON r.O_LITERAL = lo.ID
LEFT JOIN RESOURCE ldt ON lo.DATATYPE = ldt.ID
LEFT JOIN RESOURCE c ON r.C_URI = c.ID)"
},
{
	nullptr,
	nullptr,
	nullptr
}
};

using std::string;
using fb::DbConnection;
using fb::DbStatement;
using fb::DbTransaction;

void create_firebird_rdf_db(const char *dbName)
{
	DbConnection db{dbName};
	DbTransaction tr{db.nativeHandle(), 1};

	for (DbObject &obj : db_schema) {
		if (!obj.name) {
			break;
		}

		const char *checkTable = nullptr;
		const char *whereField = nullptr;
		bool isRelation = false;

		if (strcasecmp(obj.type, "SEQUENCE") == 0) {
			checkTable = "RDB$GENERATORS";
			whereField = "RDB$GENERATOR_NAME";
		} else if (strcasecmp(obj.type, "TABLE") == 0 ||
				   strcasecmp(obj.type, "VIEW") == 0) {
			checkTable = "RDB$RELATIONS";
			whereField = "RDB$RELATION_NAME";
			isRelation = true;
		} else if (strcasecmp(obj.type, "INDEX") == 0) {
			checkTable = "RDB$INDICES";
			whereField = "RDB$INDEX_NAME";
		}

		if (!checkTable || !whereField) {
			assert(false);
			continue;
		}

		string q = "select RDB$DB_KEY from ";
		q += checkTable;
		q += " where ";
		q += whereField;
		q += "=?";

		DbStatement st = db.createStatement(q.c_str(), &tr);
		st.setText(1, obj.name);

		if (st.uniqueResult()) {
			// object already exists
			continue;
		}

		db.executeUpdate(obj.sql, &tr);

		if (isRelation) {
			q = "GRANT ALL ON ";
			q += obj.name;
			q += " TO PUBLIC WITH GRANT OPTION";
			db.executeUpdate(q.c_str(), &tr);
		}
	}
}

} /* namespace rdf */
