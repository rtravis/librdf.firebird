/*
 * librdf_firebird_schema.sql
 *
 * SQL statements defining the librdf.firebird database schema
 *
 * This is part of the "librdf.firebird" storage module for the
 * "Redland RDF Library" (http://librdf.org/) that stores and
 * retrieves RDF data from a Firebird database.
 *
 * @created: Mmm dd, 2015
 *
 * @copyright: Copyright (c) 2015 Robert Zavalczki, distributed
 * under the terms and conditions of the Lesser GNU General
 * Public License version 2.1
 */

CREATE SEQUENCE SEQ_RESOURCE;
CREATE SEQUENCE SEQ_LITERAL;
CREATE SEQUENCE SEQ_BNODE;
CREATE SEQUENCE SEQ_TRIPLE;
CREATE SEQUENCE SEQ_CONTEXT;

CREATE TABLE Resource
(
    ID bigint NOT NULL,
    URI varchar(1024) NOT NULL,
    CONSTRAINT PK_RESOURCE PRIMARY KEY(ID),
    CONSTRAINT UQ_RESOURCE_URI UNIQUE (URI)
);

CREATE TABLE Context
(
    ID bigint NOT NULL,
    URI varchar(1024) NOT NULL,
    CONSTRAINT PK_CONTEXT PRIMARY KEY(ID),
    CONSTRAINT UQ_CONTEXT_URI UNIQUE (URI)
);

CREATE TABLE Literal
(
    ID bigint NOT NULL,
    VAL varchar(1250) NOT NULL,
    LANGUAGE varchar(16) DEFAULT NULL,
    DATATYPE BIGINT DEFAULT NULL,
    CONSTRAINT PK_LITERAL PRIMARY KEY(ID),
    CONSTRAINT FK_LITERAL_DATATYPE
        FOREIGN KEY (DATATYPE) REFERENCES Resource (ID),
    CONSTRAINT UQ_LITERAL_VAL UNIQUE (VAL, LANGUAGE, DATATYPE),
    CONSTRAINT CK_LITERAL_LANG_DT
        CHECK ((LANGUAGE IS NULL AND DATATYPE IS NULL) OR
               (LANGUAGE IS NOT NULL AND DATATYPE IS NULL) OR
               (LANGUAGE IS NULL AND DATATYPE IS NOT NULL))
);

CREATE TABLE Bnode
(
    ID bigint NOT NULL,
    NAME varchar(64) NOT NULL,
    CONSTRAINT PK_BNODE PRIMARY KEY(ID),
    CONSTRAINT UQ_BNODE_NAME UNIQUE (NAME)
);

CREATE TABLE Triple
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
        FOREIGN KEY (C_URI) REFERENCES Context (ID),
    -- CONSTRAINT UQ_TRIPLE
    --     UNIQUE (S_URI, S_BLANK, P_URI, O_URI, O_BLANK, O_LITERAL, C_URI),
    CONSTRAINT CK_TRIPLE_SUBJ
        CHECK ((S_URI IS NOT NULL AND S_BLANK IS NULL) OR
               (S_URI IS NULL AND S_BLANK IS NOT NULL)),
    CONSTRAINT CK_TRIPLE_OBJ
        CHECK ((O_URI IS NOT NULL AND O_BLANK IS NULL AND O_LITERAL IS NULL) OR
               (O_URI IS NULL AND O_BLANK IS NOT NULL AND O_LITERAL IS NULL) OR
               (O_URI IS NULL AND O_BLANK IS NULL AND O_LITERAL IS NOT NULL))
);

/* explicit indexes */
CREATE INDEX IDX_TRIPLE_S_URI ON TRIPLE (S_URI);
CREATE INDEX IDX_TRIPLE_S_BLANK ON TRIPLE (S_BLANK);
CREATE INDEX IDX_TRIPLE_P_URI ON TRIPLE (P_URI);
CREATE INDEX IDX_TRIPLE_O_URI ON TRIPLE (O_URI);
CREATE INDEX IDX_TRIPLE_O_BLANK ON TRIPLE (O_BLANK);
CREATE INDEX IDX_TRIPLE_O_LITERAL ON TRIPLE (O_LITERAL);
CREATE INDEX IDX_TRIPLE_C_URI ON TRIPLE (C_URI);


/* friendly view of the triples */
CREATE VIEW STATEMENTS_N3 as
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
LEFT JOIN CONTEXT c ON r.C_URI = c.ID;

/* unfriendly view of the triples */
CREATE VIEW STATEMENTS as
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
LEFT JOIN CONTEXT c ON r.C_URI = c.ID;

/* and another triples view */
CREATE VIEW STATEMENT_DETAILS (STID, SID, PID, OID, CID, SUBJ, PRED, OBJ)
AS SELECT r.ID as statement_id,
       '' || COALESCE(r.S_URI, 'N') || ',' || COALESCE(r.S_BLANK, 'N') || '' as sid,
       r.P_URI as pid,
       '' || COALESCE(r.O_URI, 'N') || ',' || COALESCE(r.O_BLANK, 'N') || ',' || COALESCE(r.O_LITERAL, 'N') || '' as oid,
       r.C_URI as cid,
       coalesce(rs.URI, bs.NAME) as subj,
       rp.URI as pred,
       coalesce(ro.URI,bo.NAME, lo.VAL || coalesce(('@' || lo.LANGUAGE), '') || coalesce('^' || lo.DATATYPE, '')) as obj
FROM TRIPLE r
LEFT JOIN RESOURCE rs ON r.S_URI = rs.ID
LEFT JOIN BNODE bs ON r.S_BLANK = bs.ID
JOIN RESOURCE rp ON r.P_URI = rp.id
LEFT JOIN RESOURCE ro ON r.O_URI = ro.ID
LEFT JOIN BNODE bo ON r.O_BLANK = bo.ID
LEFT JOIN LITERAL lo ON r.O_LITERAL = lo.ID
LEFT JOIN RESOURCE ldt ON lo.DATATYPE = ldt.ID
LEFT JOIN CONTEXT c ON r.C_URI = c.ID;

COMMIT;
