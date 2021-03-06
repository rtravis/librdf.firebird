
##tmpl='''{
##	// SELECT_TRIPLE_%d
##	"%s",
##	nullptr,
##	%d, 1
##},'''
##
##queries=[
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI=?",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI IS NULL",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI=?",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI IS NULL",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI=?",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_URI=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI IS NULL",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI=?",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_URI=? AND r.C_URI IS NULL",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI=?",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_BLANK=? AND r.C_URI IS NULL",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI=?",
##		"SELECT r.ID FROM TRIPLE r WHERE r.S_BLANK=? AND r.P_URI=? AND r.O_LITERAL=? AND r.C_URI IS NULL"
##]
##
##
##for idx, q in enumerate(queries):
##    s = tmpl % (idx, q, 3 if idx % 2 else 4)
##    print(s)
##
##for idx, q in enumerate(queries):
##    #SELECT_TRIPLE_%d
##    print('''	SELECT_TRIPLE_%d,''' % idx)


queries = """
CREATE SEQUENCE SEQ_RESOURCE;
CREATE SEQUENCE SEQ_LITERAL;
CREATE SEQUENCE SEQ_BNODE;
CREATE SEQUENCE SEQ_TRIPLE;

CREATE TABLE Resource
(
    ID bigint NOT NULL,
    URI varchar(255) NOT NULL,
    CONSTRAINT PK_RESOURCE PRIMARY KEY(ID),
    CONSTRAINT UQ_RESOURCE_URI UNIQUE (URI)
);

CREATE TABLE Literal
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
);

CREATE TABLE Bnode
(
    ID bigint NOT NULL,
    NAME varchar(40) NOT NULL,
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
);

CREATE INDEX IDX_TRIPLE_S_URI ON TRIPLE (S_URI);
CREATE INDEX IDX_TRIPLE_S_BLANK ON TRIPLE (S_BLANK);
CREATE INDEX IDX_TRIPLE_P_URI ON TRIPLE (P_URI);
CREATE INDEX IDX_TRIPLE_O_URI ON TRIPLE (O_URI);
CREATE INDEX IDX_TRIPLE_O_BLANK ON TRIPLE (O_BLANK);
CREATE INDEX IDX_TRIPLE_O_LITERAL ON TRIPLE (O_LITERAL);
CREATE INDEX IDX_TRIPLE_C_URI ON TRIPLE (C_URI);

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
LEFT JOIN RESOURCE c ON r.C_URI = c.ID;

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
LEFT JOIN RESOURCE c ON r.C_URI = c.ID;

"""

for i in queries.split(';'):
    i = i.strip()
    if not i:
        continue
    tp, name = i.split()[1:3]
    sql = i
    str = '''
{
    "%s",
    "%s",
    R"(%s)"
},
''' % (name, tp, sql)
    print tp
