/* delete all data */
DELETE FROM TRIPLE;
DELETE FROM BNODE;
DELETE FROM LITERAL;
DELETE FROM RESOURCE;
ALTER SEQUENCE SEQ_RESOURCE RESTART WITH 0;
ALTER SEQUENCE SEQ_LITERAL RESTART WITH 0;
ALTER SEQUENCE SEQ_BNODE RESTART WITH 0;
ALTER SEQUENCE SEQ_TRIPLE RESTART WITH 0;

COMMIT;
