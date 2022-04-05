CREATE TABLE PLAYERS(ID string, FIRSTNAME string, LASTNAME string, FIRSTSEASON int, LASTSEASON int, WEIGHT int, BIRTHDATE date);

SELECT P1.*, P2.* 
FROM PLAYERS P1, PLAYERS P2 
WHERE (P1.FIRSTSEASON<P2.FIRSTSEASON AND P1.LASTSEASON>P2.LASTSEASON);