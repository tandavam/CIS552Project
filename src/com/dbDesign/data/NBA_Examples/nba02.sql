CREATE TABLE PLAYERS(ID string, FIRSTNAME string, LASTNAME string, FIRSTSEASON int, LASTSEASON int, WEIGHT int, BIRTHDATE date);

SELECT FIRSTSEASON, COUNT(ID) 
FROM PLAYERS 
GROUP BY FIRSTSEASON;