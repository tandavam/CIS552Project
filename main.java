package com.database;

import com.database.sql.SQL_Create_Table;
import com.database.sql.SQL_Select;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

// You may modify this main file as long as you keep the logic
// This file is executed for each .sql file separately.

public class Main {
    public static void main(String[] args) {
        if (args.length > 1) {
            File file = new File(args[0]); //args[0] is absolute location of SQL query file
            String folderLocation = args[1]; //args[1] is absolute location of data folder - where the table data is stored

            // You need to derive the absolute location of the table from folderLocation + tableName + .csv or .dat

            // Read the whole SQL file and find all SQL queries. Query end determined by ";" and a query may be in multiple lines.

            //Assuming you put your queries in an ArrayList<String> called queries;

            for(int i = 0; i < queries.length; i++) {
              CCJSqlParser parser = new CCJSqlParser(queries.get(i));
              try {
                  Statement stmt = parser.Statement();
                  if (stmt instanceof Select) {
                      try {
                          SQL_Select query = new SQL_Select((Select) stmt);
                          query.execute();
                      } catch (Exception e) {
                          System.out.println("SQL error");
                      }
                  } else if (stmt instanceof CreateTable) {
                      SQL_Create_Table query = new SQL_Create_Table((CreateTable) stmt);
                      query.execute(); //Executing all create table queries will tell you the structure of the database and what to expect when you read the csv and dat files (columns and data types)
                  } else {
                      throw new ParseException("Only SELECT and CREATE TABLE statement is valid"); //$NON-NLS-1$
                  }
              } catch (Exception e) {
                  System.out.println("SQL syntax error"); //$NON-NLS-1$
                  e.printStackTrace();
              }
            }
        }

    }
}
