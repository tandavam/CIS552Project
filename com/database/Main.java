package com.database;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;

import java.io.*;
import java.nio.charset.StandardCharsets;


public class Main {

    public static void main(String[] args) throws FileNotFoundException {
        String str;
        File sqlQueryFile = new File(args[0]);
        GlobalVariables.collection_location = new File(args[1]);
        FileInputStream file_pointer = new FileInputStream(sqlQueryFile);
        try {
            byte[] data = new byte[(int) sqlQueryFile.length()];
            file_pointer.read(data);
            file_pointer.close();
            str = new String(data, StandardCharsets.UTF_8);
            String[] list_of_queries = str.split(";");
            for(int query = 0; query < list_of_queries.length - 1; query++){
                if (!list_of_queries[query].strip().equals("")) {
                    parse_query_string(list_of_queries[query]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        catch (NullPointerException e){
            e.printStackTrace();
        }

    }
    private static void parse_query_string(String queryString) throws ParseException {
        Reader input = new StringReader(queryString);
//        System.out.println(input);
        CCJSqlParser parser = new CCJSqlParser(input);
//        System.out.println(parser);
        Statement statement = parser.Statement();
//        System.out.println(statement);
        statement.accept(new QueryParser());
    }
}
