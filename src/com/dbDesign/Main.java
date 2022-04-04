package com.dbDesign;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.statement.Statement;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;


public class Main {

    public static void main(String[] args) {
        String str;
        try {
            File sqlQueryFile = new File(args[0]);
            GlobalVariables.collection_location = new File(args[1]);
            FileInputStream file_pointer = new FileInputStream(sqlQueryFile);
            byte[] data = new byte[(int) sqlQueryFile.length()];
            file_pointer.read(data);
            file_pointer.close();
            str = new String(data, StandardCharsets.UTF_8);
            String[] list_of_queries = str.split(";");
            for (String query : list_of_queries) {
                if (!query.strip().equals("")) {
                    Reader input = new StringReader(query);
                    CCJSqlParser parser = new CCJSqlParser(input);
                    try {
                        Statement statement = parser.Statement();
                        statement.accept(new QueryParser());
                    } catch (Exception e) {
                        System.out.println("SQL syntax error");
                        e.printStackTrace();
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
