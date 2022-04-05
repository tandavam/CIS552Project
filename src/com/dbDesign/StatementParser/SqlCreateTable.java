package com.dbDesign.StatementParser;

import com.dbDesign.GlobalVariables;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;


public class SqlCreateTable {
    private final CreateTable sql;

    public SqlCreateTable(CreateTable stmt) {
        this.sql = stmt;
    }

    public void getResult() {
        String collection_name = sql.getTable().getName();
        LinkedHashMap<String, Integer> fields = new LinkedHashMap<>();
        ArrayList<String> data_types = new ArrayList<>();
        if (!GlobalVariables.show_all_collections.containsKey(collection_name)) {
            List<ColumnDefinition> lists = sql.getColumnDefinitions();
            int i = 0;
            for (ColumnDefinition list : lists) {
                fields.put(collection_name + "." + list.getColumnName(), i);
                data_types.add(list.getColDataType().toString());
                i++;
            }
            GlobalVariables.show_all_collections.put(collection_name, fields);
            GlobalVariables.database_schema.put(collection_name, data_types);
        }

    }
}
