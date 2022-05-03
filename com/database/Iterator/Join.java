package com.database.Iterator;

import com.database.GlobalVariables;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class Join implements JoinInterface {

    JoinInterface source, destination;
    final Table table;
    private final int size;
    private Object[] next;

    public Join(Table left_table, Table right_table, JoinInterface operator
                                ) throws SQLException {

        source = operator;
        String file = right_table.getName() + ".dat";
        file = GlobalVariables.collection_location.toString() + File.separator + file.toLowerCase();
        try {
            destination = new Scanner(new File(file), right_table, true);
        } catch (NullPointerException e) {
            System.out.println("Cross Pointer");
        }


        LinkedHashMap<String, Integer> new_schema = new LinkedHashMap<>();

        ArrayList<String> data_type = new ArrayList<>();
        String destination_table = left_table.getAlias() + "," + right_table.getAlias();
        this.table = new Table(destination_table);
        this.table.setAlias(destination_table);
        data_type = create_new_schema(new_schema, left_table, right_table, data_type);
        GlobalVariables.show_all_collections.put(destination_table, new_schema);
        GlobalVariables.database_schema.put(destination_table, data_type);
        next = source.next();
        size = new_schema.size();
    }

    @Override
    public void reset() {
        source.reset();
        destination.reset();
    }


    ArrayList<String> create_new_schema(HashMap<String, Integer> new_schema, Table left_collection, Table right_collection, ArrayList<String> data_type) {
        LinkedHashMap<String, Integer> old_schema = GlobalVariables.show_all_collections.get(left_collection.getAlias());
        data_type.addAll(GlobalVariables.database_schema.get(left_collection.getName()));
        int sizes = 0;
        for (String s : old_schema.keySet()) {
            new_schema.put(s, old_schema.get(s) + sizes);
        }
        sizes = new_schema.size();
        old_schema = GlobalVariables.show_all_collections.get(right_collection.getAlias());
        data_type.addAll(GlobalVariables.database_schema.get(right_collection.getName()));
        for (String col : old_schema.keySet()) {
            new_schema.put(col, old_schema.get(col) + sizes);
        }
        return data_type;
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] next = destination.next();
        if (next == null) {
            this.next = source.next();
            if (this.next == null)
                return null;
            destination.reset();
            next = destination.next();
        }
        Object[] temp = create_tuple(this.next, next);
//        System.out.println(Arrays.toString(temp));
        return temp;
    }


    public Object[] create_tuple(Object[] left, Object[] right) {
        Object[] new_collection = new Object[size];
        int index = 0;
        for (Object o : left) {
            new_collection[index] = o;
            index++;
        }
        for (Object o : right) {
            new_collection[index] = o;
            index++;
        }
        return new_collection;
    }

    @Override
    public Table getTable() {
        return this.table;
    }
}