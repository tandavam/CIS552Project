package com.dbDesign.Iterator;

import com.dbDesign.GlobalVariables;
import net.sf.jsqlparser.schema.Table;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class CrossProductIterator implements DbIterator {

    DbIterator leftIterator, rightIterator;
    final Table table;
    private final int size;
    private Object[] temp1;

    public CrossProductIterator(DbIterator oper, Table righttable,
                                Table lefttable) throws SQLException {

        leftIterator = oper;
        String dataFileName = righttable.getName() + ".dat";
        dataFileName = GlobalVariables.collection_location.toString() + File.separator + dataFileName.toLowerCase();
        try {
            rightIterator = new ScanIterator(new File(dataFileName), righttable, true);
        } catch (NullPointerException e) {
            System.out.println("Null pointer exception in JoinOperator()");
        }


        LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();

        ArrayList<String> dataType = new ArrayList<>();
        String newTableName = lefttable.getAlias() + "," + righttable.getAlias();
        this.table = new Table(newTableName, newTableName);
        this.table.setAlias(newTableName);
        dataType = create_new_schema(newSchema, lefttable, righttable, dataType);
        GlobalVariables.list_tables.put(newTableName, newSchema);
        GlobalVariables.schema_store.put(newTableName, dataType);
        temp1 = leftIterator.next();
        size = newSchema.size();
    }

    @Override
    public void reset() {
        leftIterator.reset();
        rightIterator.reset();
    }


    ArrayList<String> create_new_schema(HashMap<String, Integer> newSchema, Table lefttable, Table righttable, ArrayList<String> dataType) {
        LinkedHashMap<String, Integer> oldschema = GlobalVariables.list_tables.get(lefttable.getAlias());
        dataType.addAll(GlobalVariables.schema_store.get(lefttable.getName()));
        int sizes = 0;
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        sizes = newSchema.size();
        oldschema = GlobalVariables.list_tables.get(righttable.getAlias());
        dataType.addAll(GlobalVariables.schema_store.get(righttable.getName()));
        for (String col : oldschema.keySet()) {
            newSchema.put(col, oldschema.get(col) + sizes);
        }
        return dataType;
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] temp2 = rightIterator.next();
        if (temp2 == null) {
            temp1 = leftIterator.next();
            if (temp1 == null)
                return null;
            rightIterator.reset();
            temp2 = rightIterator.next();
        }
        return create_row(temp1, temp2);
    }


    public Object[] create_row(Object[] left, Object[] right) {
        Object[] new_row = new Object[size];
        int index = 0;
        for (Object o : left) {
            new_row[index] = o;
            index++;
        }
        for (Object o : right) {
            new_row[index] = o;
            index++;
        }
        return new_row;
    }

    @Override
    public Table getTable() {
        return this.table;
    }
}