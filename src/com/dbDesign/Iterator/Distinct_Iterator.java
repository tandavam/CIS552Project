package com.dbDesign.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class Distinct_Iterator implements DB_Iterator {

    private final DB_Iterator DB_Iterator;
    private final HashSet<List<Object>> buffer = new HashSet<>();

    public Distinct_Iterator(DB_Iterator DB_Iterator) {
        this.DB_Iterator = DB_Iterator;
    }

    @Override
    public void reset() {

    }

    @Override
    public Object[] next() throws SQLException {
        Object[] row;
        row = DB_Iterator.next();
        while (true) {
            if (row == null) break;
            if (!buffer.contains(Arrays.asList(row))) {
                buffer.add(Arrays.asList(row));
                return row;
            }
            row = DB_Iterator.next();
        }
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}