package com.dbDesign.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;


public interface DB_Iterator {

    void reset();

    Object[] next() throws SQLException;

    Table getTable();
}
