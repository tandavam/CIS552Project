package com.database.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;


public interface JoinInterface {

    void reset();

    Object[] next() throws SQLException;

    Table getTable();
}
