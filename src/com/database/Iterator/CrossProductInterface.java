package com.database.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;


public interface CrossProductInterface {

    void reset();

    Object[] next() throws SQLException;

    Table getTable();
}
