package com.dbDesign.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;


public interface Db {

    void reset();

    Object[] next() throws SQLException;

    Table getTable();
}
