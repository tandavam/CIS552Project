package com.dbDesign.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;


public interface cross_product_interface {

    void reset();

    Object[] next() throws SQLException;

    Table getTable();
}
