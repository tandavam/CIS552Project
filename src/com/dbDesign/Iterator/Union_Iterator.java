package com.dbDesign.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;

public class Union_Iterator implements DB_Iterator {
    final DB_Iterator left;
    final DB_Iterator right;
    boolean is_left_navigated = false;

    public Union_Iterator(DB_Iterator left_node, DB_Iterator right_node) {
        this.left = left_node;
        this.right = right_node;
    }

    @Override
    public void reset() {
        left.reset();
        right.reset();
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] lout;
        if (!is_left_navigated)
            lout = left.next();
        else
            lout = null;
        if (lout == null) {
            is_left_navigated = true;
            return right.next();
        } else {
            return lout;
        }
    }

    @Override
    public Table getTable() {
        return null;
    }
}

