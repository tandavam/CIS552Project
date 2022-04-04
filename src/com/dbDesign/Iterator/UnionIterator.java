package com.dbDesign.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;

public class UnionIterator implements DbIterator {
    final DbIterator left;
    final DbIterator right;
    boolean is_left_navigated = false;

    public UnionIterator(DbIterator left_node, DbIterator right_node) {
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

