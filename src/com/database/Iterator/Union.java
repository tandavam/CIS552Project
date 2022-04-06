package com.database.Iterator;

import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;

public class Union implements CrossProductInterface {
    final CrossProductInterface left;
    final CrossProductInterface right;
    boolean is_left_navigated = false;
    Object[] lout;

    public Union(CrossProductInterface left_node, CrossProductInterface right_node) {
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
