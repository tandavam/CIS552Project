package com.dbDesign.Iterator;

import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.ArrayList;

public class Aggregate_Iterator implements DB_Iterator {
    final DB_Iterator oper;
    final ArrayList<Function> aggregator;
    final Table table;

    public Aggregate_Iterator(DB_Iterator oper, ArrayList<Function> aggregator, Table table) {
        this.oper = oper;
        this.aggregator = aggregator;
        this.table = table;
    }

    @Override
    public void reset() {
        oper.reset();
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] result = null;
        boolean finished = false;
        Object[] obj = new Object[0];
        obj = new Object[aggregator.size()];
        int i = 0;
        while (true) {
            if (i >= aggregator.size()) break;
            Object total;
            Object[] row = oper.next();
            if (row != null) {
                int count;
                count = 1;
                row = oper.next();
                if (row != null) {
                    do {
                        count++;
                        row = oper.next();
                    } while (row != null);
                }
                total = new LongValue(Integer.toString(count));
            } else {
                finished = true;
                break;
            }
            obj[i] = total;
            i++;
        }
        if (!finished) {
            result = obj;
        }
        return result;
    }

    @Override
    public Table getTable() {
        return table;
    }
}
