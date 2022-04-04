package com.dbDesign.Iterator;


import com.dbDesign.StatementParser.Evaluator;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.HashMap;


public class Selection_Iterator implements DB_Iterator {

    final DB_Iterator op;
    final HashMap<String, Integer> schema;
    final Expression condition;

    public Selection_Iterator(DB_Iterator input, Expression condition, HashMap<String, Integer> schema) {

        this.op = input;
        this.schema = schema;
        this.condition = condition;

    }

    @Override
    public void reset() {
        op.reset();
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] row;
        row = op.next();
        Evaluator eval;
        eval = new Evaluator(schema, row);
        while (true) {
            if (row == null) break;
            if (((BooleanValue) eval.eval(condition)).getValue()) return row;
            row = op.next();
            eval.setTuple(row);
        }
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}
