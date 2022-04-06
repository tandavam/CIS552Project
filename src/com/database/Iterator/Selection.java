package com.database.Iterator;


import com.database.StatementParser.Evaluator;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.HashMap;


public class Selection implements CrossProductInterface {

    final CrossProductInterface op;
    final HashMap<String, Integer> schema;
    final Expression condition;

    public Selection(CrossProductInterface input, Expression condition, HashMap<String, Integer> schema) {

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
        Evaluator evaluator;
        evaluator = new Evaluator(schema, row);
        while (true) {
            if (row == null) break;
            if (((BooleanValue) evaluator.eval(condition)).getValue()) return row;
            row = op.next();
            evaluator.setTuple(row);
        }
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }
}