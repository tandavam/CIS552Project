package com.database.Iterator;


import com.database.StatementParser.Evaluator;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Table;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;


public class Selection implements JoinInterface {

    final JoinInterface op;
    final HashMap<String, Integer> schema;
    final Expression condition;

    public Selection(JoinInterface input, Expression condition, HashMap<String, Integer> schema) {

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
        System.out.println(Arrays.toString(row));
        AndExpression abc = (AndExpression) condition;
        AndExpression abc_ = (AndExpression) abc.getLeftExpression();

        System.out.println(((EqualsTo)abc_.getLeftExpression()).getLeftExpression());
        System.out.println("****");
        System.out.println(evaluator.eval(((EqualsTo)abc_.getLeftExpression()).getRightExpression()).toRawString());
        System.out.println(evaluator.eval(((EqualsTo)abc_.getLeftExpression()).getLeftExpression()).toRawString());
        System.out.println((((BooleanValue) evaluator.eval(((EqualsTo)abc_.getLeftExpression()))).getValue()));
        System.out.println("****");
        System.out.println(schema);
        while (true) {
//            System.out.println(this.condition);
            if (row == null) break;
//            System.out.println((((BooleanValue) evaluator.eval(condition)).getValue()));
            if (((BooleanValue) evaluator.eval(condition)).getValue()) {
//                System.out.println("****");
//                System.out.println(row);
                return row;
            }

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
