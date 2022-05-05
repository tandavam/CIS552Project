package com.database.Iterator;


import com.database.StatementParser.Evaluator;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;

import java.io.Reader;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


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
//        System.out.println(Arrays.toString(row));
        AndExpression abc = (AndExpression) condition;
        AndExpression abc_ = (AndExpression) abc.getLeftExpression();
        List<Boolean> conditions = new ArrayList<Boolean>();

//        System.out.println(((EqualsTo)abc_.getLeftExpression()).getLeftExpression());
        System.out.println("****");
        System.out.println(abc.getLeftExpression() instanceof AndExpression);
        System.out.println(evaluator.eval(((EqualsTo)abc_.getLeftExpression()).getRightExpression()).toRawString());
        System.out.println(evaluator.eval(((EqualsTo)abc_.getLeftExpression()).getLeftExpression()).toRawString());
        System.out.println((((BooleanValue) evaluator.eval(((EqualsTo)abc_.getLeftExpression()))).getValue()));
        System.out.println("****");
//        System.out.println(schema);
        while (true) {
//            System.out.println(this.condition);
            if (row == null) break;
//            System.out.println((((BooleanValue) evaluator.eval(condition)).getValue()));
//            for (String condtion:
//                 conditions) {
//
//            }
            if (((BooleanValue) evaluator.eval(condition)).getValue()) {
                System.out.println("****");
                System.out.println(((BooleanValue) evaluator.eval(condition)).getValue());
                System.out.println("****");
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
