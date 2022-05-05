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
        List<Boolean> flags = new ArrayList<>();

//        System.out.println(condition.toString());

//        System.out.println(Arrays.toString(row));
//        AndExpression abc = (AndExpression) condition;
//        AndExpression abc_ = (AndExpression) abc.getLeftExpression();
//        Expression abc_2 = (Expression) abc_.getLeftExpression();
//        BinaryExpression abc_1 = (BinaryExpression) abc.getRightExpression();
////
//        System.out.println(abc.toString());
//        System.out.println(abc_.toString());
//        System.out.println(abc_2.toString());
//        System.out.println(abc_1.toString());
//
////        System.out.println(((EqualsTo)abc_.getLeftExpression()).getLeftExpression());
//        System.out.println("****");
//        System.out.println(evaluator.eval(((EqualsTo)abc_.getLeftExpression()).getRightExpression()).toRawString());
//        System.out.println(evaluator.eval(((EqualsTo)abc_.getLeftExpression()).getLeftExpression()).toRawString());
//        System.out.println((((BooleanValue) evaluator.eval(((EqualsTo)abc_.getLeftExpression()))).getValue()));
//        System.out.println("****");

//        System.out.println(schema);
        while (true) {
            if (row == null) break;

            int numberOfConditions = condition.toString().split("AND", 0).length;
            if (numberOfConditions > 2){
                AndExpression temp = (AndExpression) condition;

                while(temp.getLeftExpression() instanceof AndExpression){
                    flags.add(((BooleanValue) evaluator.eval(temp.getLeftExpression())).getValue());

                    flags.add(((BooleanValue) evaluator.eval(temp.getRightExpression())).getValue());


                    try{
                        temp = (AndExpression) temp.getLeftExpression();
                    } catch (Exception e){
//                        row = op.next();
//                        evaluator.setTuple(row);
//                        continue;
                    }

                }
                flags.add(((BooleanValue) evaluator.eval(temp)).getValue());

//                System.out.println(flags.toString());
                Boolean flag = true;
                for(boolean b : flags){
//                    System.out.println(b);
                    if(!b){
                        flag = false;
                        break;
                    }
                }

//                for (int i = 0; i < numberOfConditions - 2; i++){
//                    flags.add(((BooleanValue) evaluator.eval(temp.getLeftExpression())).getValue());
//
//                    temp = (AndExpression) temp.getLeftExpression();
//                    flags.add(((BooleanValue) evaluator.eval(temp.getRightExpression())).getValue());
//                    for(boolean b : flags) if(!b) return null;
//                }
//                System.out.println(flags.toString());
                flags = new ArrayList<Boolean>();

                if (flag){
//                    System.out.println(condition.toString());
                    return row;
                }

            } else if (((BooleanValue) evaluator.eval(condition)).getValue()) {
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
