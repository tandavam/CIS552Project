package com.database;

import com.database.Iterator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
//import net.sf.jsqlparser.statement.select.Union;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Execute {


    public static JoinInterface select_tree(JoinInterface op, Expression where, Expression condition, List<SelectItem> list, Table table, boolean allColumns, ArrayList<Table> joins) throws SQLException {
        boolean ifagg = false;
        JoinInterface operator = op;
        GlobalVariables.attribute_used = new ArrayList<String>();
        var aggregator = new ArrayList<Function>();
        if (!allColumns) {
            for (Iterator<SelectItem> iterator = list.iterator(); iterator.hasNext(); ) {
                SelectItem item = iterator.next();
                if (!(item instanceof AllTableColumns)) {
                    SelectExpressionItem exp_item = (SelectExpressionItem) item;
                    if (exp_item.getExpression() instanceof Function) {
                        aggregator.add((Function) exp_item.getExpression());
                        ifagg = true;
                    }
                }
            }
        }
        if (joins != null && !joins.isEmpty()) {
            for (Table jointly : joins) {
                operator = new Join(table, jointly, operator);
                table = operator.getTable();
            }
            table = operator.getTable();
        }
        if (where != null)
            operator = new Selection(operator, where, GlobalVariables.show_all_collections.get(table.getAlias()));
        if (condition != null)
            operator = new Selection(operator, condition, GlobalVariables.show_all_collections.get(table.getAlias()));
        else
            operator = new Projection(operator, list, table, allColumns);
        return operator;
    }


    public static void print(JoinInterface input) throws SQLException {
        Object[] row = input.next();
        if (row != null) {
            do {
                int i;
                i = 0;
                while (i < row.length - 1) {
                    if (row[i] instanceof StringValue) {
                        System.out.print(((StringValue) row[i]).getNotExcapedValue() + "|");
                    } else
                        System.out.print(row[i] + "|");
                    i++;
                }
                if (row[i] instanceof StringValue) {
                    System.out.print(((StringValue) row[i]).getNotExcapedValue());
                } else
                    System.out.print(row[i]);
                System.out.println();
                row = input.next();
            } while (row != null);
        }
    }

    public static JoinInterface union_tree(JoinInterface current, JoinInterface operator) {
//        JoinInterface output = new Union(current, operator);
//        output = new Distinct(output);
        return null;
    }
}