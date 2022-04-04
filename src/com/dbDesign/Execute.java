package com.dbDesign;

import com.dbDesign.Iterator.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Execute {


    public static DbIterator select_tree(DbIterator op, Expression where, Expression condition, List<SelectItem> list, Table table, boolean allColumns, ArrayList<Table> joins) throws SQLException {
        boolean ifagg = false;
        DbIterator operator = op;
        GlobalVariables.column_used = new ArrayList<String>();
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
                operator = new CrossProductIterator(operator, jointly, table);
                table = operator.getTable();
            }
            table = operator.getTable();
        }
        if (where != null)
            operator = new SelectionIterator(operator, where, GlobalVariables.list_tables.get(table.getAlias()));
        if (condition != null)
            operator = new SelectionIterator(operator, condition, GlobalVariables.list_tables.get(table.getAlias()));
        if (ifagg)
            operator = new AggregateIterator(operator, aggregator, table);
        else
            operator = new ProjectionIterator(operator, list, table, allColumns);
        return operator;
    }


    public static void print(DbIterator input) throws SQLException {
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

    public static DbIterator union_tree(DbIterator current, DbIterator operator) {
        DbIterator output = new UnionIterator(current, operator);
        output = new DistinctIterator(output);
        return output;
    }
}
