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


    public static DB_Iterator select_tree(DB_Iterator op, Expression where, Expression condition, List<SelectItem> list, Table table, boolean allColumns, ArrayList<Table> joins) throws SQLException {
        boolean ifagg = false;
        DB_Iterator oper = op;
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
                oper = new Cross_Product_Iterator(oper, jointly, table);
                table = oper.getTable();
            }
            table = oper.getTable();
        }
        if (where != null)
            oper = new Selection_Iterator(oper, where, GlobalVariables.list_tables.get(table.getAlias()));
        if (condition != null)
            oper = new Selection_Iterator(oper, condition, GlobalVariables.list_tables.get(table.getAlias()));
        if (ifagg)
            oper = new Aggregate_Iterator(oper, aggregator, table);
        else
            oper = new Projection_Iterator(oper, list, table, allColumns);
        return oper;
    }


    public static void print(DB_Iterator input) throws SQLException {
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

    public static DB_Iterator union_tree(DB_Iterator current, DB_Iterator operator) {
        DB_Iterator output = new Union_Iterator(current, operator);
        output = new Distinct_Iterator(output);
        return output;
    }
}
