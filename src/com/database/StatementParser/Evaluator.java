package com.database.StatementParser;

import com.database.GlobalVariables;
import com.database.Iterator.Scanner;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

import java.io.*;
import java.sql.SQLException;
import java.util.*;


public class Evaluator extends Eval {

    private final HashMap<String, Integer> collection_schema;
    private Object[] tuple;

    public Evaluator(HashMap<String, Integer> collection_name, Object[] tuple) {
        this.collection_schema = collection_name;
        this.tuple = tuple;
    }

    public void setTuple(Object[] row) {
        this.tuple = row;
    }

    public PrimitiveValue eval(Column parent_column) {
        String collection;
        int index = 0;
        if ((parent_column.getTable().getName() != null) && (parent_column.getTable() != null)) {
            collection = parent_column.getTable().getName();
            if (!collection_schema.containsKey(collection + "." + parent_column.getColumnName()))
                index = change_attribute(index, parent_column.getTable() + "." + parent_column.getColumnName());
            else index = collection_schema.get(collection + "." + parent_column.getColumnName());
        } else if (!GlobalVariables.rename.containsKey(parent_column.getColumnName()))
            index = change_attribute(index, parent_column.getColumnName());
        else if (collection_schema.containsKey(parent_column.getColumnName())) index = collection_schema.get(parent_column.getColumnName());
        else if (collection_schema.containsKey(GlobalVariables.rename.get(parent_column.getColumnName()).toString()))
            index = collection_schema.get(GlobalVariables.rename.get(parent_column.getColumnName()).toString());
        else index = change_attribute(index, parent_column.getColumnName());
        return (PrimitiveValue) tuple[index];
    }

//    public PrimitiveValue eval(InExpression inExpression) throws SQLException {
//        ItemsList i = inExpression.getItemsList();
//        Expression left = inExpression.getLeftExpression();
//        if (i instanceof ExpressionList) {
//            for (var c : ((ExpressionList) i).getExpressions()) {
//                if (eval(new EqualsTo(left, c)).toBool()) {
//                    return BooleanValue.TRUE;
//                }
//            }
//            return BooleanValue.FALSE;
//        } else {
//            Scanner scan = null;
//            if (GlobalVariables.Sub_Select.get(i.toString()) == null) {
//                Set<String> abc = GlobalVariables.column_used;
//                Organizer extractor = new Organizer(((SubSelect) i).getSelectBody());
//                ((SubSelect) i).getSelectBody().accept(extractor);
//                GlobalVariables.column_used = extractor.columns;
//                Build_Tree Build_Tree = new Build_Tree(((SubSelect) i).getSelectBody());
//                RA_Tree current = Build_Tree.getRoot();
//                RA_Tree distinctTree = new Distinct_Node(current);
//                current.setParent(distinctTree);
//                current = distinctTree;
//                Optimize.selectionpushdown(current);
//                DB_Iterator itrator = current.get_iterator();
//                FileWriter fileWriter = null;
//                BufferedWriter bufferedWriter = null;
//                PrintWriter printWriter = null;
//                String newtable = GlobalVariables.table_location.toString() + File.separator + "temp.dat";
//                new File(newtable).delete();
//                ArrayList<String> datatype = new ArrayList<>();
//                try {
//                    fileWriter = new FileWriter(newtable, true);
//                    bufferedWriter = new BufferedWriter(fileWriter);
//                    printWriter = new PrintWriter(bufferedWriter);
//                    Object[] row = itrator.next();
//
//                    if (row[0] instanceof LongValue) {
//                        datatype.add("INT");
//                    } else if (row[0] instanceof DoubleValue) {
//                        datatype.add("DOUBLE");
//                    } else if (row[0] instanceof DateValue) {
//                        datatype.add("DATE");
//                    } else {
//                        datatype.add("STRING");
//                    }
//                    while (row != null) {
//                        printWriter.println(row[0]);
//                        row = itrator.next();
//                    }
//                } catch (IOException e) {
//                    e.printStackTrace();
//                } finally {
//                    try {
//                        Objects.requireNonNull(printWriter).close();
//                        Objects.requireNonNull(bufferedWriter).close();
//                        Objects.requireNonNull(fileWriter).close();
//                    } catch (IOException e) {
//                        e.printStackTrace();
//                    }
//                }
//                LinkedHashMap<String, Integer> schema = new LinkedHashMap<>();
//                schema.put(left.toString(), 0);
//                GlobalVariables.list_tables.put("TEMP", schema);
//                GlobalVariables.schema_store.put("TEMP", datatype);
//                scan = new Scan_Iterator(new File(newtable), "TEMP", schema);
//                GlobalVariables.Sub_Select.put(i.toString(), scan);
//                GlobalVariables.column_used = abc;
//            } else {
//                scan = GlobalVariables.Sub_Select.get(i.toString());
//                scan.reset();
//            }
//
//            Object[] row = scan.next();
//            while (row != null) {
//                if (eval(new EqualsTo((PrimitiveValue) row[0], left)).toBool()) {
//                    return BooleanValue.TRUE;
//                }
//                row = scan.next();
//            }
//            return BooleanValue.FALSE;
//        }
//    }

    public int change_attribute(int index, String column_name) {
        for (Iterator<String> iterator = collection_schema.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(column_name)) index = collection_schema.get(column);
        }
        return index;
    }
}
