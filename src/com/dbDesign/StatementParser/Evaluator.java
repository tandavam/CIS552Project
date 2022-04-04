package com.dbDesign.StatementParser;

import com.dbDesign.GlobalVariables;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;
import java.util.Iterator;


public class Evaluator extends Eval {

    private final HashMap<String, Integer> schema;
    private Object[] tuple;

    public Evaluator(HashMap<String, Integer> collection_name, Object[] tuple) {
        this.schema = collection_name;
        this.tuple = tuple;
    }

    public void setTuple(Object[] row) {
        this.tuple = row;
    }

    public PrimitiveValue eval(Column parent_column) {
        String table;
        int id = 0;
        if ((parent_column.getTable() != null) && (parent_column.getTable().getName() != null)) {
            table = parent_column.getTable().getName();
            if (!schema.containsKey(table + "." + parent_column.getColumnName()))
                id = changeAttribute(id, parent_column.getTable() + "." + parent_column.getColumnName());
            else id = schema.get(table + "." + parent_column.getColumnName());
        } else if (!GlobalVariables.rename.containsKey(parent_column.getColumnName()))
            id = changeAttribute(id, parent_column.getColumnName());
        else if (schema.containsKey(parent_column.getColumnName())) id = schema.get(parent_column.getColumnName());
        else if (schema.containsKey(GlobalVariables.rename.get(parent_column.getColumnName()).toString()))
            id = schema.get(GlobalVariables.rename.get(parent_column.getColumnName()).toString());
        else id = changeAttribute(id, parent_column.getColumnName());
        return (PrimitiveValue) tuple[id];
    }

    public int changeAttribute(int id, String column_name) {
        for (Iterator<String> iterator = schema.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(column_name)) id = schema.get(column);
        }
        return id;
    }
}
