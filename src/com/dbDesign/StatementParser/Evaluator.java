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
        String collection;
        int index = 0;
        if ((parent_column.getTable().getName() != null) && (parent_column.getTable() != null)) {
            collection = parent_column.getTable().getName();
            if (!schema.containsKey(collection + "." + parent_column.getColumnName()))
                index = changeAttribute(index, parent_column.getTable() + "." + parent_column.getColumnName());
            else index = schema.get(collection + "." + parent_column.getColumnName());
        } else if (!GlobalVariables.rename.containsKey(parent_column.getColumnName()))
            index = changeAttribute(index, parent_column.getColumnName());
        else if (schema.containsKey(parent_column.getColumnName())) index = schema.get(parent_column.getColumnName());
        else if (schema.containsKey(GlobalVariables.rename.get(parent_column.getColumnName()).toString()))
            index = schema.get(GlobalVariables.rename.get(parent_column.getColumnName()).toString());
        else index = changeAttribute(index, parent_column.getColumnName());
        return (PrimitiveValue) tuple[index];
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
