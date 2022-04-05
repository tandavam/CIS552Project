package com.dbDesign.StatementParser;

import com.dbDesign.GlobalVariables;
import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

import java.util.HashMap;
import java.util.Iterator;


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
                index = changeAttribute(index, parent_column.getTable() + "." + parent_column.getColumnName());
            else index = collection_schema.get(collection + "." + parent_column.getColumnName());
        } else if (!GlobalVariables.rename.containsKey(parent_column.getColumnName()))
            index = changeAttribute(index, parent_column.getColumnName());
        else if (collection_schema.containsKey(parent_column.getColumnName())) index = collection_schema.get(parent_column.getColumnName());
        else if (collection_schema.containsKey(GlobalVariables.rename.get(parent_column.getColumnName()).toString()))
            index = collection_schema.get(GlobalVariables.rename.get(parent_column.getColumnName()).toString());
        else index = changeAttribute(index, parent_column.getColumnName());
        return (PrimitiveValue) tuple[index];
    }

    public int changeAttribute(int index, String column_name) {
        for (Iterator<String> iterator = collection_schema.keySet().iterator(); iterator.hasNext(); ) {
            String column = iterator.next();
            String x = column.substring(column.indexOf(".") + 1);
            if (x.equals(column_name)) index = collection_schema.get(column);
        }
        return index;
    }
}
