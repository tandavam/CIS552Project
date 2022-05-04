package com.database.Iterator;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.database.GlobalVariables;
import com.database.StatementParser.Evaluator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
public class Projection implements JoinInterface {

    final JoinInterface op;
    final Table table;
    ArrayList<SelectItem> retained_valued;
    final HashMap<String, Integer> table_schema;
    final boolean all_attributes;
    Object[] document;

    public Projection(JoinInterface op, List<SelectItem> p, Table table, boolean all_attributes) {

        this.op = op;
        this.document = new Object[p.size()];
        this.retained_valued = (ArrayList<SelectItem>) p;
        this.table = table;
        this.table_schema = GlobalVariables.show_all_collections.get(table.getAlias());
        this.all_attributes = all_attributes;
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] temp = op.next();
        Evaluator eval = new Evaluator(table_schema, temp);

        int index = 0;
        if (temp == null) return null;
        if (all_attributes) return temp;
        ArrayList<SelectItem> list = new ArrayList<>();
        for (int i = 0, toProjectSize = retained_valued.size(); i < toProjectSize; i++) {
            checkIfAttributeExists(list, i);
        }
        retained_valued = list;
        document = new Object[retained_valued.size()];
        for (int i = 0; i < retained_valued.size(); i++) {
            SelectItem f = retained_valued.get(i);
            try {
                SelectExpressionItem e;
                e = (SelectExpressionItem) f;
                if (e.getExpression() instanceof Function) {
                    Expression x;
                    x = new Column(null, e.getExpression().toString());
                    document[index] = eval.eval(x);
                } else document[index] = eval.eval(e.getExpression());
            } catch (SQLException e1) {
                System.out.println("Projection Iterator");
            }
            index++;
        }
        return document;
    }

    private void checkIfAttributeExists(ArrayList<SelectItem> list, int i) {
        SelectItem f;
        f = retained_valued.get(i);
        if (f instanceof AllTableColumns) {
            checkForAttributes(list, (AllTableColumns) f);
        } else {
            list.add(f);
        }
    }

    private void checkForAttributes(ArrayList<SelectItem> list, AllTableColumns f) {
        AllTableColumns a = f;
        Table tab = a.getTable();
        for (Iterator<String> iterator = GlobalVariables.show_all_collections.get(tab.getName()).keySet().iterator(); iterator.hasNext(); ) {
            aliasExpression(list, tab, iterator);
        }
    }

    private void aliasExpression(ArrayList<SelectItem> list, Table tab, Iterator<String> iterator) {
        String j;
        j = iterator.next();
        SelectExpressionItem expItem;
        expItem = new SelectExpressionItem();
        j = j.substring(j.indexOf(".") + 1);
        expItem.setAlias(j);
        expItem.setExpression(new Column(tab, j));
        list.add(expItem);
    }

    @Override
    public void reset() {}



}
