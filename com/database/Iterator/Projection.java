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
    ArrayList<SelectItem> to_keep;
    final HashMap<String, Integer> schema;
    final boolean allColumns;
    Object[] row;

    public Projection(JoinInterface op, List<SelectItem> p, Table table, boolean allColumns) {

        this.op = op;
        this.row = new Object[p.size()];
        this.to_keep = (ArrayList<SelectItem>) p;
        this.table = table;
        this.schema = GlobalVariables.show_all_collections.get(table.getAlias());
        this.allColumns = allColumns;
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public Object[] next() throws SQLException {
        Object[] temp = op.next();
        Evaluator eval = new Evaluator(schema, temp);

        int index = 0;
        if (temp == null) return null;
        if (allColumns) return temp;
        ArrayList<SelectItem> list = new ArrayList<>();
        for (int i = 0, toProjectSize = to_keep.size(); i < toProjectSize; i++) {
            checkIfAttributeExists(list, i);
        }
        to_keep = list;
        row = new Object[to_keep.size()];
        for (int i = 0; i < to_keep.size(); i++) {
            SelectItem f = to_keep.get(i);
            try {
                SelectExpressionItem e;
                e = (SelectExpressionItem) f;
                if (e.getExpression() instanceof Function) {
                    Expression x;
                    x = new Column(null, e.getExpression().toString());
                    row[index] = eval.eval(x);
                } else row[index] = eval.eval(e.getExpression());
            } catch (SQLException e1) {
                System.out.println("Projection Iterator");
            }
            index++;
        }
        return row;
    }

    private void checkIfAttributeExists(ArrayList<SelectItem> list, int i) {
        SelectItem f;
        f = to_keep.get(i);
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
