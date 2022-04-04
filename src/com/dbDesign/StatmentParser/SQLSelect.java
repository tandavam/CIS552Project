package com.dbDesign.StatmentParser;


import com.dbDesign.GlobalVariables;
import com.dbDesign.Execute;
import com.dbDesign.Iterator.DB_Iterator;
import com.dbDesign.Iterator.Scan_Iterator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class SQLSelect {
    private final Select sql;

    public SQLSelect(Select statement) {
        this.sql = statement;
    }

    public static void manage_renaming(SelectBody body) {


        ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) ((PlainSelect) body).getSelectItems();
        GlobalVariables.rename = new HashMap<>();
        for (SelectItem a : selectItems) {
            if ((a instanceof AllTableColumns) || (a instanceof AllColumns))
                return;
            SelectExpressionItem s = (SelectExpressionItem) a;
            String alias = s.getAlias();
            if (alias == null) {
                s.setAlias(s.getExpression().toString());
            }
            GlobalVariables.rename.put(s.getAlias(), s.getExpression());
        }
    }

    public static void rename_table(Table collection) {
        if (collection.getAlias() == null) {
            collection.setAlias(collection.getName());
        }

        if (!GlobalVariables.list_tables.containsKey(collection.getAlias())) {
            LinkedHashMap<String, Integer> tempSchema = GlobalVariables.list_tables.get(collection.getName());
            LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
            for (String key : tempSchema.keySet()) {
                String[] temp = key.split("\\.");
                newSchema.put(collection.getAlias() + "." + temp[1], tempSchema.get(key));
            }
            GlobalVariables.list_tables.put(collection.getAlias(), newSchema);
        }
    }

    public static DB_Iterator get_iterator(PlainSelect body) throws SQLException {
        Table t;
        DB_Iterator op;
        boolean allCol;
        int i;
        i = 0;
        ArrayList<Table> joins = new ArrayList<>();
        ArrayList<String> list_of_collections = new ArrayList<>();
        list_of_collections.add(((Table) body.getFromItem()).getName());
        Expression join_function = null;
        if (body.getJoins() != null) {
            for (Join join : body.getJoins()) {
                if (join.getOnExpression() != null) {
                    join_function = join.getOnExpression();
                }
                Table tx = (Table) join.getRightItem();
                if (list_of_collections.contains(tx.getName())) {
                    if (tx.getAlias() == null) {
                        tx.setAlias(String.format("%d", i));
                    }
                    list_of_collections.add(tx.getName());
                }
                rename_table(tx);
                joins.add(tx);
                i += 1;
            }
        }
        if (body.getFromItem() instanceof SubSelect) {

            t = new Table();
            if (body.getFromItem().getAlias() == null) {
                t.setName("SubQuery");
                t.setAlias("SubQuery");
            } else {
                t.setName(body.getFromItem().getAlias());
                t.setAlias(body.getFromItem().getAlias());
            }

            create_schema(((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody()).getSelectItems(), t, ((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody()).getFromItem());
            op = get_iterator((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody());
            op = Execute.select_tree(op,
                    body.getWhere(), join_function, body.getSelectItems(), t,
                    false, joins
            );
        } else {
            SQLSelect.manage_renaming(body);
            t = (Table) body.getFromItem();
            rename_table(t);
            allCol = ((body.getSelectItems().get(0) instanceof AllColumns));
            String tableFile = GlobalVariables.collection_location.toString() + File.separator + t.getName().toLowerCase() + ".dat";
            DB_Iterator readOp = new Scan_Iterator(new File(tableFile), t);
            op = Execute.select_tree(readOp,
                    body.getWhere(), join_function, body.getSelectItems(), t,
                    allCol, joins
            );
        }
        return op;
    }

    public static void create_schema(List<SelectItem> selectItems, Table collection, FromItem fromItem) {
        LinkedHashMap<String, Integer> dbSchema = new LinkedHashMap<>();
        if ((selectItems.get(0) instanceof AllColumns) || (selectItems.get(0) instanceof AllTableColumns)) {
            Table table = (Table) fromItem;
            dbSchema = (GlobalVariables.list_tables.get(table.getName()));
        } else {
            for (int i = 0; i < selectItems.size(); i++) {
                SelectExpressionItem abc = (SelectExpressionItem) selectItems.get(i);
                if (abc.getAlias() != null) {
                    dbSchema.put(abc.getAlias(), i);
                } else {
                    dbSchema.put(abc.getExpression().toString(), i);
                }
            }
        }
        GlobalVariables.list_tables.put(collection.getAlias(), dbSchema);
    }

    public void getResult() throws SQLException {
        SelectBody body = sql.getSelectBody();
        DB_Iterator current = null;
        if (body instanceof PlainSelect) {
            current = get_iterator((PlainSelect) body);
        } else if (body instanceof Union) {
            List<PlainSelect> plainSelects = ((Union) body).getPlainSelects();
            current = get_iterator(plainSelects.get(0));
            for (PlainSelect i : plainSelects.subList(1, plainSelects.size())) {
                current = Execute.union_tree(current, get_iterator(i));
            }
        }
        Execute.print(current);
    }
}