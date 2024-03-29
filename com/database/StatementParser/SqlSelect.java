package com.database.StatementParser;
import com.database.GlobalVariables;
import com.database.Execute;
import com.database.Iterator.JoinInterface;
import com.database.Iterator.Scanner;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
public class SqlSelect {
    private final Select sql;

    public SqlSelect(Select statement) {
        this.sql = statement;
    }

    public static void manage_renaming(SelectBody body) {


        ArrayList<SelectItem> selectItems = (ArrayList<SelectItem>) ((PlainSelect) body).getSelectItems();
//        System.out.println(selectItems);
        GlobalVariables.alias = new HashMap<>();
        for (SelectItem a : selectItems) {
//            if ((a instanceof AllTableColumns) || (a instanceof AllColumns))
//                return;
            SelectExpressionItem s = (SelectExpressionItem) a;
            System.out.println(selectItems);
            String alias = s.getAlias();
            if (alias == null) {
                s.setAlias(s.getExpression().toString());
            }
            GlobalVariables.alias.put(s.getAlias(), s.getExpression());
        }
    }

    public static void rename_table(Table collection) {
        if (collection.getAlias() == null) {
            collection.setAlias(collection.getName());
        }

        if (!GlobalVariables.show_all_collections.containsKey(collection.getAlias())) {
            LinkedHashMap<String, Integer> tempSchema = GlobalVariables.show_all_collections.get(collection.getName());
            LinkedHashMap<String, Integer> newSchema = new LinkedHashMap<>();
            for (String key : tempSchema.keySet()) {
                String[] temp = key.split("\\.");
                newSchema.put(collection.getAlias() + "." + temp[1], tempSchema.get(key));
            }
            GlobalVariables.show_all_collections.put(collection.getAlias(), newSchema);
        }
    }

    public static JoinInterface get_iterator(PlainSelect body) throws SQLException {
        Table t;
        JoinInterface op;
        int i = 0;
        ArrayList<Table> joins = new ArrayList<>();
        ArrayList<String> list_of_collections = new ArrayList<>();
        list_of_collections.add(((Table) body.getFromItem()).getName());
        Expression join_function = null;
        t = new Table();
        if (body.getJoins() != null) {
            for (Join join : body.getJoins()) {
                if (join.getOnExpression() != null) {
                    join_function = join.getOnExpression();
                }
                Table name = (Table) join.getRightItem();
//                System.out.println(name);
                if (list_of_collections.contains(name.getName())) {
                    if (name.getAlias() == null) {
                        name.setAlias(String.format("%d", i));
                    }
                    list_of_collections.add(name.getName());
                }
                rename_table(name);
                joins.add(name);
                i += 1;
            }
        }
        if (body.getFromItem() instanceof SubSelect) {
            create_schema(((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody()).getSelectItems(), t, ((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody()).getFromItem());
            op = get_iterator((PlainSelect) ((SubSelect) body.getFromItem()).getSelectBody());
            op = Execute.select_tree(op,
                    body.getWhere(), join_function, body.getSelectItems(), t,
                    false, joins
            );
        }
        else {
            SqlSelect.manage_renaming(body);
            op = execute_tree(body, joins, join_function);
        }
        return op;
    }

    private static JoinInterface execute_tree(PlainSelect body, ArrayList<Table> joins, Expression join_function) throws SQLException {
        JoinInterface op;
        boolean allCol;
        Table t;
        t = (Table) body.getFromItem();
        rename_table(t);
        allCol = ((body.getSelectItems().get(0) instanceof AllColumns));
        String tableFile = GlobalVariables.collection_location.toString() + File.separator + t.getName().toLowerCase() + ".dat";
        JoinInterface readOp = new Scanner(new File(tableFile), t);
        op = Execute.select_tree(readOp,
                body.getWhere(), join_function, body.getSelectItems(), t,
                allCol, joins
        );
        return op;
    }

    public static void create_schema(List<SelectItem> selectItems, Table collection, FromItem fromItem) {
        LinkedHashMap<String, Integer> schema = new LinkedHashMap<>();
        if ((selectItems.get(0) instanceof AllColumns) || (selectItems.get(0) instanceof AllTableColumns)) {
            Table table = (Table) fromItem;
            schema = (GlobalVariables.show_all_collections.get(table.getName()));
        } else {
            for (int i = 0; i < selectItems.size(); i++) {
                SelectExpressionItem alias = (SelectExpressionItem) selectItems.get(i);
                if (alias.getAlias() != null) {
                    schema.put(alias.getAlias(), i);
                } else {
                    schema.put(alias.getExpression().toString(), i);
                }
            }
        }
        GlobalVariables.show_all_collections.put(collection.getAlias(), schema);
    }

    public void output() throws SQLException {
        SelectBody body = sql.getSelectBody();
        JoinInterface current = null;
        if (body instanceof PlainSelect) {
            current = get_iterator((PlainSelect) body);
        }
        Execute.print(current);
    }
}
