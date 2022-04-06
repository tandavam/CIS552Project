package com.database;

import com.database.StatementParser.SqlCreateTable;
import com.database.StatementParser.SqlSelect;
import net.sf.jsqlparser.statement.StatementVisitor;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.replace.Replace;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.truncate.Truncate;
import net.sf.jsqlparser.statement.update.Update;

public class QueryParser implements StatementVisitor {

    @Override
    public void visit(Select select) {
        try {
            new SqlSelect(select).get_result();
        } catch (Exception e) {
            System.out.println("SQL syntax error"); //$NON-NLS-1$
            e.printStackTrace();
        }
        System.out.println("==========================");

    }

    @Override
    public void visit(Delete delete) {

    }

    @Override
    public void visit(Update update) {

    }

    @Override
    public void visit(Insert insert) {

    }

    @Override
    public void visit(Replace replace) {

    }

    @Override
    public void visit(Drop drop) {

    }

    @Override
    public void visit(Truncate truncate) {

    }

    @Override
    public void visit(CreateTable createTable) {
        new SqlCreateTable( createTable).get_result();
    }
}
