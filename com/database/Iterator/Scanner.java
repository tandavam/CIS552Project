package com.database.Iterator;

import com.database.GlobalVariables;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.schema.Table;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class Scanner implements JoinInterface {
    final Table table;
    private final boolean complete;
    File file;
    BufferedReader br = null;
    Iterator iter = null;
    private List<CSVRecord> records;

    public Scanner(File f, Table table) {
        this.file = f;
        this.table = table;
        this.complete = false;
        reset();
    }

    public Scanner(File f, Table table, boolean complete) {
        this.file = f;
        this.table = table;
        this.complete = complete;
        reset();
    }

    @Override
    public void reset() {
        try {
            br = new BufferedReader(new FileReader(file));
            CSVParser parser = new CSVParser(br, CSVFormat.DEFAULT.withDelimiter('|'));
            if (complete) {
                if (records == null) records = parser.getRecords();
                iter = records.iterator();
            } else {
                iter = parser.iterator();
            }
        } catch (IOException e) {
            System.out.println("Scanner Error");
        }
    }

    @Override
    public Object[] next() {
        int index = 0;
        if (!iter.hasNext())
            return null;
        CSVRecord line = (CSVRecord) iter.next();

        if (line == null)
            return null;
        Object[] tuple;
        ArrayList<String> dataType;
        tuple = new Object[line.size()];
        dataType = GlobalVariables.database_schema.get(table.getName().toUpperCase());

        while (index < line.size()) {
            if ("STRING".equalsIgnoreCase(dataType.get(index)) || "VARCHAR".equalsIgnoreCase(dataType.get(index)) || "CHAR".equalsIgnoreCase(dataType.get(index))) {
                tuple[index] = new StringValue(line.get(index));
            }
            else  if ("INT".equalsIgnoreCase(dataType.get(index))) {
                tuple[index] = new LongValue(line.get(index));
            }
            else if ("DATE".equalsIgnoreCase(dataType.get(index))) {
                tuple[index] = new DateValue(line.get(index));
            }
            else if ("DECIMAL".equalsIgnoreCase(dataType.get(index)) || "DOUBLE".equalsIgnoreCase(dataType.get(index))) {
                tuple[index] = new DoubleValue(line.get(index));
            }
            else {
                if (dataType.get(index).contains("CHAR")) {
                    tuple[index] = new StringValue(line.get(index));
                }
            }
            index++;
        }
        return tuple;

    }


    @Override
    public Table getTable() {
        return table;
    }


}
