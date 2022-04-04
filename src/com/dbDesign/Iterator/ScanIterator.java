package com.dbDesign.Iterator;

import com.dbDesign.GlobalVariables;
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


public class ScanIterator implements DbIterator {
    final Table table;
    private final boolean full;
    File file;
    BufferedReader br = null;
    Iterator scan = null;
    private List<CSVRecord> data;

    public ScanIterator(File f, Table table) {
        this.file = f;
        this.table = table;
        this.full = false;
        reset();
    }

    public ScanIterator(File f, Table table, boolean full) {
        this.file = f;
        this.table = table;
        this.full = full;
        reset();
    }

    @Override
    public void reset() {
        try {
            br = new BufferedReader(new FileReader(file));
            CSVParser parser = new CSVParser(br, CSVFormat.DEFAULT.withDelimiter('|'));
            if (full) {
                if (data == null) data = parser.getRecords();
                scan = data.iterator();
            } else {
                scan = parser.iterator();
            }
        } catch (IOException e) {
            System.out.println("error");
        }
    }

    @Override
    public Object[] next() {
        if (!scan.hasNext())
            return null;
        CSVRecord line = (CSVRecord) scan.next();

        if (line == null)
            return null;
        Object[] row;
        ArrayList<String> dataType;
        row = new Object[line.size()];
        dataType = GlobalVariables.schema_store.get(table.getName().toUpperCase());
        int i = 0;
        while (i < line.size()) {
            if ("INT".equals(dataType.get(i).toUpperCase())) {
                row[i] = new LongValue(line.get(i));
            } else if ("DECIMAL".equals(dataType.get(i).toUpperCase()) || "DOUBLE".equals(dataType.get(i).toUpperCase())) {
                row[i] = new DoubleValue(line.get(i));
            } else if ("DATE".equals(dataType.get(i).toUpperCase())) {
                row[i] = new DateValue(line.get(i));
            } else if ("CHAR".equals(dataType.get(i).toUpperCase()) || "STRING".equals(dataType.get(i).toUpperCase()) || "VARCHAR".equals(dataType.get(i).toUpperCase())) {
                row[i] = new StringValue(line.get(i));
            } else {
                if (dataType.get(i).contains("CHAR")) {
                    row[i] = new StringValue(line.get(i));
                }
            }
            i++;
        }
        return row;

    }


    @Override
    public Table getTable() {
        return table;
    }


}
