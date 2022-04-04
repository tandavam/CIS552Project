package com.dbDesign;

import net.sf.jsqlparser.expression.Expression;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class GlobalVariables {
    public static final HashMap<String, ArrayList<String>> schema_store = new HashMap<>();
    public static final HashMap<String, LinkedHashMap<String, Integer>> list_tables = new HashMap<>();
    public static ArrayList<String> column_used = new ArrayList<>();
    public static File collection_location = new File("/");
    public static HashMap<String, Expression> rename = new HashMap<>();

}
