package com.database;
import com.database.Iterator.Scanner;
import net.sf.jsqlparser.expression.Expression;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class GlobalVariables {
    public static File collection_location = new File("/");
    public static final HashMap<String, ArrayList<String>> database_schema = new HashMap<>(); // <age, Int>
    public static HashMap<String, Expression> rename = new HashMap<>();
    public static ArrayList<String> attribute_used = new ArrayList<>();
    public static final HashMap<String, LinkedHashMap<String, Integer>> show_all_collections = new HashMap<>(); // <Players:{(age, int), (name, str), (dob, datetime)}>
    public static LinkedHashMap<String, Scanner> Sub_Select = new LinkedHashMap<String, Scanner>();

}
