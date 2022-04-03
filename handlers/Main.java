import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import com.database.sql.SQL_Create_Table;


public class Main {
	
	public static void main(String[] args) {
		
		File sqlQueryFile = new File(args[0]);
		GlobalVariables.collection_location = new File(args[1]);
		String queryString;
		try {
			FileInputStream filePointer = new FileInputStream(sqlQueryFile);
			byte[] queryData = new byte[(int) sqlQueryFile.length()];
			filePointer.read(queryData);
			filePointer.close();
			queryString = new String(queryData, StandardCharsets.UTF_8);
			String[] listOfQueries = queryString.split(";");
			for(String s: listOfQueries) {
				if (!s.strip().equals("")) {
					Reader queryInput = new StringReader(s);
					CCJSqlParser queryParser = new CCJSqlParser(queryInput);
				}
			}
			
			
		} catch ( IOException e ) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
