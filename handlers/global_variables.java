import java.io.File;

public class global_variables {
	
	public static File collection_location = new File("dataset/");
	public static Object[] createTuple(Object[] left_node, Object[] right_node) {
		
		if(left_node == null || right_node == null) {
			return null;
		}
		 Object[] new_tuple = new Object[left_node.length + right_node.length];
		 System.arraycopy(left_node, 0, new_tuple, 0, left_node.length);
		 System.arraycopy(right_node, 0, new_tuple, 0, right_node.length);
		 return new_tuple;
		
		
	}

}
