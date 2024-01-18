package kubecluster.org;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsDemo {

    public static void main(String[] args) {
        // Create a Hadoop configuration object, which will load the configuration files from the classpath
        Configuration conf = new Configuration();

        // Optionally, set the HDFS namenode address directly
        // conf.set("fs.defaultFS", "hdfs://namenode_host:8020");

        try {
            // Get the FileSystem object for HDFS
            FileSystem fs = FileSystem.get(conf);

            // Define a path in HDFS to check for existence
            Path path = new Path("/path/to/test");

            // Check if the path exists in HDFS
            if (fs.exists(path)) {
                System.out.println("File or directory exists: " + path);
                return;
            }
            System.out.println("File or directory does not exist: " + path);

            // Try to create a new file to test the write operation
            boolean fileCreated = fs.createNewFile(path);
            if (fileCreated) {
                System.out.println("File created successfully: " + path);
            } else {
                System.out.println("Failed to create file: " + path);
            }
        } catch (Exception e) {
            // Print the stack trace to the console in case of an exception
            e.printStackTrace();
        }
    }
}
