import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Random;

public class Main {

    private static final String DB_URL_NO_DB = "jdbc:mysql://localhost:3306/?serverTimezone=UTC";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/mock_dataDB?serverTimezone=UTC";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "rootpassword";

    public static void main(String[] args) {
        // Step 1: Create database if it doesn't exist
        try (Connection connectionNoDB = DriverManager.getConnection(DB_URL_NO_DB, DB_USER, DB_PASSWORD);
             Statement stmt = connectionNoDB.createStatement()) {

            String createDatabaseSQL = "CREATE DATABASE IF NOT EXISTS mock_dataDB";
            stmt.executeUpdate(createDatabaseSQL);
            System.out.println("Database 'mock_dataDB' created successfully or already exists...");

        } catch (SQLException e) {
            e.printStackTrace();
            return;
        }

        // Step 2: Connect to the created database and create table if it doesn't exist
        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {

            String createTableSQL = "CREATE TABLE IF NOT EXISTS resource_data " +
                    "(id INT AUTO_INCREMENT PRIMARY KEY, " +
                    " resource_name VARCHAR(255), " +
                    " resource_group VARCHAR(255), " +
                    " resource_location VARCHAR(255), " +
                    " account_sku_name VARCHAR(255), " +
                    " access_tier VARCHAR(255), " +
                    " used_capacity_b_avg DOUBLE, " +
                    " cpu_util_pct_avg DOUBLE, " +
                    " timestamp_utc_s TIMESTAMP)";
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(createTableSQL);
            System.out.println("Table 'resource_data' created successfully or already exists...");

            // Prepare the SQL insert statement
            String sql = "INSERT INTO resource_data (resource_name, resource_group, resource_location, account_sku_name, access_tier, used_capacity_b_avg, cpu_util_pct_avg, timestamp_utc_s) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);

            // Start time for the first record
            LocalDateTime startTime = LocalDateTime.now();

            // Number of batches to generate (10 records every 10 minutes)
            for (int batch = 0; batch < 10; batch++) {
                // Generate 10 records for each batch (10 seconds apart)
                for (int i = 0; i < 10; i++) {
                    // Generate random data and add to prepared statement
                    generateRandomData(preparedStatement, startTime.plusSeconds(i * 10));

                    // Execute the prepared statement to insert the data
                    preparedStatement.executeUpdate();

                    // Print comment when data is pushed
                    System.out.println("Record " + (batch * 10 + i + 1) + " inserted at " + Timestamp.valueOf(startTime.plusSeconds(i * 10)));
                }

                // Print comment after completing each batch
                System.out.println("Batch " + (batch + 1) + " of 10 records pushed successfully.");

                // Sleep for 10 minutes before generating the next batch
                Thread.sleep(10 * 60 * 1000);
            }
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void generateRandomData(PreparedStatement preparedStatement, LocalDateTime timestamp)
            throws SQLException {
        Random random = new Random();

        // Sample data arrays
        String[] resourceNames = {"Resource1", "Resource2", "Resource3"};
        String[] resourceGroups = {"GroupA", "GroupB", "GroupC"};
        String[] locations = {"US-East", "EU-West", "AP-South"};
        String[] skuNames = {"Standard", "Premium", "Basic"};
        String[] accessTiers = {"Hot", "Cool", "Archive"};

        // Randomly select values
        String resourceName = resourceNames[random.nextInt(resourceNames.length)];
        String resourceGroup = resourceGroups[random.nextInt(resourceGroups.length)];
        String location = locations[random.nextInt(locations.length)];
        String skuName = skuNames[random.nextInt(skuNames.length)];
        String accessTier = accessTiers[random.nextInt(accessTiers.length)];
        double usedCapacityAvg = 100 + (5000 - 100) * random.nextDouble(); // Random capacity between 100 and 5000
        double cpuUtilPctAvg = random.nextDouble() * 100; // Random CPU utilization between 0% and 100%

        // Set values in the prepared statement
        preparedStatement.setString(1, resourceName);
        preparedStatement.setString(2, resourceGroup);
        preparedStatement.setString(3, location);
        preparedStatement.setString(4, skuName);
        preparedStatement.setString(5, accessTier);
        preparedStatement.setDouble(6, usedCapacityAvg);
        preparedStatement.setDouble(7, cpuUtilPctAvg);
        preparedStatement.setTimestamp(8, Timestamp.valueOf(timestamp));
    }
}
