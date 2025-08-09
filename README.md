Summary of Contributions:
Error Handling: Added robust error handling for database and CSV-related issues.
Batch Insertions: Improved efficiency by inserting data in batches.
Logging: Implemented logging for better traceability and debugging.
Environment Variables: Improved security by using environment variables for database credentials.
CSV Existence Check: Prevented crashes from missing CSV files by adding a check.
Missing Data Handling: Ensured proper handling of NaN values in DataFrame before SQL insertion.
Column Name Cleaning: Ensured column names are compatible with MySQL.
Table Creation: Continued using CREATE TABLE IF NOT EXISTS but with additional flexibility for future improvements.
