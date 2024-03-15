## Project: Acquiring and Processing Information on the World's Largest Banks

### Project tasks

#### Task 1:
Write a function `log_progress()` to log the progress of the code at different stages in a file c`ode_log.txt`. Use the list of log points provided to create log entries as every stage of the code.

#### Task 2:
Extract the tabular information from the given URL under the heading **'By market capitalization'** and save it to a dataframe.
- a. Inspect the webpage and identify the position and pattern of the tabular information in the HTML code
- b. Write the code for a function `extract()` to perform the required data extraction.
- c. Execute a function call to `extract()` to verify the output.

#### Task 3:
Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places, based on the exchange rate information shared as a CSV file.
- a. Write the code for a function `transform()` to perform the said task.
- b. Execute a function call to `transform()` and verify the output.

#### Task 4:
Load the transformed dataframe to an output CSV file. Write a function `load_to_csv()`, execute a function call and verify the output.

#### Task 5:
Load the transformed dataframe to an SQL database server as a table. Write a function `load_to_db()`, execute a function call and verify the output.

#### Task 6:
Run queries on the database table. Write a function `load_to_db()`, execute a given set of queries and verify the output.

#### Task 7:
Verify that the log entries have been completed at all stages by checking the contents of the file `code_log.txt`.


## Preliminaries: Installing libraries and downloading data
Before building the code, you need to install the required libraries.

The libraries needed for the code are:

- **requests** - The library used for accessing the information from the URL.
- **bs4** - The library containing the BeautifulSoup function used for webscraping.
- **pandas** - The library used for processing the extracted data, storing it in required formats, and communicating with the databases.
- **sqlite3** - The library required to create a database server connection.
- **numpy** - The library required for the mathematical rounding operations.
- **datetime** - The library containing the function datetime used for extracting the timestamp for logging purposes.


