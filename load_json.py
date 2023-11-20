# This is a variable to hold the names of each column in the CSV
column_names = []
# Variable to tell the loop whether to add column names to the array
set_col_names = 1
# This is the array of elements which we will eventually write to the CSV
data_rows = []
# This is a temporary array used to create each row as we iterate through the records from the JSON file
data_row = []

for i in data:
    # If our row of data has elements, add it to our data_rows element
    if data_row:
        data_rows.append(data_row)

    # For each new row of data, we want to empty the array so that it doesn't contain any data from the previous row.
    # If we did not do this, then we would keep adding more data to one row rather than creating separate rows of data.
    data_row = []
    # If our column name array has elements, set the column names to FALSE/0
    if column_names:
        set_col_names = 0

    for k, v in i.items():
        # Only add column names to the array if the array of column names is empty
        if set_col_names:
            # Fill array of column names
            column_names.append(k)

        # Fill array containing rows of data
        data_row.append(v)

try:
    # Create a CSV file to publish to
    with open('DublinAirport090220.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        # Writing the fields
        writer.writerow(column_names)

        # Writing the data rows
        writer.writerows(data_rows)
except OSError as e:
    print('open() failed', e)
