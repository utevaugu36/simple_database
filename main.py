import os
import re


class CsvProcessor:
    """
    Process csv data. Provides encoding, decoding.
    """

    def __init__(self, cols):
        """
        Init
        :param cols: columns of database:list
        """
        self.cols = cols

    def decode(self, input_data):
        """
        Decode data from csv format to list of dicts
        :param input_data: raw_csv_data:string
        :return: decoded_data:list
        """
        # Create the template for saving data
        db = list()
        # Process data line by line
        for line in input_data.split("\n"):
            # If line in csv file is a comment, do not process it
            if line.startswith("#"):
                continue
            spl_line = line.split(";")
            # Create the template for saving current line row data
            db_line = {}
            for i in range(len(spl_line)):
                # Process column in row
                db_line[self.cols[i]] = spl_line[i]
            # Append line to database
            db.append(db_line)

        return db

    def encode(self, input_data, append_cols_comm=True):
        """
        Encode data from list of dicts to csv format
        :param input_data:list
        :param append_cols_comm:boolean
        :return: csv_data:string
        """
        # Create the template for saving data
        csv_result = ""
        if append_cols_comm:
            csv_result += "#" + ';'.join(self.cols) + "\n"
        # Process input line by line
        for line in input_data:
            # Create the template variable for line
            csv_line = ""
            # Process columns keys
            for key in self.cols:
                # Process if key exists in line of raw data
                if key in line:
                    # Append it
                    csv_line += ";" + line[key]
                else:
                    # Empty line in table
                    csv_line += ";"
            # Append line to the result
            csv_result += csv_line[1:] + "\n"

        # Return result without last breakline
        return csv_result[:-1]


# This module will process database working, such as reading, writing, saving, etc.
class Database:
    """
    Provides database working functions like reading, writing, saving etc.
    """

    def __init__(self, path, cols='auto', auto_save=False, read_before_operations=False):
        """
        Init
        :param path: relative path to future database:string
        :param cols: columns of future database:list
        :param auto_save=False: save db after any operation:boolean
        :param read_before_operations=False: read db from file before any operation:boolean
        """
        # Find the path, where script is located
        root = os.path.dirname(os.path.abspath(__file__)) + "/"
        # Save *args to self for interacting in other functions
        # Adding 'root', because if we are starting script not from this directory, it will not access right file
        self.path = root + path
        self.cols = cols
        if cols == 'auto':
            with open(self.path, 'r') as f:
                fline = f.read().split('\n')[0]
                if not fline.startswith('#'):
                    # There are no cols in file, just skip, and replace them with empty cols
                    self.cols = []

                self.cols = fline.replace('#', '').split(';')
        # Save config from *args to self vars
        self.auto_save = auto_save
        self.read_before_operations = read_before_operations
        # Create database object, where we will process data when it's not in .csv file
        self.db = dict()
        # Create object of class CSV_processor and save it to process csv data
        self.csv = CsvProcessor(self.cols)

        # Let's check whether database is already created, and try to read it
        if os.path.isfile(path):
            self.read()

    def read(self):
        """
        Reads database from file, provided in __init__ path argument, and stores it in self.db.
        :return: success:boolean
        """

        # Open file, that contains necessary data
        with open(self.path, 'r') as f:
            # Try to decode read data
            try:
                self.db = self.csv.decode(f.read())
            except FileNotFoundError:
                # File doesn't exist, through an error
                self.db = []
                return False

            # Success
            return True

    def save(self):
        """
        Saves database to file, provided in __init__ path argument in csv format.
        :return: success:boolean
        """

        # Open file for saving result
        with open(self.path, 'w') as f:
            # Try to encode and save data
            if f.write(self.csv.encode(self.db)):
                # Success
                return True
            else:
                # Some error found
                return False

    def update_row(self, row_id, col, data):
        """
        Updates data in row with id row_id and column col with data
        :param row_id: row_id:int
        :param col: column_name:string
        :param data: data:auto
        :return: success:boolean
        """
        # Read db if needed (view __init__ docstring for more info)
        if self.read_before_operations:
            self.read()
        # Process database line by line
        for line in self.db:
            # If this line id matches with arg col_id
            if line['id'] == row_id:
                # Rewrite data in this row, in col, by data
                line[col] = data
                # Auto save db if needed (view __init__ docstring for more info)
                if self.auto_save:
                    self.save()
                return True

        return False

    def _retrieve_rows_by_condition(self, conditions):
        """
        ADMIN FUNCTION
        Returns list indices of rows, which matches the conditions
        :param conditions:string
        :return: list of ids:list
        """

        # Return all the rows, if condition is equal to *
        if conditions == '*':
            return range(len(self.db))

        # We retrieve the operators from conditions
        glob_logical_operators = list(
            map(lambda x: x[0], re.findall('(and|or)(?=([^"]*"[^"]*")*(?![^"]*"))', conditions)))

        # We split conditions to array via regular expression, but exclude logical operators
        conditions = re.findall('[^ =]+[=][^ ]+', conditions)

        # Compiled conditions
        comp_conditions = []

        # Final rows, that will be returned
        final_rows = []

        # Process all conditions
        for condition in conditions:
            try:
                # Retrieve the operator from certain condition
                operator = re.findall('(!=|<=|>=|=|<|>)', condition)[0]
            except IndexError:
                # Operator not specified, or specified
                return False

            # We know, what operator is, and we split the string by operator to col_name and value for this col
            col_name, value = condition.split(operator)
            # Remove brackets
            value = value.replace('"', '')
            # Append compiled value to array with compiled values
            comp_conditions.append([col_name, operator, value])

        for x in range(len(self.db)):
            row = self.db[x]
            # We store local logical_operators, because we pop elements, and want to return them back in future
            logical_operators = glob_logical_operators.copy()
            # We will store comparisons' results and then, by the list of operators, define the result
            logical_values = []
            for condition in comp_conditions:
                # We use boolean variable and the tree of conditions to prevent using insecure eval function
                res = False
                try:
                    if condition[1] == '>=':
                        res = (row[condition[0]] >= condition[2])
                    elif condition[1] == '<=':
                        res = (row[condition[0]] <= condition[2])
                    elif condition[1] == '!=':
                        res = (row[condition[0]] != condition[2])
                    elif condition[1] == '<':
                        res = (row[condition[0]] <= condition[2])
                    elif condition[1] == '>':
                        res = (row[condition[0]] >= condition[2])
                    elif condition[1] == '=':
                        res = (row[condition[0]] == condition[2])
                except TypeError:
                    # We tried to use unsupported operator for this type of variable, just pass, and save False
                    pass

                logical_values.append(res)

            # We don't use for, because while using it we can't modify i, but we need it
            i = 0
            while i < len(logical_operators):
                # Firstly, we process only 'and' operators, because they have max priority
                if logical_operators[i] == 'and':
                    # We store the result in logical values
                    logical_values[i] = (logical_values[i] and logical_values[i + 1])
                    # And delete unnecessary vars
                    del logical_values[i + 1]
                    del logical_operators[i]
                    # Because the length of array is changed, we need to decrease i
                    i -= 1

                i += 1

            # And now process for 'or' operators
            while len(logical_operators) > 0:
                # We store the result in logical values
                logical_values[0] = (logical_values[0] or logical_values[1])
                # And delete unnecessary vars
                del logical_values[1]
                del logical_operators[0]

            # If the row matches the condition, append it to result
            if logical_values[0]:
                final_rows.append(x)

        # Return result
        return final_rows

    def retrieve_rows(self, conditions):
        """
        Retrieves full rows by given conditions
        Condition must contain column_name, operator, value; Example: rid=100; name="Dan"; a=1 or b=2; uid=1 and iud=2
        :param conditions:string
        :return: requested_row:dict
        """

        # Read db if needed (view __init__ docstring for more info)
        if self.read_before_operations:
            self.read()

        # Template for final result
        result = []

        # Process the values, returned by the function _retrieve_rows_by_condition
        for i in self._retrieve_rows_by_condition(conditions):
            result.append(self.db[i])

        # Return processed values
        return result

    def update_rows(self, conditions, col_name, value):
        """
        Updates the column col_name with value value in the rows, which matches the conditions
        :param conditions:string
        :param col_name:string
        :param value:auto
        :return:
        """
        # Read db if needed (view __init__ docstring for more info)
        if self.read_before_operations:
            self.read()

        # Find the rows, that matches condition
        matches = self._retrieve_rows_by_condition(conditions)
        for i in matches:
            self.db[i][col_name] = str(value)

        return True
