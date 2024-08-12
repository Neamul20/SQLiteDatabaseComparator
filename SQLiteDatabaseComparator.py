import sqlite3
import argparse
import os 
from colorama import init, Style, Fore

def isDb3(filePath):
    splitStrings =filePath.split('.')
    lastString = splitStrings[-1]
    return lastString.lower() == 'db3'
def boldString(text):
    return f"{Style.BRIGHT}{text}{Style.RESET_ALL}"

def colorString(text, color=Fore.WHITE):

    return f"{color}{text}{Style.RESET_ALL}"


class DatabaseComparator:
    def __init__(self, db1Path, db2Path):
        self.db1Path = db1Path
        self.db2Path = db2Path
        self.db1Name = os.path.basename(db1Path)
        self.db2Name = os.path.basename(db2Path)
        self.db1 = None
        self.db2 = None
        self.maxDifferences=10
        try:
            self.db1 = sqlite3.connect(db1Path)
        except sqlite3.Error as e:
            print(f"Error connecting to database {db1Path}: {e}")

        try:
            self.db2 = sqlite3.connect(db2Path)
        except sqlite3.Error as e:
            print(f"Error connecting to database {db2Path}: {e}")

        finally:
            if not self.db1 or not self.db2:
                self.close()
    
    def getSchema(self, connection):
        '''
        retrieves the schema (table names and columns) 
        '''
        schema = {}
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            for table in tables:
                tableName = table[0]
                try:
                    #PRAGMA statement to retrieve information about the columns in the current table.
                    cursor.execute(f"PRAGMA table_info({tableName});")
                    columns = cursor.fetchall()
                    schema[tableName] = columns
                except sqlite3.Error as e:
                    print(f"Error retrieving schema for table {tableName}: {e}")
        except sqlite3.Error as e:
            print(f"Error retrieving schema: {e}")
        return schema
    def getData(self, connection, tableName):
        '''
        retrieves all data from a specified table sorted by foreign key or primary key
        '''
        data = []
        try:
            cursor = connection.cursor()
            
            # Retrieve foreign key column names
            cursor.execute(f"PRAGMA foreign_key_list({tableName});")
            foreignKeysInfo = cursor.fetchall()
            foreignKeyColumn = None
            if foreignKeysInfo:
                foreignKeyColumn = foreignKeysInfo[0][3]  # The 4th field (index 3) is the column name of the foreign key
            if foreignKeyColumn:
                # Sort by the foreign key
                cursor.execute(f"SELECT * FROM {tableName} ORDER BY {foreignKeyColumn};")
            else:
                # Retrieve the primary key column name
                cursor.execute(f"PRAGMA table_info({tableName});")
                columnsInfo = cursor.fetchall()
                primaryKeyColumn = None
                for column in columnsInfo:
                    if column[5] == 1:  # The 6th field (index 5) indicates if the column is a primary key
                        primaryKeyColumn = column[1]  # The 2nd field (index 1) is the column name
                        break
                
                if primaryKeyColumn is None:
                    raise ValueError(f"No primary key and foreign key to sort the data row found for table {tableName}")

                # Sort by the primary key
                cursor.execute(f"SELECT * FROM {tableName} ORDER BY {primaryKeyColumn};")

            data = cursor.fetchall()
        except sqlite3.Error as e:
            print(f"Error retrieving data from table {tableName}: {e}")
        except ValueError as e:
            print(e)       
        return data

    
    def getKeyConstraints(self, connection):
        '''
        get the indexes, primary, and foreign key 
        '''
        constraints = {}
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()

            for table in tables:
                tableName = table[0]
                try:
                    cursor.execute(f"PRAGMA foreign_key_list({tableName});")
                    foreignKeys = cursor.fetchall()

                    cursor.execute(f"PRAGMA index_list({tableName});")
                    indexes = cursor.fetchall()

                    cursor.execute(f"PRAGMA table_info ({tableName})")
                    columns=cursor.fetchall()
                    primaryKey=[col[1] for col in columns if col[5]==1]
                    constraints[tableName] = {
                        'foreignKeys': foreignKeys,
                        'indexes': indexes,
                        'primaryKey':primaryKey
                    }
                except sqlite3.Error as e:
                    print(f"Error retrieving constraints for table {tableName}: {e}")
        except sqlite3.Error as e:
            print(f"Error retrieving constraints: {e}")
        return constraints

    def compareSchemas(self, schema1, schema2):
        differences = []
        tables1 = set(schema1.keys())
        tables2 = set(schema2.keys())

        missingInDb2 = tables1 - tables2
        missingInDb1 = tables2 - tables1

        if missingInDb2:
            differences.append(f"Tables in ({self.db1Name}) but not in ({self.db2Name}): {missingInDb2}")
        if missingInDb1:
            differences.append(f"Tables in ({self.db2Name}) but not in ({self.db1Name}): {missingInDb1}")

        commonTables = tables1 & tables2
        for table in commonTables:
            columns1 = {col[1] for col in schema1[table]}
            columns2 = {col[1] for col in schema2[table]}
            if columns1 != columns2:
                differences.append(f"Different columns in table {table}: {columns1.symmetric_difference(columns2)}")

        return differences
    
    def compareKeyConstraints(self, constraints1, constraints2):
        differences = []
        tables1 = set(constraints1.keys())
        tables2 = set(constraints2.keys())
        
        commonTables = tables1 & tables2
        for table in commonTables:
            fk1 = set(constraints1[table]['foreignKeys'])
            fk2 = set(constraints2[table]['foreignKeys'])
            if fk1 != fk2:
                differences.append(f"Foreign key differences in table {table}: {fk1.symmetric_difference(fk2)}")

            idx1 = set(constraints1[table]['indexes'])
            idx2 = set(constraints2[table]['indexes'])
            if idx1 != idx2:
                differences.append(f"Index differences in table {table}: {idx1.symmetric_difference(idx2)}")

            pk1 = set(constraints1[table]['primaryKey'])
            pk2 = set(constraints2[table]['primaryKey'])
            if pk1 != pk2:
                differences.append(f"primary key differences in table {table}: {pk1.symmetric_difference(pk2)}")

        
        return differences

    def compareDataCounts(self, schema1, schema2):
        counts = []
        commonTables = set(schema1.keys()) & set(schema2.keys())
        for table in commonTables:
            data1 = self.getData(self.db1, table)
            data2 = self.getData(self.db2, table)
            if len(data1) != len(data2):
                counts.append(f"Table {table} has {len(data1)} entries in ({self.db1Name}) and {len(data2)} entries in ({self.db2Name})")

        return counts

    def compareData(self, schema1, schema2):
        differences = []

        commonTables = set(schema1.keys()) & set(schema2.keys())
        for table in commonTables:
            data1 = self.getData(self.db1, table)
            data2 = self.getData(self.db2, table)

            if data1 == data2:
                continue

            equalCount = 0
            unequalCount = 0
            maxDifferences = self.maxDifferences
            differencesFound = 0

            min_length = min(len(data1), len(data2))
            for i in range(min_length):
                if data1[i] == data2[i]:
                    equalCount += 1
                else:
                    unequalCount += 1
                    if differencesFound < maxDifferences:
                        differences.append(f"--------Difference in table {table} at row {i + 1}--------")
                        differences.append(f"({self.db1Name}): {data1[i]}")
                        differences.append(f"({self.db2Name}): {data2[i]}")
                        differencesFound += 1

            if len(data1) != len(data2):
                unequalCount += abs(len(data1) - len(data2))
                differences.append(f"Table {table} has {len(data1)} entries in ({self.db1Name}) and {len(data2)} entries in ({self.db2Name}).")

            differences.append(f"Table {table}: {equalCount} equal rows, {unequalCount} unequal rows")

        return differences

    def compareDatabases(self):
        if not self.db1 or not self.db2:
            print("One or both database connections are not established. Comparison cannot proceed.")
            return

        try:
            schema1 = self.getSchema(self.db1)
            schema2 = self.getSchema(self.db2)
        except sqlite3.Error as e:
            print(f"Error retrieving schemas: {e}")
            return

        schema_differences = self.compareSchemas(schema1, schema2)
        if schema_differences:
            print("==========Schema differences==========")
            for diff in schema_differences:
                print(diff)
            return
        else:
            print("==========Schemas are identical==========")

        try:
            constraints1 = self.getKeyConstraints(self.db1)
            constraints2 = self.getKeyConstraints(self.db2)
        except sqlite3.Error as e:
            print(f"Error retrieving key constraints: {e}")
            return

        key_differences = self.compareKeyConstraints(constraints1, constraints2)
        if key_differences:
            print("==========Key constraint differences==========")
            for diff in key_differences:
                print(diff)
            return
        else:
            print("==========Key constraints are identical==========")

        countDifferences = self.compareDataCounts(schema1, schema2)
        if countDifferences:
            print("==========Data count differences==========")
            for diff in countDifferences:
                print(diff)
            
        else:
            print("==========Data counts are identical==========")

        dataDifferences = self.compareData(schema1, schema2)
        if dataDifferences:
            print("==========Data differences==========")
            for diff in dataDifferences:
                print(diff)
        else:
            print("==========Data is identical==========")

    def close(self):
        if self.db1:
            self.db1.close()
        if self.db2:
            self.db2.close()




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two SQLite database files.")
    parser.add_argument("db1",type=str, help="Path to the first database file.")
    parser.add_argument("db2",type=str, help="Path to the second database file.")
    args = parser.parse_args()

    if (isDb3(args.db1) and isDb3(args.db2)):
        comparator = DatabaseComparator(args.db1, args.db2)
        comparator.compareDatabases()
        comparator.close()
    else:
        if (not isDb3(args.db1)):
            print (f"'{args.db1}' is not a db3 file")
        if (not isDb3(args.db2)):
            print (f"'{args.db2}' is not a db3 file")

