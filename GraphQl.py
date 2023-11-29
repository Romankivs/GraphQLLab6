from flask import Flask
from graphene import ObjectType, String, Int, Boolean, Schema, List, Field, Mutation, InputObjectType
from flask_graphql import GraphQLView

class DynamicTable:
    def __init__(self, column_info):
        self.column_info = column_info
        self.rows = []

    def add_column(self, column_name, column_type):
        if column_name in [col[0] for col in self.column_info]:
            raise ValueError(f"Column '{column_name}' already exists")

        self.column_info.append((column_name, column_type))

        for row in self.rows:
            row[column_name] = None

    def delete_column(self, column_name):
        if column_name not in [col[0] for col in self.column_info]:
            raise ValueError(f"Column '{column_name}' does not exist")

        index = [col[0] for col in self.column_info].index(column_name)
        del self.column_info[index]

        for row in self.rows:
            del row[column_name]

    def add_row(self, values):
        if len(values) != len(self.column_info):
            raise ValueError("Number of values must match the number of columns")

        validated_values = []
        for i, (column_name, column_type) in enumerate(self.column_info):
            value = values[i]
            if not isinstance(value, column_type):
                print('not', value, column_type)
                raise ValueError(f"Invalid type for column '{column_name}'. Expected {column_type}, got {type(value)}")
            validated_values.append(value)

        row = dict(zip([col[0] for col in self.column_info], validated_values))
        self.rows.append(row)
    
    def update_row(self, row_index, values):
        if row_index < 0 or row_index >= len(self.rows):
            raise IndexError("Row index is out of bounds")

        if len(values) != len(self.column_info):
            raise ValueError("Number of values must match the number of columns")

        validated_values = []
        for i, (column_name, column_type) in enumerate(self.column_info):
            value = values[i]
            if not isinstance(value, column_type):
                raise ValueError(f"Invalid type for column '{column_name}'. Expected {column_type}, got {type(value)}")
            validated_values.append(value)

        self.rows[row_index] = dict(zip([col[0] for col in self.column_info], validated_values))

    def display_table(self):
        header = "|".join([col[0] for col in self.column_info])
        print(header)
        print("-" * len(header))

        for row in self.rows:
            row_values = [str(row.get(column[0], "")) for column in self.column_info]
            print("|".join(row_values))

    def remove_duplicates(self):
        seen_rows = set()
        unique_rows = []

        for row in self.rows:
            key_values = tuple(row[column] for column, _ in self.column_info)
            if key_values not in seen_rows:
                seen_rows.add(key_values)
                unique_rows.append(row)

        self.rows = unique_rows

class Database:
    def __init__(self):
        self.tables = {}

    def add_table(self, table_name, column_info):
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")

        self.tables[table_name] = DynamicTable(column_info)

    def remove_table(self, table_name):
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")

        del self.tables[table_name]

    def display_tables(self):
        for table_name, dynamic_table in self.tables.items():
            print(f"\nTable: {table_name}")
            dynamic_table.display_table()


app = Flask(__name__)
database = Database()

table_info = [
    ("Name", str),
    ("Age", int),
    ("City", str),
    ("IsStudent", bool)
]

table = DynamicTable(table_info)

table.add_row(["Alice", 25, "New York", False])
table.add_row(["Bob", 30, "San Francisco", True])
table.add_row(["Alice", 25, "New York", False])  # Duplicate row
table.add_row(["Charlie", 22, "Los Angeles", True])

table.display_table()

database.tables["Table1"] = table

class RowType(ObjectType):
    column_name = String()
    column_type = String()

class RowInputType(InputObjectType):
    column_name = String()
    column_type = String()

class TableType(ObjectType):
    table_name = String()
    column_info = List(RowType)
    rows = List(String)

class Query(ObjectType):
    tables = List(TableType)
    table = Field(TableType, table_name=String())

    def resolve_tables(self, info):
        tables = []
        for table_name, dynamic_table in database.tables.items():
            table_data = {
                'table_name': table_name,
                'column_info': [{'column_name': col[0], 'column_type': str(col[1].__name__)} for col in dynamic_table.column_info],
                'rows': dynamic_table.rows
            }
            tables.append(table_data)
        return tables

    def resolve_table(self, info, table_name):
        if table_name not in database.tables:
            return None
        dynamic_table = database.tables[table_name]
        column_info = [{'column_name': col[0], 'column_type': str(col[1].__name__)} for col in dynamic_table.column_info]
        return {
            'table_name': table_name,
            'column_info': column_info,
            'rows': dynamic_table.rows
        }


def convert_values(column_info, values):
    converted_values = []
    for i, (_, column_type) in enumerate(column_info):
        value = values[i]

        if column_type == str:
            converted_values.append(str(value))
        elif column_type == int:
            converted_values.append(int(value))
        elif column_type == bool:
            converted_values.append(value.lower() == "true")
        else:
            converted_values.append(value)

    return converted_values

class AddTableMutation(Mutation):
    class Arguments:
        table_name = String(required=True)
        column_info = List(RowInputType, required=True)

    success = Boolean()

    def mutate(self, info, table_name, column_info):
        try:
            converted_column_info = [(col.column_name, eval(col.column_type)) for col in column_info]
            database.add_table(table_name, converted_column_info)
            return AddTableMutation(success=True)
        except ValueError:
            return AddTableMutation(success=False)

class RemoveTableMutation(Mutation):
    class Arguments:
        table_name = String(required=True)

    success = Boolean()

    def mutate(self, info, table_name):
        try:
            database.remove_table(table_name)
            return RemoveTableMutation(success=True)
        except ValueError:
            return RemoveTableMutation(success=False)

class AddRowMutation(Mutation):
    class Arguments:
        table_name = String(required=True)
        values = List(String, required=True)

    success = Boolean()

    def mutate(self, info, table_name, values):
        if table_name not in database.tables:
            return AddRowMutation(success=False)

        dynamic_table = database.tables[table_name]

        try:
            converted_values = convert_values(dynamic_table.column_info, values)
            dynamic_table.add_row(converted_values)
            return AddRowMutation(success=True)
        except ValueError as e:
            print(f"Error adding row to {table_name}: {e}")
            return AddRowMutation(success=False)


class UpdateRowMutation(Mutation):
    class Arguments:
        table_name = String(required=True)
        row_index = Int(required=True)
        values = List(String, required=True)

    success = Boolean()

    def mutate(self, info, table_name, row_index, values):
        if table_name not in database.tables:
            return UpdateRowMutation(success=False)

        dynamic_table = database.tables[table_name]

        try:
            converted_values = convert_values(dynamic_table.column_info, values)
            dynamic_table.update_row(row_index, converted_values)
            return UpdateRowMutation(success=True)
        except (ValueError, IndexError) as e:
            print(f"Error updating row in {table_name}: {e}")
            return UpdateRowMutation(success=False)

class DeleteRowMutation(Mutation):
    class Arguments:
        table_name = String(required=True)
        row_index = Int(required=True)

    success = Boolean()

    def mutate(self, info, table_name, row_index):
        if table_name not in database.tables:
            return DeleteRowMutation(success=False)

        dynamic_table = database.tables[table_name]

        try:
            del dynamic_table.rows[row_index]
            return DeleteRowMutation(success=True)
        except IndexError:
            return DeleteRowMutation(success=False)

class RemoveDuplicatesMutation(Mutation):
    class Arguments:
        table_name = String(required=True)

    success = Boolean()

    def mutate(self, info, table_name):
        if table_name not in database.tables:
            return RemoveDuplicatesMutation(success=False)

        dynamic_table = database.tables[table_name]

        try:
            dynamic_table.remove_duplicates()
            return RemoveDuplicatesMutation(success=True)
        except ValueError:
            return RemoveDuplicatesMutation(success=False)

class Mutation(ObjectType):
    add_table = AddTableMutation.Field()
    remove_table = RemoveTableMutation.Field()
    add_row = AddRowMutation.Field()
    update_row = UpdateRowMutation.Field()
    delete_row = DeleteRowMutation.Field()
    remove_duplicates = RemoveDuplicatesMutation.Field()

schema = Schema(query=Query, mutation=Mutation)

app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view('graphql', schema=schema, graphiql=True)
)

if __name__ == '__main__':
    app.run(debug=True)
