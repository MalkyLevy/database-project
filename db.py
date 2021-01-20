import csv
import json
import os
from dataclasses import dataclass

# from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Type

from db_api import DBField, SelectionCriteria, DBTable, DataBase, DB_ROOT


# ------------------------------------- DBField ------------------------------------- #

# @dataclass_json
@dataclass
class DBField(DBField):
    pass
    # name: str
    # type: Type


# ------------------------------------- SelectionCriteria ------------------------------------- #

# @dataclass_json
@dataclass
class SelectionCriteria(SelectionCriteria):
    pass
    # field_name: str
    # operator: str
    # value: Any


# ------------------------------------- DBTable ------------------------------------- #

# @dataclass_json
@dataclass
class DBTable(DBTable):
    # name: str
    # fields: List[DBField]
    # key_field_name: str

    def count(self) -> int:
        print("=== count ===")
        # TODO: insert variable to DataBase
        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)
            counter = 0

            for row in table_data:
                if row != []:
                    counter += 1

        return counter - 1

    def insert_record(self, values: Dict[str, Any]) -> None:
        print("=== insert_record ===")
        with open(DB_ROOT / f"{self.name}.csv", "r+") as table_file:
            table_data = csv.reader(table_file)

            for row in table_data:
                fileds = row
                break

            new_row = []
            for f in fileds:
                new_row += [values[f]]

            table_data = csv.writer(table_file)
            table_data.writerow(new_row)

    def delete_record(self, key: Any) -> None:
        print("=== delete_record ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            key_field = db_data[self.name]["key"]
            fields_list = db_data[self.name]["fields"]
            f = [list(d.keys())[0] for d in fields_list]
            key_index = f.index(key_field)

        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)
            new_table_data = []

            print(table_data)
            for row in table_data:
                print(row)
                if row != [] and str(row[key_index]) != str(key):
                    print(row[key_index])
                    print(key)
                    new_table_data.append(row)

            # if len(new_table_data) == self.count():
            #     raise ValueError()

        with open(DB_ROOT / f"{self.name}.csv", "w") as table_file:
            table_data = csv.writer(table_file)
            table_data.writerows(new_table_data)
            print(new_table_data)

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        print("=== delete_records ===")
        raise NotImplementedError

    def get_record(self, key: Any) -> Dict[str, Any]:
        print("=== get_record ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            # key_index = [d.key() for d in db_data[self.name]["fields"]].index(key_field)
            key_field = db_data[self.name]["key"]
            fields = db_data[self.name]["fields"]
            fields_list = [list(d.keys())[0] for d in fields]
            key_index = fields_list.index(key_field)

        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)

            for row in table_data:
                if row != [] and str(row[key_index]) == str(key):
                    record = row
                    break

            print(record)
            res = {}
            for i in range(len(fields_list)):
                res[fields_list[i]] = record[i]

            print(res)
            return res

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        print("=== update_record ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            key_field = db_data[self.name]["key"]
            fields = db_data[self.name]["fields"]
            fields_list = [list(d.keys())[0] for d in fields]
            key_index = fields_list.index(key_field)

        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)
            new_table_data = []

            for row in table_data:
                if row != [] and str(row[key_index]) != str(key):
                    new_table_data.append(row)

                elif row != []:
                    row_to_update = row

            # print(values)
            # print("A", row_to_update)
            # row_to_update += [values[f] for f in values.keys()]
            values_keys = [k for k in values.keys()]
            # print(values_keys)
            # print(fields_list)
            for f in values_keys:
                index = fields_list.index(f)
                row_to_update[index] = values[f]
                # print(row_to_update)

            # print("B", row_to_update)
            new_table_data.append(row_to_update)

        with open(DB_ROOT / f"{self.name}.csv", "w") as table_file:
            table_data = csv.writer(table_file)
            table_data.writerows(new_table_data)

    def get_query_res(self, criterion: SelectionCriteria):
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            fields_list = db_data[self.name]["fields"]
            field_index = [d.key() for d in fields_list].index(criterion.field_name)

        with open(DB_ROOT / f"{self.name}.csv", "r") as table_file:
            table_data = csv.reader(table_file)
            res = set()

            for row in table_data:
                if f"{row[field_index]} {criterion.operator} {criterion.value}":
                    res.add(row)

        return res

    def query_table(self, criteria: List[SelectionCriteria]) \
            -> List[Dict[str, Any]]:
        print("=== query_table ===")
        res = set()

        for SCriteria in criteria:
            res.add(self.get_query_res(SCriteria))

        dict_res = {}
        for i, row in enumerate(res, 1):
            dict_res[i] = row

        return dict_res

    def create_index(self, field_to_index: str) -> None:
        print("=== create_index ===")
        raise NotImplementedError


# ------------------------------------- DataBase ------------------------------------- #

# @dataclass_json
@dataclass
class DataBase(DataBase):
    def __init__(self):
        print("=== init ===")
        if os.path.isfile(DB_ROOT / "DataBase.json") and os.access('./db_files/DataBase.json', os.R_OK):
            print("File exists and is readable/there")
            # with (DB_ROOT / "DataBase.json").open("r") as DB_file:
            #     db_data = json.load(DB_file))

        else:
            with (DB_ROOT / "DataBase.json").open("w") as DB_file:
                json.dump({}, DB_file)

    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:
        print("=== create_table ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            # if table_name in db_data.keys():
            #     raise FileExistsError("The table is exists")

        with open(DB_ROOT / f"{table_name}.csv", "w") as table_file:
            table = csv.writer(table_file)
            table.writerow([f.name for f in fields])

            db_data[table_name] = {
                "fields": [{f.name: str(f.type)} for f in fields],
                "key": key_field_name
            }

            # db_data["num_tables"] += 1

        with open(DB_ROOT / "DataBase.json", "w") as DB_file:
            # db_data = json.load(DB_file)
            json.dump(db_data, DB_file)

        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        print("=== num_tables ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            print(len(db_data.keys()))
            return len(db_data.keys())
        # print(self.num_of_tables)
        # return self.num_of_tables

    def get_table(self, table_name: str) -> DBTable:
        print("=== get_table ===")
        with open(DB_ROOT / "DataBase.json", "r+") as DB_file:
            db_data = json.load(DB_file)

            print(db_data.keys())
            if table_name not in db_data.keys():
                raise FileNotFoundError

        fields_list = []
        for dic in db_data[table_name]["fields"]:
            fields_list.append(dic)

        return DBTable(table_name, fields_list, db_data[table_name]["key"])

    def delete_table(self, table_name: str) -> None:
        print("=== delete_table ===")
        with open(DB_ROOT / "DataBase.json", "r") as DB_file:
            db_data = json.load(DB_file)

            table_names_list = [k for k in db_data.keys()]
            print(table_names_list)
            if table_name not in table_names_list:
                raise FileNotFoundError("The table is not found")

            new_dict = {}
            for tb_name in db_data.keys():
                if tb_name != table_name:
                    new_dict[tb_name] = db_data[tb_name]

                else:
                    os.remove(f"{DB_ROOT}/{tb_name}.csv")

        with open(DB_ROOT / "DataBase.json", "w") as DB_file:
            json.dump(new_dict, DB_file)

    def get_tables_names(self) -> List[Any]:
        print("=== get_tables_names ===")
        with open(DB_ROOT / "DataBase.json", "r+") as DB_file:
            db_data = json.load(DB_file)

            res = []
            [res.append(k) for k in db_data.keys()]
            print(res)
            return res

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
