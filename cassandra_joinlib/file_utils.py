import os, shutil
# File utils to ease join usage, includes format utils

def jsonTupleKeyEncoder(data):
    # tuple-key dict to json
    nested_key_data = []
    for row in data:
        modified_row = {}
        for key, value in row["data"].items():
            col_name, table_name = key
            value = row["data"][key]

            if (col_name in modified_row):
                modified_row[col_name][table_name] = value

            else :
                modified_row[col_name] = {}
                modified_row[col_name][table_name] = value
        
        flag = row["flag"]
        modified_row_with_flag = {"data" : modified_row, "flag" : flag}
        nested_key_data.append(modified_row_with_flag)

    return nested_key_data

def jsonTupleKeyHashEncoder(data):
    # This is no flag version of jsonTupleKeyEncoder
    nested_key_data = []
    for row in data:
        modified_row = jsonTupleKeyHashUnitEncoder(row)
        nested_key_data.append(modified_row)

    return nested_key_data


def jsonTupleKeyHashUnitEncoder(row):
    modified_row = {}
    for key, value in row.items():
        col_name, table_name = key
        value = row[key]

        if (col_name in modified_row):
            modified_row[col_name][table_name] = value
        else :
            modified_row[col_name] = {}
            modified_row[col_name][table_name] = value

    return modified_row


def jsonTupleKeyDecoder(loaded_json):
    # json to tuple-key dict
    tuple_key_data = []
    for row in loaded_json:
        original_row = {}
        for col_name in row["data"]:
            for table_name in row["data"][col_name]:
                key = (col_name, table_name)
                value = row["data"][col_name][table_name]
                
                original_row[key] = value

        flag = row["flag"]
        original_row_with_flag = {"data" : original_row, "flag" : flag}
        tuple_key_data.append(original_row_with_flag)

    return tuple_key_data


def jsonTupleKeyHashDecoder(loaded_json):
    # This is no flag version of jsonTupleKeyDecoder
    tuple_key_data = []

    for row in loaded_json:
        original_row = {}
        for col_name in row:
            for table_name in row[col_name]:
                tuple_key = (col_name, table_name)
                value = row[col_name][table_name]

                original_row[tuple_key] = value
        
        tuple_key_data.append(original_row)

    return tuple_key_data



def printableTupleKeyDecoder(data):
    for idx in range(len(data)):
        row = data[idx]
        row_data = data[idx]["data"]
        printable = {}
        for key in row_data:
            col_name, table_name = key
            value = row_data[key]
            new_key = f"{col_name} [{table_name}]"
            printable[new_key] = value
        
        data[idx] = printable

    return data

def printableHashJoinDecoder(data):
    for idx in range(len(data)):
        row = data[idx]
        printable = {}
        for key in row:
            col_name, table_name = key
            value = row[key]
            new_key = f"{col_name} [{table_name}]"
            printable[new_key] = value
    
        data[idx] = printable
    
    return data


def delete_prev_result(join_order):

    cwd = os.getcwd()
    tmp_folder = "tmpfolder"

    tmp_folder_path = os.path.join(cwd, tmp_folder)
    join_order_path = os.path.join(tmp_folder_path, join_order)

    if (not os.path.isdir(join_order_path)):
        return

    shutil.rmtree(join_order_path)

    return

