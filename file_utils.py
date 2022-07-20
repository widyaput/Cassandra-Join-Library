# File utils to ease join usage

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