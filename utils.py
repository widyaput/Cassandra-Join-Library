import os
import shutil
from tabulate import tabulate


# K is the biggest prime in the first million
global K
K = 15485863

# Utils for Python Join

def print_result_as_table(result):
    # Print using Tabulate Library

    if (result == None):
        print("EMPTY RESULT SET")
        return

    if (len(result) == 0):
        print("EMPTY RESULT SET")
        return

    cols = {}
    for key in result[0]:
        cols[key] = key

    table = tabulate(result, headers=cols, tablefmt='orgtbl')

    print(table)

    return 


def partition_hash_function(M): # H1(X) Function
    # Summing all characters of the input in ASCII Number multiplied by the position of the character in the string 
    # divide the sum with big prime number P, then retrieve the remainder

    # M is input and force convert M to string
    M = str(M)

    total_sum = 0

    for i in range(len(M)):
        c = M[i]
        total_sum += ord(c) * (i+1)

    hash_value = total_sum % K

    return hash_value


def read_from_partition(iter_num, partition_id):
    return


def put_into_partition(data_page, iter_num, join_column, is_left_table):

    hash_values_set = {}

    cwd = os.getcwd()
    tmp_folder_name = "tmpfolder"

    tmp_folder_path = os.path.join(cwd, tmp_folder_name)

    if (not os.path.isdir(tmp_folder_path)):
        os.mkdir(tmp_folder_path)

    iter_path = os.path.join(tmp_folder_path, str(iter_num))

    if (not os.path.isdir(iter_path)):
        os.mkdir(iter_path)

    for data in data_page:
        join_column_value = data[join_column]
        partition_number = partition_hash_function(join_column_value)

        partition_filename = None

        if (is_left_table):
            partition_filename = str(partition_number) + "_l" + ".txt"
        else :
            partition_filename = str(partition_number) + "_r" + ".txt"
            
        parittion_fullname = os.path.join(iter_path, partition_filename)

        hash_values_set.add(partition_number)

        f = open(parittion_fullname, mode='a')
        f.write(str(data)+"\n")
        f.close()
    

    # After use all temps, delete tmpfolder
    # shutil.rmtree(tmp_folder_path)

    return hash_values_set


def bit_to_byte(b):
    as_byte = b / 8

    return as_byte

def byte_to_kilobyte(b):
    as_kb = b / (1e3)

    return as_kb

def byte_to_megabyte(b):
    as_mb = b / (1e6)

    return as_mb

def byte_to_gigabyte(b):
    as_gb = b / (1e9)

    return as_gb