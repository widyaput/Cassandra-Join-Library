from tabulate import tabulate


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
