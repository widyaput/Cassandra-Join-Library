# Intermediate result

class IntermediateResult:

    def __init__(self, join_column):
        super().__init__()
        self.hash_table = {}
        self.total_rows = 0
        self.join_column = join_column
    


    def add_row_to_intermediate(self, row_dict, is_left):
    # If certain value for join column is already available, append to left / right list
    # If not, add new hashtable member with new value (empty left and right list)

        key_value = row_dict[self.join_column]

        if (key_value == "null" or key_value == None): 
            # Do not insert when value is null or none
            return

        if (key_value in self.hash_table):
            # Insert to left / right list
            if (is_left):
                self.hash_table[key_value][0].append(row_dict)
            
            else :
                self.hash_table[key_value][1].append(row_dict)
            

        else :
            # Make tuple of left list and right list
            if (is_left):
                self.hash_table[key_value] = ([row_dict],[])
            else :
                self.hash_table[key_value] = ([],[row_dict])

        return

    
    def build_result(self):
        # Join is N x M operation
        result = []

        for key in self.hash_table:
            left_list = self.hash_table[key][0]
            right_list = self.hash_table[key][1]

            if (len(left_list) == 0 or len(right_list) == 0):
                # No Result for current key
                continue


            for left_item in left_list:
                for right_item in right_list:
                    merged = dict(list(left_item.items()) + list(right_item.items()))
                    result.append(merged)

            # Remove after usage, maintain memory usage
            self.hash_table[key] = []

        # Reset
        self.hash_table = {}

        return result




