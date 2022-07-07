from pympler import asizeof

from utils import *

# Intermediate result for hash join
class IntermediateHashResult:

    def __init__(self, join_column, join_order, max_size, next_join_column):
        super().__init__()
        self.hash_table = {}
        self.total_rows = 0
        self.join_column = join_column
        self.max_data_size = max_size
        self.join_order = join_order
        self.next_join_column = next_join_column
    


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

        # Flag to use partition on local or not
        should_use_partition = False
        partitions_id = {}

        for key in self.hash_table:
            left_list = self.hash_table[key][0]
            right_list = self.hash_table[key][1]

            if (len(left_list) == 0 or len(right_list) == 0):
                # No Result for current key
                continue


            for left_idx in range(len(left_list)):
                left_item = left_list[left_idx]
                
                # Use merged_buffer when data should be divided
                # merged_buffer = []

                for right_idx in range(len(right_list)):
                    right_item = right_list[right_idx]
                    # Double checking real value (hash value already matched)
                    if (left_item[self.join_column] == right_item[self.join_column]):
                        merged = dict(list(left_item.items()) + list(right_item.items()))

                        result_size = asizeof.asizeof(result)
                        merged = asizeof.asizeof(merged)

                        # Checking whether memory still fit
                        if ((self.max_data_size <= result_size + merged) and (not should_use_partition)):
                            should_use_partition = True

                        if (should_use_partition):
                            result_join_num = self.join_order + 1
                            
                            row_partition_id = put_into_partition([merged], result_join_num, self.next_join_column)
                            partitions_id.union(row_partition_id)

                        else :
                            result.append(merged)
                
                # Preserve Memory
                left_item[left_idx] = None

            # Remove after usage, maintain memory usage
            self.hash_table[key] = []

        # Reset
        self.hash_table = {}

        return result, partitions_id




