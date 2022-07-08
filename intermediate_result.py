from pympler import asizeof

from utils import *

# Intermediate result for hash join
class IntermediateDirectHashResult:

    def __init__(self, join_column, join_type, join_order, max_size, next_join_column):
        super().__init__()
        self.join_type = join_type
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

        result_join_num = self.join_order + 1

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

        # Flush Result
        put_into_partition(result, result_join_num, self.next_join_column)
        result = []

        return result, partitions_id




class IntermediatePartitionedHashResult:

    def __init__(self, join_column, join_type, join_order, max_size, next_join_column):
        super().__init__()
        self.total_rows = 0

        self.join_type = join_type
        self.join_column = join_column

        self.max_data_size = max_size
        self.join_order = join_order
        self.next_join_column = next_join_column

    def process_partition_pair(self, partition_num):

        partition_hash_table = {}

        left_partition = read_from_partition(join_order, partition_num, True)
        right_partition = read_from_partition(join_order, partition_num, False)

        # Check if left partition or right partition is not found
        if ((left_partition == None) or (right_partition == None)):
            # Gives no result, return empty hash table immediately
            return {}
        
        # Process left table. Assume left table always be the Build Table
        for left_row in left_partition:
            key = left_row[self.join_column]
            if (key in partition_hash_table):
                # There is already a key for that join column
                partition_hash_table[key][0].append(left_row)
            
            else :
                # Key is new for the hash table
                partition_hash_table[key] = ([left_row],[])
            
        
        # Process right table. Assume right table always be the Probe table
        for right_row in right_partition:
            key = right_row[self.join_column]
            if (key in partition_hash_table):
                # There is already a key for that join column
                partition_hash_table[key][1].append(right_row)
            
            else :
                partition_hash_table[key][1] = ([],[right_row])

        return partition_hash_table

    def build_result(self, all_partitions):

        join_order = self.join_order

        result_partition_ids = {}
        
        for partition_id in all_partitions:
            
            # Hash table access 
            # partition_hash_table[key][0] for left rows
            # partition_hash_table[key][1] for right rows
            partition_hash_table = self.process_partition_pair(partition_id)
            partition_join_result = []

            # Do the partition join based on join type
            if (self.join_type == "INNER"):

                # Do the join
                for key in partition_hash_table:
                    left_list = partition_hash_table[key][0]
                    right_list = partition_hash_table[key][1]
                    # Check if left list or right list is empty

                    if ((left_list == None) or (right_list == None)):
                        # Gives no result, continue to next key
                        continue

                    for left_row in left_list:
                        for right_row in right_list:
                            # Compare for double checking equality
                            if (left_row[self.join_column] == right_row[self.join_column]):
                                merged = dict(list(left_row.items()) + list(right_row.items()))
                                partition_join_result.append(merged)
                    
                # For each partition, flush into local disk
                next_join_order = self.join_order + 1
                result_hash_values = put_into_partition(partition_join_result, next_join_order, self.next_join_column, True)

                # Merge partition ids with other ids
                result_partition_ids.union(result_hash_values)
            
            else :
                # TODO: Create for Left outer, Right outer, and Full outer join case
                return []

        return result_partition_ids
        