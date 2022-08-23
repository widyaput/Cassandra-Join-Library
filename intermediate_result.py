import json
from pympler import asizeof

from utils import *

global NULL_DATA
NULL_DATA = None

# Intermediate result for hash join
class IntermediateDirectHashResult:

    def __init__(self, join_info, max_size, next_join_info):
        super().__init__()
        self.left_table = join_info['left_table']
        self.right_table = join_info['right_table']
        self.left_alias = join_info['left_alias']
        self.right_alias = join_info['right_alias']

        if (self.left_alias != None):
            self.left_table = self.left_alias

        if (self.right_alias != None):
            self.right_table = self.right_alias

        self.join_type = join_info['join_type']
        self.join_order = join_info['join_order']
        self.join_column = join_info['join_column']
        self.join_column_right = join_info['join_column_right']

        self.hash_table = {}
        self.total_rows = 0

        self.max_data_size = max_size

        self.next_join_column = next_join_info[0]
        self.next_join_table = next_join_info[1]

        # Metadata for both tables
        self.left_table_meta = join_info['left_columns']
        self.right_table_meta = join_info['right_columns']

        # Build and Probe table matching with Left and Right table
        self.build = "L"
        self.probe = "R"

        # Non-matching rows storage
        self.no_match_left_rows = []
        self.no_match_right_rows = []

        # Next join info
    

    def swap_build_and_probe(self):
        self.build = "R"
        self.probe = "L"


    def add_row_to_left_nomatch(self, row):
        self.no_match_left_rows.append(row)
        return

    def add_row_to_right_nomatch(self, row):
        self.no_match_right_rows.append(row)
        return

    def is_key_in_hashtable(self, key):
        
        if (key in self.hash_table):
            return True

        # Skip null or None values, return as True
        if (key == "null" or key == None or key == "None"):
            return True

        return False


    def add_row_to_intermediate(self, row_dict, is_build):
    # If certain value for join column is already available, append to left / right list
    # If not, add new hashtable member with new value (empty left and right list)

        dict_key = None

        if (is_build and self.build == "L"):
            dict_key = (self.join_column, self.left_table)
        
        elif (is_build and self.build == "R"):
            dict_key = (self.join_column_right, self.right_table)
        
        elif ((not is_build) and self.build == "L"):
            dict_key = (self.join_column_right, self.right_table)

        elif ((not is_build) and self.build == "R"):
            dict_key = (self.join_column, self.left_table)

        key_value = row_dict[dict_key]


        if (key_value == "null" or key_value == None or key_value == "None"): 
            # Do not insert when value is null or none
            return

        if (key_value in self.hash_table):
            # Insert to left / right list
            if (is_build):
                self.hash_table[key_value][0].append(row_dict)
            
            else :
                self.hash_table[key_value][1].append(row_dict)
            

        else :
            # Make tuple of left list and right list
            if (is_build):
                self.hash_table[key_value] = ([row_dict],[])
            else :
                self.hash_table[key_value] = ([],[row_dict])

        return

    
    def build_result(self):
        # Join is N x M operation
        result = []

        # Flag to use partition on local or not
        should_use_partition = False
        partitions_id = set()

        result_join_num = self.join_order + 1

        # Build and Probe key
        build_key = None
        probe_key = None

        if (self.build == "L"):
            build_key = (self.join_column, self.left_table)
            probe_key = (self.join_column_right, self.right_table)
        
        else :
            build_key = (self.join_column_right, self.right_table)
            probe_key = (self.join_column, self.left_table)

        for key in self.hash_table:
            # Left list is ACTUALLY BUILD LIST
            build_list = self.hash_table[key][0]
            # Right list is ACTUALLY PROBE LIST
            probe_list = self.hash_table[key][1]
            

            if (self.join_type == "INNER"):
                if (len(build_list) == 0 or len(probe_list) == 0):
                    # No Result for current key
                    continue
            
            elif (self.join_type == "LEFT_OUTER"):
                if ((len(build_list) == 0) and self.build == "L"):
                    continue

                if ((len(probe_list) == 0) and self.build == "R"):
                    continue

            
            elif (self.join_type == "RIGHT_OUTER"):
                if ((len(probe_list) == 0) and self.build == "L"):
                    continue

                if ((len(build_list) == 0) and self.build == "R"):
                    continue

            
            else : # join_type == FULL_OUTER
                pass


            for left_idx in range(len(build_list)):
                left_item = build_list[left_idx]
                
                # Use merged_buffer when data should be divided
                # merged_buffer = []

                for right_idx in range(len(probe_list)):
                    right_item = probe_list[right_idx]
                    # Double checking real value (hash value already matched).
                    if (left_item[build_key] == right_item[probe_key]):

                        merged = dict(list(left_item.items()) + list(right_item.items()))

                        result_size = asizeof.asizeof(result)
                        merged_size = asizeof.asizeof(merged)

                        # Checking whether memory still fit
                        if ((self.max_data_size <= result_size + merged_size) and (not should_use_partition)):
                            should_use_partition = True

                        if (should_use_partition):
                            
                            row_partition_id = put_into_partition([merged], result_join_num, self.next_join_table, self.next_join_column)
                            partition_id = partitions_id.union(row_partition_id)

                        else :
                            result.append(merged)

                # Action when probe list is empty
                if (len(probe_list) == 0):
                # When no match found, still insert row to result if join type is OUTER
                    if (self.join_type == "INNER"):
                        pass

                    elif (self.join_type == "LEFT_OUTER"):
                        if (self.build == "L"):
                            # Build dummy based on right table
                            dummy_right = construct_null_columns(self.right_table, self.right_table_meta)

                            merged_list = []
                            for left_idx in range(len(build_list)):
                                left_item = build_list[left_idx]
                                merged = dict(list(left_item.items()) + list(dummy_right.items()))

                                merged_list.append(merged)

                            if (should_use_partition):
                                row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                                partition_id = partition_id.union(row_partition_id)
                            
                            else :
                                result = result + merged_list

                        # When build is R, Flush no matching rows at the end

                    elif (self.join_type == "RIGHT_OUTER"):
                        if (self.build == "R"):
                            # Build dummy based on left table
                            dummy_left = construct_null_columns(self.left_table, self.left_table_meta)

                            merged_list = []
                            for right_idx in range(len(build_list)):
                                right_item = build_list[right_idx]
                                merged = dict(list(right_item.items()) + list(dummy_left.items()))

                                merged_list.append(merged)

                            if (should_use_partition):
                                row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                                partition_id = partition_id.union(row_partition_id)
                            
                            else :
                                result = result + merged_list

                        # When build is L, flush no matching rows at the end

                    else : # FULL OUTER JOIN
                        if (self.build == "L"):
                            dummy_right = construct_null_columns(self.right_table, self.right_table_meta)

                            merged_list = []
                            for left_idx in range(len(build_list)):
                                left_item = build_list[left_idx]
                                merged = dict(list(left_item.items()) + list(dummy_right.items()))

                                merged_list.append(merged)

                                if (should_use_partition):
                                    row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                                    partition_id = partition_id.union(row_partition_id)

                                else :
                                    result = result + merged_list
                        
                        else :
                            dummy_left = construct_null_columns(self.left_table, self.left_table_meta)

                            merged_list = []
                            for right_idx in range(len(build_list)):
                                right_item = build_list[right_idx]
                                merged = dict(list(right_item.items()) + list(dummy_left.items()))

                                merged_list.append(merged)

                                if (should_use_partition):
                                    row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                                    partition_id = partition_id.union(row_partition_id)

                                else :
                                    result = result + merged_list
                        
                    # Preserve Memory
                    left_item[left_idx] = None


        # Flush no matching rows
        if (self.add_row_to_left_nomatch != [] or self.add_row_to_right_nomatch != []):
            if (self.join_type == "LEFT_OUTER"):
                if (self.build == "R"):
                    dummy_right = construct_null_columns(self.right_table, self.right_table_meta)
                    merged_list = []

                    for row_idx in range(len(self.no_match_left_rows)):
                        row = self.no_match_left_rows[row_idx]
                        merged = dict(list(row.items()) + list(dummy_right.items()))

                        merged_list.append(merged)

                        # Preserve memory
                        self.no_match_left_rows[row_idx] = None

                    if (should_use_partition):
                        row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                    
                    else :
                        result = result + merged_list
                
            elif (self.join_type == "RIGHT_OUTER"):
                if (self.build == "L"):
                    dummy_left = construct_null_columns(self.left_table, self.left_table_meta)
                    merged_list = []

                    for row_idx in range(len(self.no_match_right_rows)):
                        row = self.no_match_right_rows[row_idx]
                        merged = dict(list(row.items()) + list(dummy_left.items()))

                        merged_list.append(merged)

                        # Preserve memory
                        self.no_match_right_rows[row_idx] = None

                    if (should_use_partition):
                        row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                    
                    else :
                        result = result + merged_list

            else : # FULL OUTER
                if (self.build == "R"):
                    dummy_right = construct_null_columns(self.right_table, self.right_table_meta)
                    merged_list = []

                    for row_idx in range(len(self.no_match_left_rows)):
                        row = self.no_match_left_rows[row_idx]
                        merged = dict(list(row.items()) + list(dummy_right.items()))

                        merged_list.append(merged)

                        self.no_match_left_rows[row_idx] = None

                    if (should_use_partition):
                        row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                    
                    else :
                        result = result + merged_list

                else :
                    dummy_left = construct_null_columns(self.left_table, self.left_table_meta)
                    merged_list = []

                    for row_idx in range(len(self.no_match_right_rows)):
                        row = self.no_match_right_rows[row_idx]
                        merged = dict(list(row.items()) + list(dummy_left.items()))

                        merged_list.append(merged)


                        self.no_match_right_rows[row_idx] = None

                    if (should_use_partition):
                        row_partition_id = put_into_partition(merged_list, result_join_num, self.next_join_table, self.next_join_column)
                    
                    else :
                        result = result + merged_list


            # Remove after usage, maintain memory usage
            self.hash_table[key] = []

        # Reset
        self.hash_table = {}

        # Flush Result if partition needed
        if (should_use_partition):
            flushed_partition_ids = put_into_partition(result, result_join_num, self.next_join_table, self.next_join_column, True)
            partition_id = partitions_id.union(flushed_partition_ids)
            result = []

        return result, partitions_id




class IntermediatePartitionedHashResult:

    def __init__(self, join_info, max_size, next_join_info):
        super().__init__()
        self.total_rows = 0

        self.left_table = join_info['left_table']
        self.right_table = join_info['right_table']
        self.left_alias = join_info['left_alias']
        self.right_alias = join_info['right_alias']

        if (self.left_alias != None):
            self.left_table = self.left_alias

        if (self.right_alias != None):
            self.right_table = self.right_alias

        self.join_type = join_info['join_type']
        self.join_column = join_info['join_column']
        self.join_column_right = join_info['join_column_right']
        self.join_order = join_info['join_order']

        self.max_data_size = max_size
        
        # Metadata for both tables
        self.left_table_meta = join_info['left_columns']
        self.right_table_meta = join_info['right_columns']

        # # If next join column is None, therefore it is the final result
        # # For final result, set next_join_column = join_column
        # if (next_join_column == None):
        #     self.next_join_column = join_info['join_column']
        # else :
        #     self.next_join_column = next_join_column
        self.next_join_column = next_join_info[0]
        self.next_join_table = next_join_info[1]


    def process_partition_pair(self, partition_num):

        partition_hash_table = {}
        join_order = self.join_order

        left_partition = read_from_partition(join_order, partition_num, True)
        right_partition = read_from_partition(join_order, partition_num, False)

        left_joincol_key = (self.join_column, self.left_table)
        right_joincol_key = (self.join_column_right, self.right_table)

        # Check if left partition or right partition is not found
        if ((left_partition == None) and (right_partition == None)):
            # Gives no result, return empty hash table immediately
            return {}

        # Process left table. Assume left table always be the Build Table
        if (left_partition != None):
            for left_row in left_partition:

                key = left_row[left_joincol_key]
                if (key in partition_hash_table):
                    # There is already a key for that join column
                    partition_hash_table[key][0].append(left_row)
                
                else :
                    # Key is new for the hash table
                    partition_hash_table[key] = ([left_row],[])
            
        
        # Process right table. Assume right table always be the Probe table
        if (right_partition != None):
            for right_row in right_partition:

                key = right_row[right_joincol_key]
                if (key in partition_hash_table):
                    # There is already a key for that join column
                    partition_hash_table[key][1].append(right_row)
                
                else :
                    partition_hash_table[key] = ([],[right_row])


        return partition_hash_table

    def build_result(self, all_partitions):

        join_order = self.join_order

        result_partition_ids = set()

        left_joincol_key = (self.join_column, self.left_table)
        right_joincol_key = (self.join_column_right, self.right_table)
        
        for partition_id in all_partitions:
            
            # Hash table access 
            # partition_hash_table[key][0] for left rows
            # partition_hash_table[key][1] for right rows
            partition_hash_table = self.process_partition_pair(partition_id)
            partition_join_result = []

            # Do the partition join based on join type
            # In partition mode there is no build or probe, only left and right

            # Do the join
            for key in partition_hash_table:
                left_list = partition_hash_table[key][0]
                right_list = partition_hash_table[key][1]
                # Check if left list or right list is empty

                if ((left_list == None) and (right_list == None)):
                    # Gives no result, continue to next key
                    continue
                    
                
                if (self.join_type == "LEFT_OUTER" and right_list == []):
                    for left_row in left_list:
                        dummy_right = construct_null_columns(self.right_table, self.right_table_meta)
                        merged = dict(list(left_row.items()) + list(dummy_right.items()))
                        partition_join_result.append(merged)

                elif (self.join_type == "RIGHT_OUTER" and left_list == []):
                    for right_row in right_list:
                        dummy_left = construct_null_columns(self.left_table, self.left_table_meta)
                        merged = dict(list(right_row.items()) + list(dummy_left.items()))
                        partition_join_result.append(merged)

                elif (self.join_type == "FULL_OUTER"):
                    if (left_list == []):
                        for right_row in right_list:
                            dummy_left = construct_null_columns(self.left_table, self.left_table_meta)
                            merged = dict(list(right_row.items()) + list(dummy_left.items()))
                            partition_join_result.append(merged)

                    elif (right_list == []) :
                        for left_row in left_list:
                            dummy_right = construct_null_columns(self.right_table, self.right_table_meta)
                            merged = dict(list(left_row.items()) + list(dummy_right.items()))
                            partition_join_result.append(merged)

                    else :
                        for left_row in left_list:
                            for right_row in right_list:
                                # Compare for double checking equality
                                if (left_row[left_joincol_key] == right_row[right_joincol_key]):
                                    merged = dict(list(left_row.items()) + list(right_row.items()))
                                    partition_join_result.append(merged)

                else :
                    for left_row in left_list:
                        for right_row in right_list:
                            # Compare for double checking equality
                            if (left_row[left_joincol_key] == right_row[right_joincol_key]):
                                merged = dict(list(left_row.items()) + list(right_row.items()))
                                partition_join_result.append(merged)


            # For each partition, flush into local disk
            next_join_order = self.join_order + 1

            result_hash_values = put_into_partition(partition_join_result, next_join_order, self.next_join_table, self.next_join_column, True)

            # Merge partition ids with other ids
            result_partition_ids = result_partition_ids.union(result_hash_values)
            

        return result_partition_ids
        