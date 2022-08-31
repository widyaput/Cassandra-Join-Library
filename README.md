# Cassandra-Join-Library
Cassandra Join Operation Extension for Python. Client-sided and runs on single machine.
### Latest Version : 0.2.0

Requirements :
1. Python >= 3.7
2. Supporting Only Cassandra 4.0

## Installation Guide
To install the package you will need `pip`, then run `pip install cassandra-joinlib`.
Pip install will automatically install all dependencies and you are ready to go. 🍻

## Usage Guide
It is actually pretty simple to use this library, this is the general steps for you to follow:
1. This library works as an executor for join operation, so it focuses on `JoinExecutor` class which has 2 types that you can utilize based on your needs:
    - If you need an equi join, it is better to use `HashJoinExecutor`. `HashJoinExecutor` is importable from `cassandra_joinlib.hash_join`
    - For non equi-join, you may use `NestedJoinExecutor` (Hash join doesn't support non equi-join). Import `NestedJoinExecutor` from `cassandra_joinlib.nested_join`. 
2. Both `HashJoinExecutor` and `NestedJoinExecutor` requires two parameters to be initialized, your connection with cassandra `session` and the name of the keyspace `keyspace_name`. 
    > Example: `HashJoinExecutor(session, keyspace_name)`.
4. Once you have initialized the executor, you may add join operation/operations using these functions:
   - `join(TableInfo left_table_info, TableInfo, right_table_info, str operator)` for inner join
   - `rightJoin(TableInfo left_table_info, TableInfo right_table_info, str operator)` for right outer join
   - `leftJoin(TableInfo, left_table_info, TableInfo right_table_info, str operator)` for left outer join
   - `fullOuterJoin(TableInfo left_table_info, TableInfo right_table_info, str operator)` for full outer join
5. We need `TableInfo` object for our join function. To initialize it, we need 2 params which are name of the table and name of the join column for that table. `TableInfo` can be imported from `cassandra_joinlib.commands`.
    > Example: Let's say we want to (inner) join table `user` and table `payment_received` based on join column `email` using `HashJoinExecutor`.
    > 
    > ```
    > left_table_info = TableInfo('user', 'email')
    > right_table_info = TableInfo('payment_received', 'email')
    > HashJoinExecutor(session, keyspace_name).join(left_table_info, right_table_info)
    > ```
5. Each of the join function returns join executor itself, so you may add a chained join operation like this:
    > ```
    > NestedJoinExecutor(session, keyspace_name) \
    > .join(left_table_info, right_table_info, "<") \
    > .fullOuterJoin(left_table_info_2, right_table_info2, "=")
    > ```
6. After you add join operation/operations to the executor, it actually queue the join command(s). You may execute the join operation when you think you are ready by using `.execute()` from the join executor and do not forget to save the join result with `.save_result(filename)`.
    > Example: 
    > ```
    > HashJoinExecutor(session, keyspace_name) \
    > .join(left_tbl_info, right_tbl_info) \
    > .execute() \
    > .save_result("my_first_join")
    > ```
    > Note that the result will be saved in `.txt` file in JSON Format

7. Run the python script and wait until it finishes execution. 
8. You may also print the join result by using `printJoinResult(filename, max_buffer_size)` which can be imported from `cassandra_joinlib.utils`. Param `max_buffer_size` is the number of rows that will be printed per `tabulate` and has `10000` as a default value. Print function should be called after `.execute()` function.
9. That's it, you're done 🥳 🥳 🥳
---

Hey... you can also get join execution time with `.get_time_elapsed()` called on the join executor after `.execute()` function.
## Important things you need to know
This library executes any chained-join with left deep join method. It means that result of first join operation would be the left table of the second join operation, the result of second join operation would be the left table of the third join operation, and so on...
![Left deep join tree](/assets/leftdeeptree.png)

For NON-first join, make sure you use the result from the previous join as left table. 
> Example:
> ```
> tableinfo1_L = TableInfo("user", "email")
> tableinfo1_R = TableInfo("payment_received", "email")
>
> tableinfo2_L = TableInfo("user", "userid")
> tableinfo2_R = TableInfo("user_item_like", "userid")
>
> HashJoinExecutor(session, keyspace_name) \
> .fullOuterJoin(tableinfo1_L, tableinfo1_R) \
> .join(tableinfo2_L, tableinfo2_R) \
> .execute() \
> .save_result("chained_hash_join")
> ```

Notice that on left table info for second join operation `tableinfo2_L`, we can use `user` table since `user` and `payment_received` are in the result of the previous join operation.

## Cautions and Notes
`cassandra-joinlib` will save some/all the incoming data from Cassandra due to the big size that cannot be fit into memory entirely. 
Both `HashJoinExecutor` and `NestedJoinExecutor` have partitioning mechanism and save the partitioned data inside a temporary folder called `tmpfolder`.
The result of the join operation will be saved inside the `result` folder.

Note that both `tmpfolder` and `result` will be created inside your current working directory, please make sure you have enough space available for them.




**Author : Rafi Adyatma**
---
Initially developed for my final project at Bandung Institute of Technology
