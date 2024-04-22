## Prototype

- Architecture

  <img src="img/post/Prototype Design/image-20230413220511457.png" alt="image-20230413220511457" style="zoom:80%;" />

  - Coordinator only manage meta data information and does not transfer any original data.

- Specify the size of data object, adopt per-object coding scheme and the construction of Azure-LRC, divide an object into k blocks and encoding into a stripe.

- Mainly consider 4 types of operation

  - set, storage the data value to the system as a file with the key as the filename
    - set(key, value)
  - get, get the value of the specified key from the system
    - get(key)
  - delete, delete the blocks of the specified key or stripe
    - delete(key) or delete(stripe_id)
  - merge, merge every specified number of stripes into a larger stripe
    - merge(number_of_stripes_per_merge)

- Adopt socket to transfer original data, such as the data transferring between client and proxy, and adopt rpc to transfer meta data among client, coordinator and proxy.

- Data nodes access data by directly writing to or reading from the disk.

### Procedure of set

1. the client calls `set(key, value)` to start write process, firstly, client send the write command, key and the length of value to the coordinator.
2. the coordinator receives key and the length of  value from client, then
   - generates data placement scheme for the stripe, updates the meta data information of object and stripe;
   - selects a proxy to divide and encode, informs the proxy of the placement scheme and lets the proxy prepare to accept data from the client;
   - at the same time, informs the client of the address of the selected proxy.
3. the client receives the address of the selected proxy and sends the data object to the proxy.
4. the selected proxy receives data object from client, divides and encodes the object into a stripe, places each block of the stripe to the data node specified by the data placement scheme. After all the block are saved, reports ACK to coordinator.
5. the coordinator commits the meta data information.

### Procedure of get

1. the client calls `get(key)` to start the read process, and the client sends the read command and key to the coordinator.
2. the coordinator receives key from client, then
   - gets the stripe and block information from the meta data information, finds out the address of data node that place each block;
   - selects a proxy to read the blocks and splice them, and sends the meta data and client's IP to the proxy.
3. the selected proxy read all the data blocks from data nodes and splice or just read enough blocks and decode to get the original data object.
4. the selected proxy sends ACK to the coordinator and sends the data object to the client.
5. the client receives the data.

### Procedure of delete

1. the client calls `delete(key)` or `delete(stripe_id)` to start the delete process, and the client sends the delete command and key or stripe_id to the coordinator.
2. the coordinator receives key or stripe_id from client, then
   - gets the stripe and block information from the meta data information, finds out the address of data node that place each block;
   - selects a proxy to delete the blocks and informs the proxy of the delete plan.
3. the selected proxy receives the delete plan and call data nodes to delete the blocks.
4. the selected proxy sends ACK to the coordinator, the coordinator updates the meta data information and sends ACK to the client.

### Procedure of stripe merging

1. the client calls `merge(number)` to start the merge process, and the client sends the number of stripes to merge into a large stripe to the coordinator.
2. For each large stripe to be merged, the coordinator first generates the meta data information of the large stripe while still keeping the information of the small ones, generates the local parity block recalculation scheme for each local group in the large stripe, and assigns a proxy each to read the old local parity blocks and recalculate new one
3. the coordinator generates the global parity block recalculation scheme and designates a certain proxy as the main proxy and selects several other proxies as helper proxies.
5. the coordinator sends the main global parity block recalculation plan to the main proxy,
   - in the plan, the coordinator tells the main proxy which block to read on which data node in the local cluster, the address of the helper proxy and which blocks will be retrieved from the helper proxy, and whether to take encode-and-transfer policy,
   - the main proxy collects all the data blocks or partial blocks, and then recalculate new global parity blocks for the large stripe,
   - the main proxy sets the new one to the data node specified by the coordinator,
   - the main proxy deletes the old global parity blocks in the  corresponding data nodes,
   - the main proxy sends ACK to the coordinator.
6. the coordinator sends the help global parity block recalculation plan to the helper proxy,
   - in the plan, the coordinator tells the helper proxy which block to read on which data node in the local cluster, the address of the main proxy, and whether to take encode-and-transfer policy,
   - the helper proxy collects all the data blocks, if take encode-and-transfer policy,  firstly encodes the data blocks into a partial block, then sends the partial block to the main proxy, if not, directly sends all the data blocks to the main proxy,
   - the helper proxy sends ACK to the coordinator.
6. after receiving the ACK from the proxies, the coordinator generates the local parity block recalculation scheme and act the same as global parity block recalculation.
7. Then, the coordinator generates the data block relocation scheme (including the blocks that need to be moved in order to maintain the single-stripe optimal data placement and the blocks that need to be moved if the single-cluster fault tolerance is violated), and for each block, the coordinator assigns a proxy to be responsible for relocation and sends the plan,
   - in the plan, the coordinator tells the proxy which block needs to be read on which source data node and which block needs to be placed in which destination data node, as well as the address of the relevant data nodes,
   - the proxy gets each block from source data node and sets it to the destination data node, finally delete the block in the source data node
   - the proxy sends ACK to the coordinator.
8. after receiving the ACK from the proxy, the coordinator updates the meta data information of stripes or keys, sends ACK to the client and ends the merge process.