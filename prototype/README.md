## Prototype

The architecture follows master-worker style, like many state-of-art distributed file storage such as HDFS and Ceph. Four major components are client, coordinator, proxy and datanode. The implementation is introduced in the `doc/` directory.

### Environment Configuration

- Required packages

  * grpc v1.50

  * asio 1.24.0

  * jerasure

- we call them third_party libraries, and the source codes are provided in the `third_party/` directory.

- Before installing these packages, you should install the dependencies of grpc.

  - ```
    sudo apt install -y build-essential autoconf libtool pkg-config
    ```

- Run the following command to install these packages

  - ```
    sh install_third_party.sh
    ```

### Compile and Run

- Compile

```
cd project
sh compile.sh
```

- Run

```
sh run_proxy_datanode.sh
cd project/cmake/build
./run_coordinator
./run_client false Azure_LRC Optimal OPT 8 2 2 100 2 2 0 1024
```

- The parameter meaning of `run_client`

```
./run_client partial_decoding encode_type singlestripe_placement_type multistripes_placement_type k l g_m upload_stripes_num stage_x1 stage_x2 stage_x3 value_length
```

#### Tips. 

- `partial_decoding` denotes if apply `encode-and-transfer`.
- `encode_type` denotes the encoding type of a single stripe, such as `RS`,  `Azure_LRC`, etc. Now support  `Azure_LRC` and `Optimal_Cauchy_LRC`.
- `singlestripe_placement_type` denotes the data placement type of a single stripe, such as `Flat`, `Random` and `Optimal`. Now only support  `Optimal`.
- `multistripes_placement_type` denotes the data placement type of multiple stripes, such as `Ran`, `DIS`, `AGG` and `OPT`. Now all are supported.
- In our experiment, we mainly test 2 or 3 stages of stripe merging, and the `stage_xi` denotes the number of stripes to merge into a large-size stripe in `i-th` stage. 
- `value_length` is the object size of each object to form a stripe initially, with the unit of `KiB`.

#### Attention

> About implementation.

For stripe merging, the coordinator introduces a variable called `merge_groups` to manage stripe merging, for the number of stripes in each merge group, it will be the lesser of the following two numbers.

- the maximum number of stripes that matches specified placement scheme such as `OPT`, `AGG`, `DIS` and `Ran`;
- the product of `xi` (for example, equal to `x1*x2*x3` when there is three stages of merging, `xi!=0`), the total number of stripes before the first stage of merging to be merged into a large stripe after all the stages of merging.

We recommend that the former is always larger than the latter, otherwise the latter stages of stripe merging can not be processed. Thus, the number of clusters, `upload_stripes_num`, `stage_xi`, `k`, `l` and `g_m` should be carefully set.

- For example, when `(k, l, g_m) = (8, 2, 2)`, we consider two stages of stripe merging and `stage_x1 = stage_x2 = 2`, then the number of stripes in each merge group should be `4`, and `upload_stripes_num` should be divisible by `4`, and the number of cluster should be `20` at least, since `4` stripes of `(8, 2, 2)` should be placed to `20` clusters with `DIS` placement scheme (`5` clusters for each stripe with optimal data placement).

### Other

#### Directory

- directory `doc/`  is the introduction of system implementation.
- directory `project/` is the system implementation.
- create directory `data/` to store the original test data object for the client to upload.
- create directory `client_get/` to store the data object getting from proxy for the client.
- create directory `storage/` to store the data blocks for data nodes.
- create directory `run_cluster_sh/` to store the running shell for each cluster.

#### Tools

- use `small_tools/generator_file.py` to generate files with random string of specified length.
- use `small_tools/generator_sh.py` to generate configuration file and running shell for proxy and data node.

