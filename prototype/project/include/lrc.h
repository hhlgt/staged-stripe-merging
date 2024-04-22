#ifndef LRC_H
#define LRC_H
#include "jerasure.h"
#include "reed_sol.h"
#include "cauchy.h"
#include "meta_definition.h"

namespace ECProject
{
    bool lrc_make_matrix(int k, int g, int real_l, int *final_matrix, EncodeType encode_type);
    void dfs(std::vector<int> temp, std::shared_ptr<std::vector<std::vector<int>>> ans, int cur, int n, int k);
    bool combine(std::shared_ptr<std::vector<std::vector<int>>> ans, int n, int k);
    bool encode(int k, int m, int real_l, char **data_ptrs, char **coding_ptrs, int blocksize, EncodeType encode_type);
    bool decode(int k, int m, int real_l, char **data_ptrs, char **coding_ptrs, std::shared_ptr<std::vector<int>> erasures, int blocksize, EncodeType encode_type, bool repair = false);
    bool check_received_block(int k, int expect_block_number, std::shared_ptr<std::vector<int>> shards_idx_ptr, int shards_ptr_size = -1);
    bool check_k_data(std::vector<int> erasures, int k);
    int check_decodable_azure_lrc(int k, int g, int l, std::vector<int> failed_block, std::vector<int> new_matrix);
    bool encode_partial_blocks_for_gr(int k, int m, char **data_ptrs, char **coding_ptrs, int blocksize, std::shared_ptr<std::vector<int>> data_idx_ptrs, int block_num, EncodeType encode_type);
    bool perform_addition(char **data_ptrs, char **coding_ptrs, int blocksize, int block_num, int parity_num);
}
#endif