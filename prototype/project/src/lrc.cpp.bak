#include <lrc.h>

void ECProject::dfs(std::vector<int> temp, std::shared_ptr<std::vector<std::vector<int>>> ans, int cur, int n, int k)
{
    if (int(temp.size()) + (n - cur + 1) < k)
    {
        return;
    }
    if (int(temp.size()) == k)
    {
        ans->push_back(temp);
        return;
    }
    temp.push_back(cur);
    dfs(temp, ans, cur + 1, n, k);
    temp.pop_back();
    dfs(temp, ans, cur + 1, n, k);
}

bool ECProject::combine(std::shared_ptr<std::vector<std::vector<int>>> ans, int n, int k)
{
    std::vector<int> temp;
    dfs(temp, ans, 1, n, k);
    return true;
}

// check if any data block is failed
bool ECProject::check_k_data(std::vector<int> erasures, int k)
{
    int flag = 1;
    for (int i = 0; i < k; i++)
    {
        if (std::find(erasures.begin(), erasures.end(), i) != erasures.end())
        {
            flag = 0;
        }
    }
    if (flag)
    {
        return true;
    }

    return false;
}

// generate encoding matrix for lrc
bool ECProject::lrc_make_matrix(int k, int g, int real_l, int *final_matrix, EncodeType encode_type)
{
    int r = (k + real_l - 1) / real_l;
    int *matrix = NULL;

    if (encode_type == Azure_LRC){
        matrix = reed_sol_vandermonde_coding_matrix(k, g + 1, 8); //(k, g, w)
    }else if(encode_type == Optimal_Cauchy_LRC){
        matrix = cauchy_good_general_coding_matrix(k, g + 1, 8); //(k, g, w)
    }
    
    if (matrix == NULL)
    {
        std::cout << "matrix == NULL" << std::endl;
    }

    if (final_matrix == NULL)
    {
        std::cout << "final_matrix == NULL" << std::endl;
    }
    bzero(final_matrix, sizeof(int) * k * (g + real_l));

    for (int i = 0; i < g; i++)
    {
        for (int j = 0; j < k; j++)
        {
            final_matrix[i * k + j] = matrix[(i + 1) * k + j];
        }
    }

    for (int i = 0; i < real_l; i++)
    {
        for (int j = 0; j < k; j++)
        {
            if (i * r <= j && j < (i + 1) * r)
            {
                final_matrix[(i + g) * k + j] = 1;
            }
        }
    }

    free(matrix);
    return true;
}

// encode
bool ECProject::encode(int k, int g_m, int real_l, char **data_ptrs, char **coding_ptrs, int blocksize, EncodeType encode_type)
{
    std::vector<int> new_matrix((g_m + real_l) * k, 0);
    lrc_make_matrix(k, g_m, real_l, new_matrix.data(), encode_type);
    jerasure_matrix_encode(k, g_m + real_l, 8, new_matrix.data(), data_ptrs, coding_ptrs, blocksize);
    return true;
}

// decode
bool ECProject::decode(int k, int g_m, int real_l, char **data_ptrs, char **coding_ptrs, std::shared_ptr<std::vector<int>> erasures, int blocksize, EncodeType encode_type, bool repair)
{

    if (encode_type == Azure_LRC || encode_type == Optimal_Cauchy_LRC)
    {
        std::vector<int> matrix((g_m + real_l) * k, 0);
        lrc_make_matrix(k, g_m, real_l, matrix.data(), encode_type);
        if (!repair)
        {
            if (check_k_data(*erasures, k)) // if there is no failed data block, return true
            {
                return true;
            }
        }
        // decode the original data blocks by any k blocks from the stripe
        if (jerasure_matrix_decode(k, g_m + real_l, 8, matrix.data(), 0, erasures->data(), data_ptrs, coding_ptrs, blocksize) == -1)
        {
            std::vector<int> new_erasures(g_m + real_l + 1, 1);
            int survival_number = k + g_m + real_l - erasures->size() + 1;
            std::vector<int> survival_index;
            auto part_new_erasure = std::make_shared<std::vector<std::vector<int>>>();
            for (int i = 0; i < int(erasures->size() - 1); i++)
            {
                new_erasures[i] = (*erasures)[i];
            }
            new_erasures[g_m + real_l] = -1;

            for (int i = 0; i < k + g_m + real_l; i++)
            {
                if (std::find(erasures->begin(), erasures->end(), i) == erasures->end())
                {
                    survival_index.push_back(i);
                }
            }
            if (survival_number > k)
            {
                combine(part_new_erasure, survival_index.size(), survival_number - k);
            }
            for (int i = 0; i < int(part_new_erasure->size()); i++)
            {
                for (int j = 0; j < int((*part_new_erasure)[i].size()); j++)
                {
                    new_erasures[erasures->size() - 1 + j] = survival_index[(*part_new_erasure)[i][j] - 1];
                }

                if (jerasure_matrix_decode(k, g_m + real_l, 8, matrix.data(), 0, new_erasures.data(), data_ptrs, coding_ptrs, blocksize) != -1)
                {
                    return true;
                    break;
                }
            }
        }
        else
        {
            return true;
        }
        std::cout << "undecodable!!!!!!!!!!!!" << std::endl;
    }
    return false;
}

bool ECProject::check_received_block(int k, int expect_block_number, std::shared_ptr<std::vector<int>> shards_idx_ptr, int shards_ptr_size)
{
    if (shards_ptr_size != -1)
    {
        if (int(shards_idx_ptr->size()) != shards_ptr_size)
        {
            return false;
        }
    }

    if (int(shards_idx_ptr->size()) >= expect_block_number)
    {
        return true;
    }
    else if (int(shards_idx_ptr->size()) == k) 
    {
        for (int i = 0; i < k; i++)
        {
            if (std::find(shards_idx_ptr->begin(), shards_idx_ptr->end(), i) == shards_idx_ptr->end())
            {
                return false;
            }
        }
    }
    else
    {
        return false;
    }
    return true;
}
int ECProject::check_decodable_azure_lrc(int k, int g, int l, std::vector<int> failed_block, std::vector<int> new_matrix)
{
    std::vector<int> survive_block;
    for (int i = 0; i < k + l + g; i++)
    {
        if (std::find(failed_block.begin(), failed_block.end(), i) == failed_block.end())
        {
            survive_block.push_back(i);
        }
    }
    if (survive_block.size() != size_t(k))
    {
        return -2;
    }
    std::set<int> group_number;
    for (int block_index : survive_block)
    {
        group_number.insert(block_index / l);
    }
    if (survive_block.size() > g + group_number.size())
    {
        return -1;
    }

    std::vector<int> matrix((k + g + l) * k, 0);
    for (int i = 0; i < k; i++)
    {
        matrix[i * k + i] = 1;
    }
    for (int i = 0; i < (g + l) * k; i++)
    {
        matrix[k * k + i] = new_matrix[i];
    }
    std::vector<int> k_k_matrix(k * k, 0);

    for (size_t i = 0; i < survive_block.size(); i++)
    {
        for (int j = 0; j < k; j++)
        {
            k_k_matrix[i * k + j] = matrix[survive_block[i] * k + j];
        }
    }
    if (jerasure_invertible_matrix(k_k_matrix.data(), k, 8) == 0)
    {
        return -1;
    }
    return 1;
}