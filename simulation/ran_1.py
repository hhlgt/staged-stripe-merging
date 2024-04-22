import math
import random

from utils import *


class RandomPlacement(DataPlacement):
    def placement(self):
        rand_lower_bound = math.ceil((self.b + 1) / (self.g + 1)) * self.l + 1
        if self.b % (self.g + 1) == 0:
            rand_lower_bound -= self.l
        rand_cluster_num = rand_lower_bound * self.x
        cluster_id = 0
        for i in range(rand_cluster_num):
            new_cluster = Cluster(cluster_id)
            new_cluster.init_stripe_flag(self.x)
            self.clusters.append(new_cluster)
            cluster_id += 1
        self.num_of_clusters = rand_cluster_num
        remaining_local_groups = []
        for i in range(self.l):
            # place every x(g+1) blocks to randomly selected clusters
            if self.b >= self.g + 1:
                for j in range(self.x):
                    group = self.stripes[j].stripe_information[i]
                    for block_num in range(0, len(group), self.g + 1):
                        if block_num + self.g + 1 > len(group):  # derive the remaining local groups
                            remaining_local_groups.append(group[block_num:])
                        else:
                            cluster_id = random.randint(0, rand_cluster_num - 1)
                            while self.clusters[cluster_id].stripe_flag[j]:
                                cluster_id = random.randint(0, rand_cluster_num - 1)
                            self.clusters[cluster_id].stripe_flag[j] = True
                            for block_id in group[block_num:block_num + self.g + 1]:
                                block = Block(block_id, i, self.stripes[j].stripe_id)
                                block.place_to_cluster(cluster_id)
                                self.clusters[cluster_id].add_new_block(block)
                                self.stripes[j].place_to_cluster(cluster_id)
            else:
                for j in range(self.x):  # derive the remaining local groups
                    group = self.stripes[j].stripe_information[i]
                    remaining_local_groups.append(group)
        m = 0
        id_tag = []
        if len(remaining_local_groups) > 0:
            m = len(remaining_local_groups[0])
            if m == 1:
                for i in range(self.x):
                    cluster_id = random.randint(0, rand_cluster_num - 1)
                    while self.clusters[cluster_id].stripe_flag[i]:
                        cluster_id = random.randint(0, rand_cluster_num - 1)
                    self.clusters[cluster_id].stripe_flag[i] = True
                    id_tag.append(cluster_id)
                    for j in range(self.l):
                        k = j * self.x + i
                        group = remaining_local_groups[k]
                        for block_id in group:
                            block = Block(block_id, j, self.stripes[i].stripe_id)
                            block.place_to_cluster(cluster_id)
                            self.clusters[cluster_id].add_new_block(block)
                            self.stripes[i].place_to_cluster(cluster_id)
            else:  # randomly place the remaining local groups
                for i in range(0, self.l):
                    for j in range(self.x):
                        k = i * self.x + j
                        group = remaining_local_groups[k]
                        cluster_id = random.randint(0, rand_cluster_num - 1)
                        while self.clusters[cluster_id].stripe_flag[j]:
                            cluster_id = random.randint(0, rand_cluster_num - 1)
                        self.clusters[cluster_id].stripe_flag[j] = True
                        for block_id in group:
                            block = Block(block_id, i, self.stripes[j].stripe_id)
                            block.place_to_cluster(cluster_id)
                            self.clusters[cluster_id].add_new_block(block)
                        self.stripes[j].place_to_cluster(cluster_id)
        # place the global parity blocks
        if m == 1:
            for i in range(self.x):
                global_blocks = self.stripes[i].stripe_information[-1]
                for block_id in global_blocks:
                    block = Block(block_id, self.l, self.stripes[i].stripe_id)
                    block.place_to_cluster(id_tag[i])
                    self.clusters[id_tag[i]].add_new_block(block)
                self.stripes[i].place_to_cluster(id_tag[i])
                self.g_cluster_id_set.add(id_tag[i])
        else:
            for i in range(self.x):
                global_blocks = self.stripes[i].stripe_information[-1]
                cluster_id = random.randint(0, rand_cluster_num - 1)
                while self.clusters[cluster_id].stripe_flag[i]:
                    cluster_id = random.randint(0, rand_cluster_num - 1)
                self.clusters[cluster_id].stripe_flag[i] = True
                for block_id in global_blocks:
                    block = Block(block_id, self.l, self.stripes[i].stripe_id)
                    block.place_to_cluster(cluster_id)
                    self.clusters[cluster_id].add_new_block(block)
                self.stripes[i].place_to_cluster(cluster_id)
                self.g_cluster_id_set.add(cluster_id)


class StagedStripeMergingForRan(StagedStripeMerging):
    def stripe_merging(self, debug=False):
        self.check_parameter()
        stage_num = 1
        m_flag = True
        for xi in self.stage_x:
            LC = 0
            DC = 0
            GC = 0
            larger_stripe_num = 0
            new_stripes = []
            new_g_cluster_set = set()
            for s in range(0, self.x, xi):  # for every xi stripes
                if xi == 1:
                    break
                larger_stripe_id = 'S' + str(larger_stripe_num)
                # generate new stripe information
                larger_stripe = Stripe(self.placement.encoding_type, larger_stripe_id, self.k * xi, self.l, self.g)
                for group_num in range(self.l):
                    larger_stripe.stripe_information.append([])
                    for j in range(s, s + xi):
                        group = self.placement.stripes[j].stripe_information[group_num]
                        for block_id in group:  # data blocks for each group
                            if block_id[0] == 'D':
                                new_block_id = block_id
                                larger_stripe.stripe_information[group_num].append(new_block_id)
                    # local parity block for each group
                    new_local_parity_id = 'L' + str(group_num + larger_stripe_num * self.l)
                    larger_stripe.stripe_information[group_num].append(new_local_parity_id)
                larger_stripe.stripe_information.append([])
                for num in range(self.g):  # global parity blocks
                    new_global_parity_id = 'G' + str(num + larger_stripe_num * self.g)
                    larger_stripe.stripe_information[self.l].append(new_global_parity_id)
                # find the clusters that the xi stripes span
                old_stripe_id = []
                for j in range(s, s + xi):
                    larger_stripe.place2cluster = \
                        larger_stripe.place2cluster.union(self.placement.stripes[j].place2cluster)
                    old_stripe_id.append(self.placement.stripes[j].stripe_id)
                cluster_set = larger_stripe.place2cluster

                # calculate LC
                global_c_set = set()
                # local parity blocks recalculation needs derive old local parity blocks
                for group_num in range(self.l):
                    local_c_set = set()
                    for cluster_id in cluster_set:
                        for stripe_id in old_stripe_id:
                            if self.placement.clusters[cluster_id].find_local_parity_block(stripe_id, group_num):
                                local_c_set.add(cluster_id)
                                break
                    LC += len(local_c_set) - 1
                if self.placement.encoding_type == "Optimal Cauchy LRC":
                    for cluster_id in cluster_set:
                        for stripe_id in old_stripe_id:
                            if self.placement.clusters[cluster_id].find_global_parity_block(stripe_id):
                                global_c_set.add(cluster_id)

                # delete the old parity blocks
                # figure out the clusters to place new local parity blocks and new global parity blocks
                local_cluster_id = []
                for j in range(self.l):
                    local_cluster_id.append(-1)
                global_cluster_id = -1
                for cluster_id in cluster_set:
                    # merge to new larger stripe
                    for stripe_id in old_stripe_id:
                        for j in range(self.placement.clusters[cluster_id].num_of_blocks):
                            self.placement.clusters[cluster_id].blocks[j].merge_to_larger_stripe(stripe_id,
                                                                                                 larger_stripe_id)

                    # delete old parity blocks
                    if self.placement.clusters[cluster_id].find_local_parity_block(larger_stripe_id):
                        group_set = self.placement.clusters[cluster_id].remove_block(larger_stripe_id, 'L')
                        for group_id in group_set:
                            local_cluster_id[group_id] = cluster_id
                        group_set = self.placement.clusters[cluster_id].remove_block(larger_stripe_id, 'G')
                        if len(group_set) > 0:
                            global_cluster_id = cluster_id
                        if not self.placement.clusters[cluster_id].find_local_parity_block():
                            if not self.placement.clusters[cluster_id].find_global_parity_block():
                                self.placement.clusters[cluster_id].type = 'D'
                            else:
                                self.placement.clusters[cluster_id].type = 'G'
                    elif self.placement.clusters[cluster_id].find_global_parity_block(larger_stripe_id):
                        self.placement.clusters[cluster_id].remove_block(larger_stripe_id, 'G')
                        global_cluster_id = cluster_id
                        if not self.placement.clusters[cluster_id].find_global_parity_block() and \
                                self.placement.clusters[cluster_id].type == 'G':
                            self.placement.clusters[cluster_id].type = 'D'

                # for Optimal Cauchy LRC, local parity blocks recalculation needs derive old and new global parities
                if self.placement.encoding_type == "Optimal Cauchy LRC":
                    for j in range(self.l):
                        for cid in global_c_set:
                            if local_cluster_id[j] != cid:
                                LC += self.g
                        if global_cluster_id != local_cluster_id[j]:
                            LC += self.g

                for cluster_id in cluster_set:
                    if cluster_id != global_cluster_id:  # don't include the cluster that will recalculate new ones
                        # transfer data blocks for global parity block recalculation
                        cnt = self.placement.clusters[cluster_id].count_blocks_num(larger_stripe_id)
                        m = cnt[1]
                        GC += int(m)

                # recalculate local parity blocks and global parity blocks
                for global_id in range(self.g):
                    new_global_parity_block_id = 'G' + str(global_id + larger_stripe_num * self.g)
                    global_parity_block = Block(new_global_parity_block_id, self.l, larger_stripe_id)
                    global_parity_block.place_to_cluster(global_cluster_id)
                    self.placement.clusters[global_cluster_id].add_new_block(global_parity_block)
                new_g_cluster_set.add(global_cluster_id)
                for group_id in range(self.l):
                    new_local_parity_block_id = 'L' + str(group_id + larger_stripe_num * self.l)
                    local_parity_block = Block(new_local_parity_block_id, group_id, larger_stripe_id)
                    local_parity_block.place_to_cluster(local_cluster_id[group_id])
                    self.placement.clusters[local_cluster_id[group_id]].add_new_block(local_parity_block)

                # overall goal: to keep optimal data placement for each new local group after each stage of merging
                # data relocation
                new_cluster_set = set()
                for cluster_id in cluster_set:
                    # figure out the group that has the most blocks in the cluster
                    cnt = self.placement.clusters[cluster_id].count_blocks_num(larger_stripe_id)
                    m = cnt[0]
                    data_to_move = 0
                    max_group_id = cnt[2]
                    max_group_num = cnt[3]
                    other_group_to_move = 0
                    # firstly, we never relocate any global parity block
                    # relocate the blocks that are not from the group that has the most blocks in the cluster
                    # and relocate the blocks that violate single-cluster fault tolerance
                    if m > self.g + 1:
                        data_to_move = m - (self.g + 1)
                        if 0 < max_group_num < self.g + 1:
                            data_to_move = m - max_group_num
                        other_group_to_move = m - max_group_num
                        if self.placement.clusters[cluster_id].find_global_parity_block(larger_stripe_id):
                            data_to_move = m - self.g
                            other_group_to_move = data_to_move
                            max_group_id = self.l
                    elif m == self.g + 1 and \
                            self.placement.clusters[cluster_id].find_global_parity_block(larger_stripe_id):
                        if not self.placement.clusters[cluster_id].find_local_parity_block(larger_stripe_id) or \
                                self.m != 0:
                            data_to_move = m - self.g
                            other_group_to_move = data_to_move
                            max_group_id = self.l
                    elif m != max_group_num:
                        data_to_move = m - max_group_num
                        other_group_to_move = data_to_move
                    DC += data_to_move
                    moved = 0  # the number of data blocks that have been moved to other clusters
                    index = 0
                    while index < self.placement.clusters[cluster_id].num_of_blocks:
                        if moved == data_to_move:
                            break
                        block = self.placement.clusters[cluster_id].blocks[index]
                        # relocate the blocks that are not from the group that have the most blocks in the cluster
                        # or relocate the blocks from the group but violate single-cluster fault tolerance
                        if block.map2stripe == larger_stripe_id and \
                                ((block.map2group != max_group_id and moved < other_group_to_move) or
                                 (block.map2group == max_group_id and moved >= other_group_to_move)):
                            flag = False
                            # firstly consider to place to the cluster that stores less than g+1 blocks from xi stripes
                            # the group that the moving block comes from should be same with most of blocks in cluster
                            # and not place to the cluster that stores the new global parity block
                            for cid in cluster_set:
                                if cid != cluster_id and \
                                        not self.placement.clusters[cid].find_global_parity_block(larger_stripe_id):
                                    cnt_ = self.placement.clusters[cid].count_blocks_num(larger_stripe_id)
                                    m_ = cnt_[0]
                                    if 0 < m_ < self.g + 1 and block.map2group == cnt_[2]:
                                        block.place_to_cluster(cid)
                                        self.placement.clusters[cid].add_new_block(block)
                                        self.placement.clusters[cluster_id].blocks.pop(index)
                                        self.placement.clusters[cluster_id].num_of_blocks -= 1
                                        if block.block_id[0] == 'D':
                                            self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                        index -= 1
                                        flag = True
                                        break
                            # then consider to place blocks to an existed new cluster
                            if not flag:
                                for cid in new_cluster_set:
                                    if cid != cluster_id and \
                                            not self.placement.clusters[cid].find_global_parity_block(larger_stripe_id):
                                        cnt_ = self.placement.clusters[cid].count_blocks_num(larger_stripe_id)
                                        m_ = cnt_[0]
                                        if 0 < m_ < self.g + 1 and block.map2group == cnt_[2]:
                                            block.place_to_cluster(cid)
                                            self.placement.clusters[cid].add_new_block(block)
                                            self.placement.clusters[cluster_id].blocks.pop(index)
                                            self.placement.clusters[cluster_id].num_of_blocks -= 1
                                            if block.block_id[0] == 'D':
                                                self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                            index -= 1
                                            flag = True
                                            break
                            # consider to select an empty cluster and place into it
                            if not flag:
                                for cid in range(self.placement.num_of_clusters):
                                    if cid != cluster_id and self.placement.clusters[cid].num_of_blocks == 0:
                                        block.place_to_cluster(cid)
                                        self.placement.clusters[cid].add_new_block(block)
                                        self.placement.clusters[cluster_id].blocks.pop(index)
                                        self.placement.clusters[cluster_id].num_of_blocks -= 1
                                        if block.block_id[0] == 'D':
                                            self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                        index -= 1
                                        new_cluster_set.add(cid)
                                        break
                            moved += 1
                        index += 1
                    if self.placement.clusters[cluster_id].find_local_parity_block():
                        self.placement.clusters[cluster_id].type = 'L'
                    elif self.placement.clusters[cluster_id].find_global_parity_block():
                        self.placement.clusters[cluster_id].type = 'G'
                    else:
                        self.placement.clusters[cluster_id].type = 'D'

                # some data blocks of the larger stripe are placed to other new clusters
                larger_stripe.place2cluster = larger_stripe.place2cluster.union(new_cluster_set)

                # try to merge tails and keep each group in the fewest cluster
                # to achieve optimal data placement of each stripe after each stage of merging
                bi = self.k / self.l
                # c_a is the minimum number of clusters that each group spans after this stage of merging
                c_a = math.ceil((bi * xi + 1) / (self.g + 1))
                for i in range(self.l):
                    # calculate the number of blocks from group i of new stripe in each cluster
                    counter = self.placement.calculate_cluster_num(larger_stripe_id, i)
                    # c_b is currently the minimum number of clusters that each group spans
                    c_b = np.count_nonzero(counter)
                    if c_b > c_a:
                        c_m = c_b - c_a
                        # move the data blocks in c_m clusters that have fewest data blocks from group i of new stripe
                        counter_index = counter.argsort()
                        c_cnt = 0
                        for cluster_id in counter_index:
                            if c_cnt == c_m:
                                break
                            block_num = counter[cluster_id]
                            if block_num != 0:
                                LC += block_num
                                c_cnt += 1
                                moved = 0
                                cluster = self.placement.clusters[cluster_id]
                                index = 0
                                while cluster.num_of_blocks > index >= 0:
                                    if moved == block_num:
                                        break
                                    block = cluster.blocks[index]
                                    if block.map2stripe == larger_stripe_id and block.map2group == i:
                                        for cid in cluster_set:
                                            # consider to place to the cluster that stores less than g+1 blocks
                                            # from group i of new stripe
                                            # don not place to the cluster that stores global parity blocks
                                            if cid != cluster_id and \
                                                    not self.placement.clusters[cid].find_global_parity_block(
                                                        larger_stripe_id):
                                                cnt_ = self.placement.clusters[cid].count_blocks_num(larger_stripe_id)
                                                m_ = cnt_[0]
                                                if 0 < m_ < self.g + 1 and block.map2group == cnt_[2]:
                                                    block.place_to_cluster(cid)
                                                    self.placement.clusters[cid].add_new_block(block)
                                                    self.placement.clusters[cluster_id].blocks.pop(index)
                                                    self.placement.clusters[cluster_id].num_of_blocks -= 1
                                                    if block.block_id[0] == 'D':
                                                        self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                                    index -= 1
                                                    moved += 1
                                                    break
                                    index += 1

                # remove the empty clusters from the set
                empty_set = set()
                for cluster_id in cluster_set:
                    if self.placement.clusters[cluster_id].num_of_blocks == 0:
                        empty_set.add(cluster_id)
                for empty_set_id in empty_set:
                    cluster_set.remove(empty_set_id)

                for cluster_id in cluster_set:
                    if self.placement.clusters[cluster_id].find_local_parity_block():
                        self.placement.clusters[cluster_id].type = 'L'
                    elif self.placement.clusters[cluster_id].find_global_parity_block():
                        self.placement.clusters[cluster_id].type = 'G'
                    else:
                        self.placement.clusters[cluster_id].type = 'D'

                new_stripes.append(larger_stripe)
                larger_stripe_num += 1
            if xi > 1:
                self.placement.stripes = new_stripes
                self.placement.g_cluster_id_set = new_g_cluster_set
            stage_num += 1
            self.x = int(self.x / xi)
            self.k = self.k * xi
            self.LC.append(LC)
            self.DC.append(DC)
            self.GC.append(GC)
            self.TOTAL.append(LC + DC + GC)
            if debug:
                print('-----------------------Stage {}-----------------------'.format(stage_num - 1))
                self.placement.print_placement_res()
                print('Cost: LC = {}, DC = {}, GC = {}, Total = {}'.format(LC, DC, GC, LC + DC + GC))
                print('-----------------------------------------------------')


if __name__ == '__main__':
    encoding_type = "Optimal Cauchy LRC"
    x_list = [2, 2]
    k = 8
    l = 2
    g = 2
    x = np.prod(x_list)
    ran = RandomPlacement(encoding_type, x, k, l, g)
    ran.placement()
    ran.print_placement_res()
    staged_merge = StagedStripeMergingForRan(len(x_list), x_list, k, l, g, ran)
    staged_merge.stripe_merging(True)
    staged_merge.print_res('Random')
