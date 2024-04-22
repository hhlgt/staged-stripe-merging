import math
from utils import *


class DispersedPlacement(DataPlacement):

    def placement(self):
        remaining_local_groups = []
        cluster_id = 0
        for i in range(self.l):
            # disperse every x(g+1) blocks across x clusters
            if self.b >= self.g + 1:
                for j in range(self.x):
                    group = self.stripes[j].stripe_information[i]
                    for block_num in range(0, len(group), self.g + 1):
                        if block_num + self.g + 1 > len(group):     # derive the remaining local groups
                            remaining_local_groups.append(group[block_num:])
                        else:
                            new_cluster = Cluster(cluster_id)
                            for block_id in group[block_num:block_num + self.g + 1]:
                                block = Block(block_id, i, self.stripes[j].stripe_id)
                                block.place_to_cluster(cluster_id)
                                new_cluster.add_new_block(block)
                                self.stripes[j].place_to_cluster(cluster_id)
                            self.clusters.append(new_cluster)
                            cluster_id += 1
            else:
                for j in range(self.x):     # derive the remaining local groups
                    group = self.stripes[j].stripe_information[i]
                    remaining_local_groups.append(group)
        m = 0
        id_tag = cluster_id
        if len(remaining_local_groups) > 0:
            m = len(remaining_local_groups[0])
            # place all the remaining local groups to the cluster that will also place global parity blocks
            # each stripe with a single cluster
            if m == 1:
                for i in range(self.x):
                    new_cluster = Cluster(cluster_id)
                    for j in range(self.l):
                        k = j * self.x + i
                        group = remaining_local_groups[k]
                        for block_id in group:
                            block = Block(block_id, j, self.stripes[i].stripe_id)
                            block.place_to_cluster(cluster_id)
                            new_cluster.add_new_block(block)
                            self.stripes[i].place_to_cluster(cluster_id)
                    self.clusters.append(new_cluster)
                    cluster_id += 1
            else:   # disperse every x remaining local groups across x clusters
                for i in range(0, self.l):
                    for j in range(self.x):
                        k = i * self.x + j
                        group = remaining_local_groups[k]
                        new_cluster = Cluster(cluster_id)
                        for block_id in group:
                            block = Block(block_id, i, self.stripes[j].stripe_id)
                            block.place_to_cluster(cluster_id)
                            new_cluster.add_new_block(block)
                        self.stripes[j].place_to_cluster(cluster_id)
                        self.clusters.append(new_cluster)
                        cluster_id += 1
        # disperse x sets of the global parity blocks across x clusters
        if m == 1:
            for i in range(self.x):
                global_blocks = self.stripes[i].stripe_information[-1]
                for block_id in global_blocks:
                    block = Block(block_id, self.l, self.stripes[i].stripe_id)
                    block.place_to_cluster(id_tag)
                    self.clusters[id_tag].add_new_block(block)
                self.stripes[i].place_to_cluster(id_tag)
                self.g_cluster_id_set.add(id_tag)
                id_tag += 1
        else:
            for i in range(self.x):
                global_blocks = self.stripes[i].stripe_information[-1]
                new_cluster = Cluster(cluster_id)
                for block_id in global_blocks:
                    block = Block(block_id, self.l, self.stripes[i].stripe_id)
                    block.place_to_cluster(cluster_id)
                    new_cluster.add_new_block(block)
                self.stripes[i].place_to_cluster(cluster_id)
                self.clusters.append(new_cluster)
                self.g_cluster_id_set.add(cluster_id)
                cluster_id += 1
        self.num_of_clusters = cluster_id


class StagedStripeMergingForDis(StagedStripeMerging):

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
                if self.placement.encoding_type == "Optimal Cauchy LRC":
                    for group_num in range(self.l):
                        local_c_set = set()
                        for cluster_id in cluster_set:
                            for stripe_id in old_stripe_id:
                                if self.placement.clusters[cluster_id].find_local_parity_block(stripe_id, group_num):
                                    local_c_set.add(cluster_id)
                                # local parity blocks recalculation needs derive global parities
                                if self.placement.clusters[cluster_id].find_global_parity_block(stripe_id):
                                    local_c_set.add(cluster_id)
                        LC += len(local_c_set) - 1
                else:
                    LC += (xi - 1) * self.l

                # transfer data blocks or partial blocks for global parity block recalculation
                for cluster_id in cluster_set:
                    m = self.placement.clusters[cluster_id].num_of_data_blocks
                    if m <= self.g:
                        GC += int(m)
                    else:
                        GC += self.g
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
                    if self.placement.clusters[cluster_id].type == 'L':
                        group_set = self.placement.clusters[cluster_id].remove_block(larger_stripe_id, 'L')
                        for group_id in group_set:
                            local_cluster_id[group_id] = cluster_id
                        if len(group_set) > 0:
                            self.placement.clusters[cluster_id].type = 'D'
                        group_set = self.placement.clusters[cluster_id].remove_block(larger_stripe_id, 'G')
                        if len(group_set) > 0:
                            global_cluster_id = cluster_id
                    elif self.placement.clusters[cluster_id].type == 'G':
                        self.placement.clusters[cluster_id].remove_block(larger_stripe_id, 'G')
                        global_cluster_id = cluster_id
                        self.placement.clusters[cluster_id].type = 'D'

                # recalculate global parity blocks and local parity blocks
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

                bi = self.k / self.l
                c_b = math.ceil((bi + 1) / (self.g + 1)) * xi  # number of clusters that xi stripes span before merge
                c_a = math.ceil((bi * xi + 1) / (self.g + 1))  # number of clusters that xi stripes span after merge
                m = self.g
                if stage_num == 1:
                    m = bi % (self.g + 1)
                    if m == 0:  # if m = 0 in the first stage, there is no need to migrate any data blocks in any stage
                        m_flag = False
                # try to keep blocks from the same group in the fewest clusters
                if m_flag and m > 0 and c_b > c_a:
                    c_m = c_b - c_a
                    LC += c_m * m * self.l
                    for j in range(self.l):
                        c_cnt = 0
                        for cluster_id in cluster_set:
                            if c_cnt >= c_m:
                                break
                            cluster = self.placement.clusters[cluster_id]
                            # for each group, relocate c_m clusters of data blocks
                            # relocate data blocks in the cluster that only have m data blocks
                            if cluster.type == 'D' and cluster.num_of_blocks == m and cluster.blocks[0].map2group == j:
                                index = 0
                                while cluster.num_of_blocks > index >= 0:
                                    block = cluster.blocks[index]
                                    flag = False
                                    # firstly consider to place to the cluster with less than g data blocks and one
                                    # local parity block, thus to make 'g+1' placement after merging of this stage
                                    for cid in cluster_set:
                                        if cid != cluster_id and self.placement.clusters[cid].type == 'L' and \
                                                0 < self.placement.clusters[cid].num_of_data_blocks < self.g and \
                                                block.map2group == self.placement.clusters[cid].blocks[0].map2group:
                                            block.place_to_cluster(cid)
                                            self.placement.clusters[cid].add_new_block(block)
                                            self.placement.clusters[cluster_id].blocks.pop(index)
                                            self.placement.clusters[cluster_id].num_of_blocks -= 1
                                            self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                            flag = True
                                            index -= 1
                                            break
                                    # then consider to place to an existed cluster with less than g+1 data blocks
                                    if not flag:
                                        for cid in cluster_set:
                                            if cid != cluster_id and \
                                                    0 < self.placement.clusters[cid].num_of_data_blocks <= self.g and \
                                                    block.map2group == self.placement.clusters[cid].blocks[0].map2group:
                                                block.place_to_cluster(cid)
                                                self.placement.clusters[cid].add_new_block(block)
                                                self.placement.clusters[cluster_id].blocks.pop(index)
                                                self.placement.clusters[cluster_id].num_of_blocks -= 1
                                                self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                                flag = True
                                                index -= 1
                                                break
                                    # finally consider to select empty cluster and place into it
                                    if not flag:
                                        for cid in cluster_set:
                                            if cid != cluster_id and self.placement.clusters[cid].num_of_blocks == 0:
                                                block.place_to_cluster(cid)
                                                self.placement.clusters[cid].add_new_block(block)
                                                self.placement.clusters[cluster_id].blocks.pop(index)
                                                self.placement.clusters[cluster_id].num_of_blocks -= 1
                                                self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                                index -= 1
                                                break
                                    index += 1
                                c_cnt += 1

                # remove the empty clusters from the set
                empty_set = set()
                for cluster_id in cluster_set:
                    if self.placement.clusters[cluster_id].num_of_blocks == 0:
                        empty_set.add(cluster_id)
                for empty_set_id in empty_set:
                    cluster_set.remove(empty_set_id)

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
    k = 4
    l = 2
    g = 2
    x = np.prod(x_list)
    dis = DispersedPlacement(encoding_type, x, k, l, g)
    dis.placement()
    dis.print_placement_res()
    staged_merge = StagedStripeMergingForDis(len(x_list), x_list, k, l, g, dis)
    staged_merge.stripe_merging(True)
    staged_merge.print_res('Dispersed')
