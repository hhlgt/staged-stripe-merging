import math
from utils import *


class OptimalPlacement(DataPlacement):

    def placement(self):
        remaining_local_groups = []
        cluster_id = 0
        for i in range(self.l):
            # disperse every x(g+1) blocks across x clusters
            if self.b >= self.g + 1:
                for j in range(self.x):
                    group = self.stripes[j].stripe_information[i]
                    for block_num in range(0, len(group), self.g + 1):
                        if block_num + self.g + 1 > len(group) - 1:     # derive the remaining local groups
                            remaining_local_groups.append(group[block_num:])
                        else:
                            new_cluster = Cluster(cluster_id)
                            for block_id in group[block_num:block_num+self.g+1]:
                                block = Block(block_id, i, self.stripes[j].stripe_id)
                                block.place_to_cluster(cluster_id)
                                new_cluster.add_new_block(block)
                                self.stripes[j].place_to_cluster(cluster_id)
                            self.clusters.append(new_cluster)
                            cluster_id += 1
            else:
                for j in range(self.x):  # derive the remaining local groups
                    group = self.stripes[j].stripe_information[i]
                    remaining_local_groups.append(group)
        m = 0
        if len(remaining_local_groups) > 0:
            m = len(remaining_local_groups[0])
            # place all the remaining local groups to the cluster that will also place global parity blocks
            if m == 1:
                new_cluster = Cluster(cluster_id)
                for i in range(self.l):
                    for j in range(self.x):
                        k = i * self.x + j
                        group = remaining_local_groups[k]
                        for block_id in group:
                            block = Block(block_id, i, self.stripes[j].stripe_id)
                            block.place_to_cluster(cluster_id)
                            new_cluster.add_new_block(block)
                        self.stripes[j].place_to_cluster(cluster_id)
                self.clusters.append(new_cluster)
                cluster_id += 1
            else:   # aggregate every x remaining local groups in a cluster
                for i in range(0, self.l):
                    new_cluster = Cluster(cluster_id)
                    for j in range(self.x):
                        k = i * self.x + j
                        group = remaining_local_groups[k]
                        for block_id in group:
                            block = Block(block_id, i, self.stripes[j].stripe_id)
                            block.place_to_cluster(cluster_id)
                            new_cluster.add_new_block(block)
                        self.stripes[j].place_to_cluster(cluster_id)
                    self.clusters.append(new_cluster)
                    cluster_id += 1
        # aggregate all the global parity blocks in a cluster
        if m != 1:
            new_cluster = Cluster(cluster_id)
            self.clusters.append(new_cluster)
        else:
            cluster_id -= 1
        for i in range(self.x):
            global_blocks = self.stripes[i].stripe_information[-1]
            for block_id in global_blocks:
                block = Block(block_id, self.l, self.stripes[i].stripe_id)
                block.place_to_cluster(cluster_id)
                self.clusters[cluster_id].add_new_block(block)
            self.stripes[i].place_to_cluster(cluster_id)
        self.g_cluster_id = cluster_id
        cluster_id += 1
        self.num_of_clusters = cluster_id


class StagedStripeMergingForOpt(StagedStripeMerging):

    def stripe_merging(self, debug=False):
        self.check_parameter()
        stage_num = 1
        for xi in self.stage_x:
            LC = 0
            DC = 0
            GC = 0
            larger_stripe_num = 0
            new_stripes = []
            for s in range(0, self.x, xi):  # for every xi stripes
                if xi == 1:     # if xi == 1, skip this stage
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
                for num in range(self.g):     # global parity blocks
                    new_global_parity_id = 'G' + str(num + larger_stripe_num * self.g)
                    larger_stripe.stripe_information[self.l].append(new_global_parity_id)
                # find the clusters that the xi stripes span
                old_stripe_id = []
                for j in range(s, s + xi):
                    larger_stripe.place2cluster = \
                        larger_stripe.place2cluster.union(self.placement.stripes[j].place2cluster)
                    old_stripe_id.append(self.placement.stripes[j].stripe_id)
                cluster_set = larger_stripe.place2cluster
                # figure out the cluster that places the global parity blocks
                g_cluster_id = self.placement.g_cluster_id

                if self.placement.encoding_type == "Optimal Cauchy LRC":
                    # calculate LC
                    for group_num in range(self.l):
                        local_c_set = set()
                        for cluster_id in cluster_set:
                            for stripe_id in old_stripe_id:
                                if self.placement.clusters[cluster_id].find_local_parity_block(stripe_id, group_num):
                                    local_c_set.add(cluster_id)
                                    break
                        local_c_set.add(g_cluster_id)  # local parity blocks recalculation needs derive global parities
                        LC += len(local_c_set) - 1

                for cluster_id in cluster_set:
                    # merge to new larger stripe by changing the stripe that each block belongs to
                    for stripe_id in old_stripe_id:
                        for j in range(self.placement.clusters[cluster_id].num_of_blocks):
                            self.placement.clusters[cluster_id].blocks[j].merge_to_larger_stripe(stripe_id,
                                                                                                 larger_stripe_id)

                # transfer data blocks or partial blocks for global parity block recalculation
                for cluster_id in cluster_set:
                    m = self.placement.clusters[cluster_id].num_of_data_blocks
                    if self.placement.clusters[cluster_id].type == 'L':
                        m = self.placement.clusters[cluster_id].num_of_data_blocks / (self.x / xi)
                    if m <= self.g:
                        GC += int(m)
                    else:
                        GC += self.g

                # recalculate global parity blocks
                # get and delete old global parity blocks with tag of larger_stripe_id
                self.placement.clusters[g_cluster_id].remove_block(larger_stripe_id, 'G')
                # recalculate new global parity blocks for larger stripe
                for g in range(self.g):
                    new_global_parity_block_id = 'G' + str(g + larger_stripe_num * self.g)
                    global_parity_block = Block(new_global_parity_block_id, self.l, larger_stripe_id)
                    global_parity_block.place_to_cluster(g_cluster_id)
                    self.placement.clusters[g_cluster_id].add_new_block(global_parity_block)

                new_cluster_set = set()
                for cluster_id in cluster_set:
                    m = self.placement.clusters[cluster_id].num_of_data_blocks
                    # local parity block recalculation
                    if self.placement.clusters[cluster_id].type == 'L':
                        m = math.ceil(self.placement.clusters[cluster_id].num_of_data_blocks / (self.x / xi))
                        # get and delete old local parity blocks with the tag of larger_stripe_id
                        group_set = self.placement.clusters[cluster_id].remove_block(larger_stripe_id, 'L')
                        # recalculate new local parity blocks for larger stripe
                        for group_id in group_set:
                            new_local_parity_block_id = 'L' + str(group_id + larger_stripe_num * self.l)
                            local_parity_block = Block(new_local_parity_block_id, group_id, larger_stripe_id)
                            local_parity_block.place_to_cluster(cluster_id)
                            self.placement.clusters[cluster_id].add_new_block(local_parity_block)
                    # data block relocation, consider merging tails
                    if m > self.g:
                        if self.placement.clusters[cluster_id].type != 'D':
                            data_to_move = (xi - 1) * self.g
                            DC += data_to_move
                            cluster_num = self.placement.num_of_clusters
                            moved = 0
                            index = 0
                            while index < self.placement.clusters[cluster_id].num_of_blocks:
                                if moved == data_to_move:
                                    break
                                block = self.placement.clusters[cluster_id].blocks[index]
                                if block.block_id[0] == 'D' and block.map2stripe == larger_stripe_id:
                                    flag = False
                                    # firstly consider to place to the cluster that store less than g + 1 data blocks
                                    for cid in cluster_set:
                                        if cid != cluster_id and self.placement.clusters[cid].type == 'D' and \
                                                self.placement.clusters[cid].num_of_blocks < self.g + 1 and \
                                                block.map2group == self.placement.clusters[cid].blocks[0].map2group:
                                            block.place_to_cluster(cid)
                                            self.placement.clusters[cid].add_new_block(block)
                                            self.placement.clusters[cluster_id].blocks.pop(index)
                                            self.placement.clusters[cluster_id].num_of_blocks -= 1
                                            self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                            index -= 1
                                            flag = True
                                            break
                                    # if not, consider to place every g + 1 data blocks to an existed new cluster
                                    if not flag:
                                        for cid in new_cluster_set:
                                            if cid != cluster_id and self.placement.clusters[cid].type == 'D' and \
                                                    self.placement.clusters[cid].num_of_blocks < self.g + 1 and \
                                                    block.map2group == self.placement.clusters[cid].blocks[0].map2group:
                                                block.place_to_cluster(cid)
                                                self.placement.clusters[cid].add_new_block(block)
                                                self.placement.clusters[cluster_id].blocks.pop(index)
                                                self.placement.clusters[cluster_id].num_of_blocks -= 1
                                                self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                                index -= 1
                                                flag = True
                                                break
                                    if not flag:  # finally consider to select a new cluster and place into it
                                        new_cluster = Cluster(cluster_num)
                                        block.place_to_cluster(cluster_num)
                                        new_cluster.add_new_block(block)
                                        self.placement.clusters.append(new_cluster)
                                        self.placement.num_of_clusters += 1
                                        self.placement.clusters[cluster_id].blocks.pop(index)
                                        self.placement.clusters[cluster_id].num_of_blocks -= 1
                                        self.placement.clusters[cluster_id].num_of_data_blocks -= 1
                                        index -= 1
                                        new_cluster_set.add(cluster_num)   # add to the new cluster set
                                        cluster_num += 1
                                    moved += 1
                                index += 1
                # some data blocks of the larger stripe are placed to other new clusters, save the information
                larger_stripe.place2cluster = larger_stripe.place2cluster.union(new_cluster_set)

                new_stripes.append(larger_stripe)
                larger_stripe_num += 1
            if xi > 1:  # if xi == 1, skip this stage
                self.placement.stripes = new_stripes
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
    encoding_type = "Azure-LRC"
    x_list = [2, 2, 2]
    k = 8
    l = 2
    g = 2
    x = np.prod(x_list)
    opt = OptimalPlacement(encoding_type, x, k, l, g)
    opt.placement()
    opt.print_placement_res()
    staged_merge = StagedStripeMergingForOpt(len(x_list), x_list, k, l, g, opt)
    staged_merge.stripe_merging(True)
    staged_merge.print_res('Optimal')
    staged_merge.display('Optimal')
