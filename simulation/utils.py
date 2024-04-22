import numpy as np


class Block(object):
    def __init__(self, block_id, group_id, stripe_id):
        self.block_id = block_id
        self.map2group = group_id
        self.map2stripe = stripe_id
        self.map2cluster = -1

    def place_to_cluster(self, cluster_id):
        self.map2cluster = cluster_id

    def merge_to_larger_stripe(self, old_stripe_id, new_stripe_id):
        if self.map2stripe == old_stripe_id:
            # self.block_id += '_' + new_stripe_id
            self.map2stripe = new_stripe_id

    def print_block_information(self):
        print('[{}:{},G{}]'.format(self.block_id, self.map2stripe, self.map2group), end=' ')
        # print('[{}]'.format(self.block_id), end=' ')

    def equal(self, block2):
        flag = True
        if self.block_id != block2.block_id or self.map2group != block2.map2group or \
                self.map2cluster != block2.map2cluster or self.map2stripe != block2.map2stripe:
            flag = False
        return flag

    def from_same_stripe_and_group(self, block2):
        if self.map2group == block2.map2group and self.map2stripe == block2.map2stripe:
            return True
        return False


class Cluster(object):
    def __init__(self, cluster_id):
        self.cluster_id = cluster_id
        self.blocks = []
        self.type = 'D'
        self.num_of_blocks = 0
        self.num_of_data_blocks = 0
        self.stripe_flag = []

    def return_id(self):
        return self.cluster_id

    # type of cluster, priority: L > G > D, a flag to denote if there is any parity block in the cluster
    def add_new_block(self, new_block):
        self.blocks.append(new_block)
        if new_block.block_id[0] == 'L':
            self.type = 'L'
        elif self.type == 'D' and new_block.block_id[0] == 'G':
            self.type = 'G'
        elif new_block.block_id[0] == 'D':
            self.num_of_data_blocks += 1
        self.num_of_blocks += 1

    def remove_block(self, stripe_id, t):
        group_id = set()
        i = 0
        while i < self.num_of_blocks:
            block = self.blocks[i]
            if block.map2stripe == stripe_id and block.block_id[0] == t:
                group_id.add(block.map2group)
                if block.block_id[0] == 'D':
                    self.num_of_data_blocks -= 1
                self.num_of_blocks -= 1
                self.blocks.pop(i)
                i -= 1
            i += 1
        return group_id

    # def remove_data_block(self, block_id):
    #     i = 0
    #     while i < self.num_of_blocks:
    #         block = self.blocks[i]
    #         if block.block_id == block_id:
    #             self.num_of_blocks -= 1
    #             self.blocks.pop(i)
    #             i -= 1
    #         i += 1

    def return_all_blocks(self):
        return self.blocks

    def print_cluster_information(self):
        print('Cluster {}: '.format(self.cluster_id), end='')
        for block in self.blocks:
            block.print_block_information()
        print(' {}'.format(self.type))
        # print(', num_of_data_blocks: {}, num_of_blocks: {}'.format(self.num_of_data_blocks, self.num_of_blocks))

    def init_stripe_flag(self, x):
        for i in range(x):
            self.stripe_flag.append(False)

    # count the number of blocks from specific stripe in the cluster
    def count_blocks_num(self, stripe_id):
        cnt = 0
        data_cnt = 0
        group_set = [0, 0, 0, 0, 0]
        flag = False
        for block in self.blocks:
            if block.map2stripe == stripe_id:
                cnt += 1
                if block.block_id[0] == 'D':
                    data_cnt += 1
                group_set[block.map2group] += 1
                flag = True
        group_id = -1
        max_group_num = 0
        if flag:
            group_id = group_set.index(max(group_set))
            max_group_num = max(group_set)
        return [cnt, data_cnt, group_id, max_group_num]

    # if there is any local parity block in the cluster
    def find_local_parity_block(self, stripe_id=None, group_id=None):
        for block in self.blocks:
            if group_id is None and stripe_id is None and block.block_id[0] == 'L':
                return True
            elif group_id is None and block.block_id[0] == 'L' and block.map2stripe == stripe_id:
                return True
            elif block.block_id[0] == 'L' and block.map2stripe == stripe_id and block.map2group == group_id:
                return True
        return False

    # if there is any global parity block in the cluster
    def find_global_parity_block(self, stripe_id=None):
        for block in self.blocks:
            if block.block_id[0] == 'G' and stripe_id is None:
                return True
            elif block.block_id[0] == 'G' and block.map2stripe == stripe_id:
                return True
        return False


def index_to_id(str1, index):
    return str1 + str(index)
    # return str1 + str(index) + '_' + str(self.stripe_id)


class Stripe(object):
    def __init__(self, encoding_type, stripe_id, k, l, g):
        self.encoding_type = encoding_type
        self.k = k
        self.l = l
        self.g = g
        self.r = k / l
        self.stripe_id = stripe_id
        self.stripe_information = []
        self.place2cluster = set()

    def place_to_cluster(self, cluster_id):
        self.place2cluster.add(cluster_id)

    def generate_stripe_information(self):
        '''
            Based on Azure-LRC
            stripe_information: [['D0', 'D1', 'D2', 'L0'], ['D3', 'D4', 'D5', 'L1'], ['G0', 'G1', 'G2']]
        '''
        group_id = 0
        stripe_num = int(self.stripe_id.replace('S', ''))
        self.stripe_information.append([])
        for i in range(self.k):
            if i == self.r * (group_id + 1):
                group_id += 1
                self.stripe_information.append([])
            block_id = index_to_id('D', i + stripe_num * self.k)
            self.stripe_information[group_id].append(block_id)
        group_id += 1
        self.stripe_information.append([])
        for i in range(self.g):
            block_id = index_to_id('G', i + stripe_num * self.g)
            self.stripe_information[group_id].append(block_id)
        for i in range(self.l):
            block_id = index_to_id('L', i + stripe_num * self.l)
            self.stripe_information[i].append(block_id)

    def print_stripe_information(self):
        print('{}:'.format(self.stripe_id), end='')
        print(self.stripe_information)


class DataPlacement(object):
    def __init__(self, encoding_type, x, k, l, g):
        self.encoding_type = encoding_type
        self.k = k
        self.l = l
        self.g = g
        self.x = x
        self.b = k / l
        self.stripes = []
        self.clusters = []
        self.num_of_clusters = 0
        self.g_cluster_id = -1
        self.g_cluster_id_set = set()
        self.generate_stripes()

    def generate_stripes(self):
        for i in range(self.x):
            stripe_id = 'S' + str(i)
            stripe_i = Stripe(self.encoding_type, stripe_id, self.k, self.l, self.g)
            stripe_i.generate_stripe_information()
            self.stripes.append(stripe_i)

    def remove_stripe(self, stripe_id):
        for stripe in self.stripes:
            if stripe.stripe_id == stripe_id:
                self.stripes.remove(stripe)
                break

    def placement(self):
        pass

    def print_cluster(self, cluster_id):
        self.clusters[cluster_id].print_cluster_information()

    def print_placement_res(self):
        print('Stripes information:')
        for stripe in self.stripes:
            stripe.print_stripe_information()
        print('Data placement:')
        for cluster in self.clusters:
            if cluster.num_of_blocks > 0:
                cluster.print_cluster_information()
        print('Global blocks cluster set:')
        print(self.g_cluster_id_set)

    def calculate_cluster_num(self, strip_id, group_id):
        counter = np.zeros((self.num_of_clusters,), dtype=int)
        for cluster in self.clusters:
            for block in cluster.blocks:
                if block.map2stripe == strip_id and block.map2group == group_id:
                    counter[cluster.cluster_id] += 1
        return counter


class StagedStripeMerging(object):

    def __init__(self, n, x_list, k, l, g, placement):
        self.num_of_stages = n
        self.stage_x = x_list
        self.x = np.prod(self.stage_x)
        self.k = k
        self.l = l
        self.g = g
        self.b = k / l
        self.m = self.b % (self.g + 1)
        self.placement = placement
        self.LC = []
        self.DC = []
        self.GC = []
        self.TOTAL = []

    def check_parameter(self):
        # m = 0 or m = g or g % m = 0
        assert (not self.m) or (self.m == self.g) or \
               (self.m and self.g % self.m == 0 and self.stage_x[0] == self.g / self.m), 'Parameters do not meet ' \
                                                                                         'requirements! '
        assert self.x == self.placement.x, 'Parameters do not meet requirements!'
        assert self.k % self.l == 0, 'Parameters do not meet requirements!'

    def stripe_merging(self):
        pass

    def return_cost(self):
        return sum(self.TOTAL)

    def print_res(self, title):
        self.placement.print_placement_res()
        print('--------------------------------------------')
        print('Cost for {}'.format(title))
        print('Local Parity Block Recalculation: {}'.format(np.sum(self.LC)))
        print('Data Block Relocation: {}'.format(np.sum(self.DC)))
        print('Global Parity Block Recalculation: {}'.format(np.sum(self.GC)) )
        print('Total Cost: {}'.format(np.sum(self.TOTAL)))
        print('--------------------------------------------')

    def display(self, title):
        print('| %10s | %3d+%3d+%3d=%3d |' % (title, self.LC[0], self.DC[0], self.GC[0], self.TOTAL[0]), end='')
        for i in range(1, len(self.stage_x)):
            print(' %3d+%3d+%3d=%3d |' % (self.LC[i], self.DC[i], self.GC[i], self.TOTAL[i]), end='')
        print(' %3d+%3d+%3d=%3d |' % (sum(self.LC), sum(self.DC), sum(self.GC), sum(self.TOTAL)))
