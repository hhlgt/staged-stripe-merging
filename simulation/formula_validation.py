import math
import numpy as np
from parameters import *
from main import print_line


def display(title, length, LC, DC, GC, TOTAL):
    print('| %10s | %3d+%3d+%3d=%3d |' % (title, LC[0], DC[0], GC[0], TOTAL[0]), end='')
    for i in range(1, length):
        print(' %3d+%3d+%3d=%3d |' % (LC[i], DC[i], GC[i], TOTAL[i]), end='')
    print(' %3d+%3d+%3d=%3d |' % (sum(LC), sum(DC), sum(GC), sum(TOTAL)))


class CostForStripeMerging(object):
    def __init__(self, encoding_type, n, x_list, k, l, g):
        self.encoding_type = encoding_type
        self.num_of_stages = n
        self.stage_x = x_list
        self.x = np.prod(self.stage_x)
        self.k = k
        self.l = l
        self.g = g
        self.b = k / l
        self.m = self.b % (self.g + 1)

    def cal_cost_for_opt(self):
        OPT_LC = []
        OPT_DC = []
        OPT_GC = []
        OPT_TOTAL = []
        stage_num = 1
        k_ = self.k
        x_ = self.x
        for xi in self.stage_x:
            LC = 0
            DC = 0
            GC = 0
            for s in range(0, x_, xi):
                if xi == 1:
                    break
                if self.m == 0 or stage_num == 1:
                    DC += 0
                else:
                    DC += (xi - 1) * self.g * self.l
                bi = k_ / self.l
                if self.m == 0:
                    GC += (bi / (self.g + 1)) * self.g * self.l * xi
                elif (self.g > self.m > 0 and stage_num == 1) or (self.m == self.g and stage_num == 2):
                    GC += math.floor(bi / (self.g + 1)) * self.g * self.l * xi + self.g * self.l
                else:
                    GC += ((math.floor((bi + 1) / (self.g + 1)) - 1) * self.g + (bi + 1) % (self.g + 1)) * self.l * xi \
                           + self.g * self.l
                if encoding_type == "Optimal Cauchy LRC" and self.m > 0:
                    LC += self.l
            stage_num += 1
            x_ = int(x_ / xi)
            k_ = k_ * xi
            OPT_LC.append(LC)
            OPT_DC.append(DC)
            OPT_GC.append(GC)
            OPT_TOTAL.append(LC + DC + GC)
        display('Optimal', len(self.stage_x), OPT_LC, OPT_DC, OPT_GC, OPT_TOTAL)

    def cal_cost_for_opt_2(self):
        OPT_LC = []
        OPT_DC = []
        OPT_GC = []
        OPT_TOTAL = []
        stage_num = 1
        k_ = self.k
        x_ = self.x
        x_post = 1
        flag = False
        for xi in self.stage_x:
            LC = 0
            DC = 0
            GC = 0
            for s in range(0, x_, xi):
                if xi == 1:
                    break
                bi = k_ / self.l
                alfa = 0
                beta = 0
                if self.m == 0 or x_post * xi * self.m <= self.g:
                    DC += 0
                    alfa = math.floor(bi / (self.g + 1)) * self.g * self.l * xi
                    beta = (bi % (self.g + 1)) * self.l * xi
                elif not flag and x_post * xi * self.m > self.g:
                    DC += (x_post * xi * self.m - self.g) * self.l
                    alfa = math.floor(bi / (self.g + 1)) * self.g * self.l * xi
                    beta = self.g * self.l
                    flag = True
                else:
                    DC += (xi - 1) * self.g * self.l
                    alfa = ((math.floor((bi + 1) / (self.g + 1)) - 1) * self.g + (bi + 1) % (self.g + 1)) * self.l * xi
                    beta = self.g * self.l
                GC += alfa + beta
            stage_num += 1
            x_ = int(x_ / xi)
            x_post = x_post * xi
            k_ = k_ * xi
            OPT_LC.append(LC)
            OPT_DC.append(DC)
            OPT_GC.append(GC)
            OPT_TOTAL.append(LC + DC + GC)
        display('Optimal', len(self.stage_x), OPT_LC, OPT_DC, OPT_GC, OPT_TOTAL)

    def cal_cost_for_agg(self):
        AGG_LC = []
        AGG_DC = []
        AGG_GC = []
        AGG_TOTAL = []
        stage_num = 1
        k_ = self.k
        x_ = self.x
        for xi in self.stage_x:
            LC = 0
            DC = 0
            GC = 0
            for s in range(0, x_, xi):
                if xi == 1:
                    break
                DC += math.floor(self.b / (self.g + 1)) * (xi - 1) * (self.g + 1) * self.l
                if self.m == 0 or stage_num == 1:
                    DC += 0
                else:
                    DC += (xi - 1) * self.g * self.l
                bi = k_ / self.l
                if self.m == 0:
                    GC += ((bi / (self.g + 1)) * xi - (self.b / (self.g + 1)) * (xi - 1)) * self.g * self.l
                elif (self.g > self.m > 0 and stage_num == 1) or (self.m == self.g and stage_num == 2):
                    GC += math.ceil(self.b / (self.g + 1)) * self.g * self.l
                else:
                    GC += ((math.floor((bi + 1) / (self.g + 1)) - 1) * self.g + (bi + 1) % (self.g + 1)) * self.l * xi \
                          + self.g * self.l - math.floor(self.b / (self.g + 1)) * (xi - 1) * self.g * self.l
                if encoding_type == "Optimal Cauchy LRC" and self.m > 0:
                    LC += self.l
            stage_num += 1
            x_ = int(x_ / xi)
            k_ = k_ * xi
            AGG_LC.append(LC)
            AGG_DC.append(DC)
            AGG_GC.append(GC)
            AGG_TOTAL.append(LC + DC + GC)
        display('Aggregated', len(self.stage_x), AGG_LC, AGG_DC, AGG_GC, AGG_TOTAL)

    def cal_cost_for_dis(self):
        DIS_LC = []
        DIS_DC = []
        DIS_GC = []
        DIS_TOTAL = []
        stage_num = 1
        k_ = self.k
        x_ = self.x
        for xi in self.stage_x:
            LC = 0
            DC = 0
            GC = 0
            for s in range(0, x_, xi):
                if xi == 1:
                    break
                bi = k_ / self.l
                LC += (xi - 1) * self.l
                if self.m > 0:
                    m = self.g
                    if stage_num == 1:
                        m = self.m
                    LC += (math.ceil((bi + 1) / (self.g + 1)) * xi - math.ceil((bi*xi + 1) / (self.g + 1))) * m * self.l
                if encoding_type == "Optimal Cauchy LRC" and self.m > 0:
                    LC += xi * self.l
                if self.m == 0:
                    GC += (bi / (self.g + 1)) * self.g * self.l * xi
                elif (self.g > self.m > 0 and stage_num == 1) or (self.m == self.g and stage_num == 2):
                    GC += (math.floor(bi / (self.g + 1)) * self.g + bi % (self.g + 1)) * self.l * xi
                else:
                    GC += math.ceil((bi + 1) / (self.g + 1)) * self.g * self.l * xi
            stage_num += 1
            x_ = int(x_ / xi)
            k_ = k_ * xi
            DIS_LC.append(LC)
            DIS_DC.append(DC)
            DIS_GC.append(GC)
            DIS_TOTAL.append(LC + DC + GC)
        display('Dispersed', len(self.stage_x), DIS_LC, DIS_DC, DIS_GC, DIS_TOTAL)


if __name__ == '__main__':
    encoding_type = "Optimal Cauchy LRC"
    # experiment 1
    for i in range(len(EXP1_DIFFERENT_X_LIST)):
        for j in range(len(EXP1_DIFFERENT_K_L_G)):
            x_list = EXP1_DIFFERENT_X_LIST[i][j]
            k = EXP1_DIFFERENT_K_L_G[j][0]
            l = EXP1_DIFFERENT_K_L_G[j][1]
            g = EXP1_DIFFERENT_K_L_G[j][2]
            cost = CostForStripeMerging(encoding_type, len(x_list), x_list, k, l, g)
            print('Staged Stripe Merging: {}({}, {}, {})'.format(x_list, k, l, g))
            print_line(len(x_list))
            print('| %10s |     Stage %d     |' % ('Scheme', 1), end='')
            for ii in range(1, len(x_list)):
                print('     Stage %d     |' % (ii + 1), end='')
            print('      Total      |')
            print_line(len(x_list))
            cost.cal_cost_for_dis()
            cost.cal_cost_for_agg()
            cost.cal_cost_for_opt()
            print_line(len(x_list))
            print('')

    # experiment 2
    for i in range(len(EXP2_DIFFERENT_K_L_G)):
        k = EXP2_DIFFERENT_K_L_G[i][0]
        l = EXP2_DIFFERENT_K_L_G[i][1]
        g = EXP2_DIFFERENT_K_L_G[i][2]
        for j in range(len(EXP2_DIFFERENT_X_LIST[i])):
            x_list = EXP2_DIFFERENT_X_LIST[i][j]
            cost = CostForStripeMerging(encoding_type, len(x_list), x_list, k, l, g)
            print('Staged Stripe Merging: {}({}, {}, {})'.format(x_list, k, l, g))
            print_line(len(x_list))
            print('| %10s |     Stage %d     |' % ('Scheme', 1), end='')
            for ii in range(1, len(x_list)):
                print('     Stage %d     |' % (ii + 1), end='')
            print('      Total      |')
            print_line(len(x_list))
            cost.cal_cost_for_dis()
            cost.cal_cost_for_agg()
            cost.cal_cost_for_opt()
            print_line(len(x_list))
            print('')