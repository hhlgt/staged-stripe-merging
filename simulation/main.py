from optimal import *
from aggregated import *
from dispersed import *
from ran_1 import *
from ran_2 import *
from parameters import *
import pandas as pd


def print_line(x_len):
    print('--------------', end='')
    while x_len > -1:
        x_len -= 1
        print('------------------', end='')
    print('')


if __name__ == '__main__':
    saved_to_excel = False
    res_data = {'Parameters': [], 'Scheme': [], 'S1_LC': [], 'S1_DC': [], 'S1_GC': [], 'S1_Tot': [],
                'S2_LC': [], 'S2_DC': [], 'S2_GC': [], 'S2_Tot': [],
                'S3_LC': [], 'S3_DC': [], 'S3_GC': [], 'S3_Tot': [],
                'S4_LC': [], 'S4_DC': [], 'S4_GC': [], 'S4_Tot': [],
                'LC': [], 'DC': [], 'GC': [], 'Total': []}


    def save_data(para, title, scheme):
        res_data['Parameters'].append(para)
        res_data['Scheme'].append(title)
        for s in range(0, len(scheme.stage_x)):
            if s == 0:
                res_data['S1_LC'].append(scheme.LC[s])
                res_data['S1_DC'].append(scheme.DC[s])
                res_data['S1_GC'].append(scheme.GC[s])
                res_data['S1_Tot'].append(scheme.TOTAL[s])
            elif s == 1:
                res_data['S2_LC'].append(scheme.LC[s])
                res_data['S2_DC'].append(scheme.DC[s])
                res_data['S2_GC'].append(scheme.GC[s])
                res_data['S2_Tot'].append(scheme.TOTAL[s])
            elif s == 2:
                res_data['S3_LC'].append(scheme.LC[s])
                res_data['S3_DC'].append(scheme.DC[s])
                res_data['S3_GC'].append(scheme.GC[s])
                res_data['S3_Tot'].append(scheme.TOTAL[s])
            else:
                res_data['S4_LC'].append(scheme.LC[s])
                res_data['S4_DC'].append(scheme.DC[s])
                res_data['S4_GC'].append(scheme.GC[s])
                res_data['S4_Tot'].append(scheme.TOTAL[s])
        for s in range(len(scheme.stage_x), 4):
            if s == 2:
                res_data['S3_LC'].append(0)
                res_data['S3_DC'].append(0)
                res_data['S3_GC'].append(0)
                res_data['S3_Tot'].append(0)
            else:
                res_data['S4_LC'].append(0)
                res_data['S4_DC'].append(0)
                res_data['S4_GC'].append(0)
                res_data['S4_Tot'].append(0)
        res_data['LC'].append(sum(scheme.LC))
        res_data['DC'].append(sum(scheme.DC))
        res_data['GC'].append(sum(scheme.GC))
        res_data['Total'].append(sum(scheme.TOTAL))


    def val_to_str(xl, k, l, g):
        index = 1
        s = str(xl[0])
        if xl[0] == 1:
            s = str(xl[1])
            index = 2
        while index < len(xl):
            s += '×'
            s += str(xl[index])
            index += 1
        s += '(' + str(k) + ',' + str(l) + ',' + str(g) + ')'
        return s


    # experiment 1
    for i in range(len(EXP1_DIFFERENT_X_LIST)):
        for j in range(len(EXP1_DIFFERENT_K_L_G)):
            x_list = EXP1_DIFFERENT_X_LIST[i][j]
            k = EXP1_DIFFERENT_K_L_G[j][0]
            l = EXP1_DIFFERENT_K_L_G[j][1]
            g = EXP1_DIFFERENT_K_L_G[j][2]
            x = np.prod(x_list)
            ran = RandomPlacement(x, k, l, g)
            ran.placement()
            ran_staged_merge = StagedStripeMergingForRan(len(x_list), x_list, k, l, g, ran)
            ran_staged_merge.stripe_merging()
            ran_p = RandomPlacementP(x, k, l, g)
            ran_p.placement()
            ran_p_staged_merge = StagedStripeMergingForRanP(len(x_list), x_list, k, l, g, ran_p)
            ran_p_staged_merge.stripe_merging()
            dis = DispersedPlacement(x, k, l, g)
            dis.placement()
            dis_staged_merge = StagedStripeMergingForDis(len(x_list), x_list, k, l, g, dis)
            dis_staged_merge.stripe_merging()
            agg = AggregatedPlacement(x, k, l, g)
            agg.placement()
            agg_staged_merge = StagedStripeMergingForAgg(len(x_list), x_list, k, l, g, agg)
            agg_staged_merge.stripe_merging()
            opt = OptimalPlacement(x, k, l, g)
            opt.placement()
            opt_staged_merge = StagedStripeMergingForOpt(len(x_list), x_list, k, l, g, opt)
            opt_staged_merge.stripe_merging()
            print('Staged Stripe Merging: {}({}, {}, {})'.format(x_list, k, l, g))
            print_line(len(x_list))
            print('| %10s |     Stage %d     |' % ('Scheme', 1), end='')
            for ii in range(1, len(x_list)):
                print('     Stage %d     |' % (ii + 1), end='')
            print('      Total      |')
            print_line(len(x_list))
            ran_staged_merge.display('Ran')
            ran_p_staged_merge.display('Ran-P')
            dis_staged_merge.display('Dispersed')
            agg_staged_merge.display('Aggregated')
            opt_staged_merge.display('Optimal')
            print_line(len(x_list))
            print('')
            if saved_to_excel:
                x_k_l_g = val_to_str(x_list, k, l, g)
                save_data(x_k_l_g, 'Ran', ran_staged_merge)
                save_data(x_k_l_g, 'Ran-P', ran_p_staged_merge)
                save_data(x_k_l_g, 'Dispersed', dis_staged_merge)
                save_data(x_k_l_g, 'Aggregated', agg_staged_merge)
                save_data(x_k_l_g, 'Optimal', opt_staged_merge)

    # experiment 2
    for i in range(len(EXP2_DIFFERENT_K_L_G)):
        k = EXP2_DIFFERENT_K_L_G[i][0]
        l = EXP2_DIFFERENT_K_L_G[i][1]
        g = EXP2_DIFFERENT_K_L_G[i][2]
        for j in range(len(EXP2_DIFFERENT_X_LIST[i])):
            x_list = EXP2_DIFFERENT_X_LIST[i][j]
            x = np.prod(x_list)
            ran = RandomPlacement(x, k, l, g)
            ran.placement()
            ran_staged_merge = StagedStripeMergingForRan(len(x_list), x_list, k, l, g, ran)
            ran_staged_merge.stripe_merging()
            ran_p = RandomPlacementP(x, k, l, g)
            ran_p.placement()
            ran_p_staged_merge = StagedStripeMergingForRanP(len(x_list), x_list, k, l, g, ran_p)
            ran_p_staged_merge.stripe_merging()
            dis = DispersedPlacement(x, k, l, g)
            dis.placement()
            dis_staged_merge = StagedStripeMergingForDis(len(x_list), x_list, k, l, g, dis)
            dis_staged_merge.stripe_merging()
            agg = AggregatedPlacement(x, k, l, g)
            agg.placement()
            agg_staged_merge = StagedStripeMergingForAgg(len(x_list), x_list, k, l, g, agg)
            agg_staged_merge.stripe_merging()
            opt = OptimalPlacement(x, k, l, g)
            opt.placement()
            opt_staged_merge = StagedStripeMergingForOpt(len(x_list), x_list, k, l, g, opt)
            opt_staged_merge.stripe_merging()
            print('Staged Stripe Merging: {}({}, {}, {})'.format(x_list, k, l, g))
            print_line(len(x_list))
            print('| %10s |     Stage %d     |' % ('Scheme', 1), end='')
            for ii in range(1, len(x_list)):
                print('     Stage %d     |' % (ii + 1), end='')
            print('      Total      |')
            print_line(len(x_list))
            ran_staged_merge.display('Ran')
            ran_p_staged_merge.display('Ran-P')
            dis_staged_merge.display('Dispersed')
            agg_staged_merge.display('Aggregated')
            opt_staged_merge.display('Optimal')
            print_line(len(x_list))
            print('')
            if saved_to_excel:
                x_k_l_g = val_to_str(x_list, k, l, g)
                save_data(x_k_l_g, 'Ran', ran_staged_merge)
                save_data(x_k_l_g, 'Ran-P', ran_p_staged_merge)
                save_data(x_k_l_g, 'Dispersed', dis_staged_merge)
                save_data(x_k_l_g, 'Aggregated', agg_staged_merge)
                save_data(x_k_l_g, 'Optimal', opt_staged_merge)

    if saved_to_excel:
        df = pd.DataFrame(res_data)
        df.to_excel('result.xlsx')