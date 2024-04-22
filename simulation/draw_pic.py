import matplotlib.pyplot as plt
import numpy as np
from optimal import *
from aggregated import *
from dispersed import *
from ran_1 import *
from ran_2 import *
from parameters import *


def generate_ytick(max_cost):
    tick = int(max_cost / 5)
    if tick % 10 != 0:
        tick = (int(tick / 10) + 1) * 10
    t = tick
    ytick = []
    for i in range(6):
        ytick.append(t)
        t += tick
    return ytick


def draw_multi_bars(datas, labels, ytick, xlabel, ylabel, fig_size, dpi, text_size, legend_size, datat=None, datae=None,
                    name=None):
    bar_width = 0.18
    col = ["#B0C4DE", "#9AC9DB", "#cd5b45", "#f8ac8c", "#3CB371"]
    label = ['Ran', 'Ran-P', 'DIS', 'AGG', 'OPT']
    bar_num = len(datas)
    bars = []
    tmp = np.arange(len(labels))
    bars.append(tmp)
    for it in range(bar_num - 1):
        tmp = [i + bar_width for i in tmp]
        bars.append(tmp)
    fig, ax = plt.subplots(figsize=fig_size, dpi=dpi, constrained_layout=True)
    for i in range(bar_num):
        ax.bar(bars[i], datas[i], color=col[i], edgecolor='black', width=bar_width, label=label[i])
        for x, y in zip(bars[i], datas[i]):
            plt.text(x, y + 1.5, '%d' % y, ha='center', fontsize=text_size)
        if datat is not None:
            for j in range(len(datat[i])):
                x = bars[i][j]
                for k in range(len(datat[i][j])):
                    if datat[i][j][k] != 0 and datat[i][j][k] != datas[i][j]:
                        ax.hlines(y=datat[i][j][k], xmin=x-bar_width/2, xmax=x+bar_width/2, colors='black')
                        # y = datat[i][j][k]
                        # plt.text(x, y + 2, '%d' % y, ha='center', fontsize=text_size, color='purple')
                    if datae is not None:
                        if datae[i][j][k] != 0:
                            y = (datat[i][j][k] * 2 - datae[i][j][k]) / 2 - text_size * 1.3
                            val = datae[i][j][k]
                            plt.text(x, y, '%d' % val, ha='center', fontsize=text_size, color='purple')
    plt.xlabel(xlabel=xlabel, fontsize=12)
    plt.ylabel(ylabel=ylabel, fontsize=12)
    xtick = [i - bar_width for i in bars[math.floor(len(datas) / 2)]]
    if bar_num % 2 != 0:
        xtick = [i for i in bars[math.floor(len(datas) / 2)]]
    plt.xticks(xtick, labels, rotation=45, fontsize=12)
    plt.yticks(ytick, fontsize=12)
    ax.legend(loc='upper left', fontsize=legend_size)
    plt.show()
    if name is not None:
        fig.savefig(name)

def draw_multi_bars_ad(datas, labels, ytick, xlabel, ylabel, fig_size, dpi, text_size, legend_size, vertical=True,
                        datat=None, datae=None, name=None):
    for i in range(len(datat)):
        for j in range(len(datat[i])):
            if 0 in datat[i][j]:
                datat[i][j].remove(0)
    for i in range(len(datae)):
        for j in range(len(datae[i])):
            if 0 in datae[i][j]:
                datae[i][j].remove(0)

    fig, ax = plt.subplots(figsize=fig_size, dpi=dpi, constrained_layout=True)
    bar_width = 0.18
    col = ["#000000", "#2e8b57", "#ffa500", "#104e8b", "#cd5b45"]
    # hatchs = ['xxxx', '////', '++++']
    hatchs = ['xxx', '///', '+++']
    label = ['Ran-1', 'Ran-2', 'Dis', 'Agg', 'Opt']
    bar_num = len(datas)
    bars = []
    tmp = np.arange(len(labels))
    plt.xlim(min(tmp) - 0.25, max(tmp) + 1)
    bars.append(tmp)
    for it in range(bar_num - 1):
        tmp = [i + bar_width for i in tmp]
        bars.append(tmp)

    for i in range(bar_num):
        ax.bar(bars[i], datas[i], color='white', edgecolor=col[i], width=bar_width, label=label[i])
        # for x, y in zip(bars[i], datas[i]):
        #     plt.text(x, y + 2.5, '%d' % y, ha='center', fontsize=text_size)
    if vertical:
        ax.legend(loc='upper left', fontsize=legend_size, frameon=False, handlelength=1.0, handletextpad=0.3)
        # ax.legend(loc='upper left', fontsize=legend_size, frameon=False, labelspacing=0.08)
    else:
        ax.legend(loc='upper left', fontsize=legend_size, frameon=False, ncol=5, columnspacing=0.2, handlelength=0.8,
                  handletextpad=0.2)

    for i in range(len(datae)):
        for j in range(len(datae[i])):
            for k in range(len(datae[i][j])):
                if k == 0:
                    ax.bar(bars[i][j], datae[i][j][k], color='white', edgecolor=col[i], width=bar_width, label=label[i],
                           hatch=hatchs[k])
                else:
                    ax.bar(bars[i][j], datae[i][j][k], color='white', edgecolor=col[i], width=bar_width, label=label[i],
                           bottom=datat[i][j][k-1], hatch=hatchs[k])

    plt.xlabel(xlabel=xlabel, fontsize=20)
    plt.ylabel(ylabel=ylabel, fontsize=20)
    xtick = [i - bar_width for i in bars[math.floor(len(datas) / 2)]]
    if bar_num % 2 != 0:
        xtick = [i for i in bars[math.floor(len(datas) / 2)]]
    plt.xticks(xtick, labels, fontsize=18)
    plt.yticks(ytick, fontsize=18)
    plt.show()
    if name is not None:
        fig.savefig(name)

if __name__ == '__main__':
    rand_times = 5

    def cal_cum_res(res_pre):
        res_post = [res_pre[0]]
        for i in range(1, len(res_pre)):
            res_post.append(res_pre[i] + res_post[i - 1])
        return res_post

	
	# encoding_type = "Azure-LRC"
	encoding_type = "Optimal Cauchy LRC"
    # experiment 1
    for i in range(len(EXP1_DIFFERENT_X_LIST)):
        data = [[], [], [], [], []]
        data_st = [[], [], [], [], []]
        data_total = [[], [], [], [], []]
        for j in range(len(EXP1_DIFFERENT_K_L_G)):
            x_list = EXP1_DIFFERENT_X_LIST[i][j]
            k = EXP1_DIFFERENT_K_L_G[j][0]
            l = EXP1_DIFFERENT_K_L_G[j][1]
            g = EXP1_DIFFERENT_K_L_G[j][2]
            x = np.prod(x_list)
            ran_res = []
            ran_p_res = []
            for ii in range(len(x_list)):
                ran_res.append(0)
                ran_p_res.append(0)
            for rt in range(rand_times):
                ran = RandomPlacement(encoding_type, x, k, l, g)
                ran.placement()
                ran_staged_merge = StagedStripeMergingForRan(len(x_list), x_list, k, l, g, ran)
                ran_staged_merge.stripe_merging()
                temp = ran_staged_merge.TOTAL
                ran_res = [a + b for a, b in zip(ran_res, temp)]
            data_st[0].append([round(a / rand_times) for a in ran_res])
            ran_res = cal_cum_res(ran_res)
            ran_res = [round(a / rand_times) for a in ran_res]
            data[0].append(ran_res[-1])
            data_total[0].append(ran_res)
            for rt in range(rand_times):
                ran_p = RandomPlacementP(encoding_type, x, k, l, g)
                ran_p.placement()
                ran_p_staged_merge = StagedStripeMergingForRanP(len(x_list), x_list, k, l, g, ran_p)
                ran_p_staged_merge.stripe_merging()
                temp = ran_p_staged_merge.TOTAL
                ran_p_res = [a + b for a, b in zip(ran_p_res, temp)]
            data_st[1].append([round(a / rand_times) for a in ran_p_res])
            ran_p_res = cal_cum_res(ran_p_res)
            ran_p_res = [round(a / rand_times) for a in ran_p_res]
            data[1].append(ran_p_res[-1])
            data_total[1].append(ran_p_res)
            dis = DispersedPlacement(encoding_type, x, k, l, g)
            dis.placement()
            dis_staged_merge = StagedStripeMergingForDis(len(x_list), x_list, k, l, g, dis)
            dis_staged_merge.stripe_merging()
            data[2].append(dis_staged_merge.return_cost())
            data_st[2].append(dis_staged_merge.TOTAL)
            data_total[2].append(cal_cum_res(dis_staged_merge.TOTAL))
            agg = AggregatedPlacement(encoding_type, x, k, l, g)
            agg.placement()
            agg_staged_merge = StagedStripeMergingForAgg(len(x_list), x_list, k, l, g, agg)
            agg_staged_merge.stripe_merging()
            data[3].append(agg_staged_merge.return_cost())
            data_st[3].append(agg_staged_merge.TOTAL)
            data_total[3].append(cal_cum_res(agg_staged_merge.TOTAL))
            opt = OptimalPlacement(encoding_type, x, k, l, g)
            opt.placement()
            opt_staged_merge = StagedStripeMergingForOpt(len(x_list), x_list, k, l, g, opt)
            opt_staged_merge.stripe_merging()
            data[4].append(opt_staged_merge.return_cost())
            data_st[4].append(opt_staged_merge.TOTAL)
            data_total[4].append(cal_cum_res(opt_staged_merge.TOTAL))
        max_cost = max(max(data))
        yticks = generate_ytick(max_cost)
		if i == 0:
            # yticks = [0, 50, 100, 150, 200, 250, 300, 350]
            yticks = [0, 100, 200, 300]
            if encoding_type == "Optimal Cauchy LRC":
                # yticks = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450]
                yticks = [0, 100, 200, 300, 400]
        X_LIST_STR = EXP1_DIFFERENT_X_LIST_STR
        filename = 'sim_para_' + str(i) + '.pdf'
        fn = 'sim_para.pdf'
        if encoding_type == "Optimal Cauchy LRC":
            X_LIST_STR = EXP1_DIFFERENT_X_LIST_STR_CAUCHY
            filename = 'sim_para_' + str(i) + '_cauchy.pdf'
            fn = 'sim_para_cauchy.pdf'
        # draw_multi_bars(data, EXP1_DIFFERENT_K_L_G_STR, yticks, X_LIST_STR[i], 'Merging Cost',
        #                    (9.8, 3.2), 60, 11, 16, False, data_total, data_st, filename)
        draw_multi_bars_ad(data, EXP1_DIFFERENT_K_L_G_STR, yticks, X_LIST_STR[i], 'Merging Cost',
                           (9.8, 3.2), 60, 11, 16, False, data_total, data_st, fn)

    # experiment 2
    for i in range(len(EXP2_DIFFERENT_K_L_G)):
        data = [[], [], [], [], []]
        data_st = [[], [], [], [], []]
        data_total = [[], [], [], [], []]
        k = EXP2_DIFFERENT_K_L_G[i][0]
        l = EXP2_DIFFERENT_K_L_G[i][1]
        g = EXP2_DIFFERENT_K_L_G[i][2]
        for j in range(len(EXP2_DIFFERENT_X_LIST[i])):
            x_list = EXP2_DIFFERENT_X_LIST[i][j]
            x = np.prod(x_list)
            ran_res = []
            ran_p_res = []
            for ii in range(len(x_list)):
                ran_res.append(0)
                ran_p_res.append(0)
            for rt in range(rand_times):
                ran = RandomPlacement(encoding_type, x, k, l, g)
                ran.placement()
                ran_staged_merge = StagedStripeMergingForRan(len(x_list), x_list, k, l, g, ran)
                ran_staged_merge.stripe_merging()
                temp = ran_staged_merge.TOTAL
                ran_res = [a + b for a, b in zip(ran_res, temp)]
            data_st[0].append([round(a / rand_times) for a in ran_res])
            ran_res = cal_cum_res(ran_res)
            ran_res = [round(a / rand_times) for a in ran_res]
            data[0].append(ran_res[-1])
            data_total[0].append(ran_res)
            for rt in range(rand_times):
                ran_p = RandomPlacementP(encoding_type, x, k, l, g)
                ran_p.placement()
                ran_p_staged_merge = StagedStripeMergingForRanP(len(x_list), x_list, k, l, g, ran_p)
                ran_p_staged_merge.stripe_merging()
                temp = ran_p_staged_merge.TOTAL
                ran_p_res = [a + b for a, b in zip(ran_p_res, temp)]
            data_st[1].append([round(a / rand_times) for a in ran_p_res])
            ran_p_res = cal_cum_res(ran_p_res)
            ran_p_res = [round(a / rand_times) for a in ran_p_res]
            data[1].append(ran_p_res[-1])
            data_total[1].append(ran_p_res)
            dis = DispersedPlacement(encoding_type, x, k, l, g)
            dis.placement()
            dis_staged_merge = StagedStripeMergingForDis(len(x_list), x_list, k, l, g, dis)
            dis_staged_merge.stripe_merging()
            data[2].append(dis_staged_merge.return_cost())
            data_st[2].append(dis_staged_merge.TOTAL)
            data_total[2].append(cal_cum_res(dis_staged_merge.TOTAL))
            agg = AggregatedPlacement(encoding_type, x, k, l, g)
            agg.placement()
            agg_staged_merge = StagedStripeMergingForAgg(len(x_list), x_list, k, l, g, agg)
            agg_staged_merge.stripe_merging()
            data[3].append(agg_staged_merge.return_cost())
            data_st[3].append(agg_staged_merge.TOTAL)
            data_total[3].append(cal_cum_res(agg_staged_merge.TOTAL))
            opt = OptimalPlacement(encoding_type, x, k, l, g)
            opt.placement()
            opt_staged_merge = StagedStripeMergingForOpt(len(x_list), x_list, k, l, g, opt)
            opt_staged_merge.stripe_merging()
            data[4].append(opt_staged_merge.return_cost())
            data_st[4].append(opt_staged_merge.TOTAL)
            data_total[4].append(cal_cum_res(opt_staged_merge.TOTAL))
        max_cost = max(max(data))
        yticks = generate_ytick(max_cost)
        if i == 0:
            yticks = [0, 200, 400, 600, 800]
            if encoding_type == "Optimal Cauchy LRC":
                yticks.append(1000)
        X_LIST_STR = EXP2_DIFFERENT_X_LIST_STR
        filename = 'sim_x_' + str(i) + '.pdf'
        fn = 'sim_x.pdf'
        if encoding_type == "Optimal Cauchy LRC":
            X_LIST_STR = EXP2_DIFFERENT_X_LIST_STR_CAUCHY
            filename = 'sim_x_' + str(i) + '_cauchy.pdf'
            fn = 'sim_x_cauchy.pdf'
        # draw_multi_bars(data, EXP2_DIFFERENT_STR[i], yticks, X_LIST_STR[i], 'Merging Cost',
        #                    (6.2, 4.0), 60, 11, 15.5, False, data_total, data_st, filename)
        draw_multi_bars_ad(data, EXP2_DIFFERENT_STR[i], yticks, X_LIST_STR[i], 'Merging Cost',
                           (6.2, 4.0), 60, 11, 15.5, False, data_total, data_st, fn)
