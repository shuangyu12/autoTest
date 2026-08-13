"""增速总结趋势判断规则。

根据本周与上周的同比增速（YoY），输出趋势描述用语，共 11 种情况：

| 序号 | 本周   | 上周   | 结论     | 规则说明                         |
|------|--------|--------|----------|----------------------------------|
| 1    | +1.0%  | +2.0%  | 增速下降 | 双方都为正，写增速上升/下降      |
| 2    | +2.0%  | +1.0%  | 增速上升 |                                  |
| 3    | -1.0%  | -2.0%  | 降幅收窄 | 双方都为负，写降幅收窄/扩大      |
| 4    | -2.0%  | -1.0%  | 降幅扩大 |                                  |
| 5    | -1.0%  | +2.0%  | 环比转负 | 双方一正一负，写环比转负/转正    |
| 6    | +1.0%  | -1.0%  | 环比转正 |                                  |
| 7    | 0%     | -1.0%  | 环比转正 | 特殊情况                         |
| 8    | 0%     | +1.0%  | 增速下降 |                                  |
| 9    | -1.0%  | 0%     | 环比转负 |                                  |
| 10   | +1.0%  | 0%     | 增速上升 |                                  |
| 11   | 相等   | 相等   | 环比持平 |                                  |
"""


def judge_trend(current: float, previous: float) -> str:
    """根据本周与上周增速返回趋势描述。

    :param current: 本周同比增速（百分数值，如 +1.0 表示 +1.0%）
    :param previous: 上周同比增速
    :return: 趋势描述用语
    """
    if current == previous:
        return "环比持平"  # 规则11：两者一致
    if current > 0 and previous > 0:
        return "增速上升" if current > previous else "增速下降"  # 规则1/2
    if current < 0 and previous < 0:
        return "降幅收窄" if current > previous else "降幅扩大"  # 规则3/4
    if current < 0 <= previous:
        return "环比转负"  # 规则5/9（含上周为0）
    if current > 0 > previous:
        return "环比转正"  # 规则6
    if current == 0:
        return "环比转正" if previous < 0 else "增速下降"  # 规则7/8
    return "增速上升"  # 规则10：本周为正、上周为0
