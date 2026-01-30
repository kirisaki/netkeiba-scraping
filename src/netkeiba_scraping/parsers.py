import re


def parse_lap(x: str) -> float:
    """タイム文字列を秒に変換する"""
    if not x or x == '0' or ':' not in x:
        return 0.0
    xs = x.split(':')
    return float(xs[0]) * 60.0 + float(xs[1])


def parse_margin(x: str) -> float:
    """着差を馬身数に変換する"""
    x = str(x)
    if x == '0':
        return 0
    elif x == '同着':
        return 0
    elif x == 'ハナ':
        return 1/16
    elif x == 'アタマ':
        return 1/8
    elif x == 'クビ':
        return 1/4
    elif x == '1/2':
        return 1/2
    elif x == '1/4':
        return 1/4
    elif x == '3/4':
        return 3/4
    elif x == '大':
        return 11
    else:
        xs = re.split(r'[.+]', x)
        if len(xs) == 1:
            return float(x[0])
        else:
            return parse_margin(xs[0]) + parse_margin(xs[1])
