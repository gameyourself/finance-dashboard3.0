
from __future__ import annotations

from typing import Dict, List, Tuple

import pandas as pd


def safe_ratio(value: float, total: float) -> float:
    if total is None or abs(float(total)) < 1e-9:
        return 0.0
    try:
        return float(value) / float(total)
    except Exception:
        return 0.0


def safe_corr(series_a: pd.Series, series_b: pd.Series) -> float:
    try:
        if series_a.nunique(dropna=True) <= 1 or series_b.nunique(dropna=True) <= 1:
            return 0.0
        v = float(series_a.corr(series_b))
        if pd.isna(v):
            return 0.0
        return v
    except Exception:
        return 0.0


def get_time_window_keys(df: pd.DataFrame, end_year: int, end_month: int, window_label: str) -> List[str]:
    if df is None or df.empty or '期间' not in df.columns:
        return []
    valid_keys = df['期间'].astype(str).tolist()
    end_key = f'{end_year}年{end_month}月'
    if end_key not in valid_keys:
        return valid_keys
    end_idx = valid_keys.index(end_key)
    if window_label == '全部历史':
        return valid_keys[:end_idx + 1]
    if window_label == '本年':
        return [p for p in valid_keys[:end_idx + 1] if p.startswith(f'{end_year}年')]
    size_map = {'近3个月': 3, '近6个月': 6, '近1年': 12}
    size = size_map.get(window_label, 12)
    start_idx = max(0, end_idx - size + 1)
    return valid_keys[start_idx:end_idx + 1]


def slice_df_by_periods(df: pd.DataFrame, periods: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    order = {p: i for i, p in enumerate(periods)}
    work = df[df['期间'].isin(periods)].copy()
    if work.empty:
        return work
    work['_order'] = work['期间'].map(order)
    work = work.sort_values('_order').drop(columns=['_order']).reset_index(drop=True)
    return work


def latest_nonzero_months(df: pd.DataFrame, column: str, top_n: int = 3) -> pd.DataFrame:
    cols = ['期间', '年份', '月份', column, '决策说明', '说明']
    cols = [c for c in cols if c in df.columns]
    work = df[cols].copy()
    if column not in work.columns:
        return pd.DataFrame()
    work = work[work[column].abs() > 1e-9]
    if work.empty:
        return work
    sort_cols = ['_abs']
    work = work.assign(_abs=work[column].abs())
    if '年份' in work.columns and '月份' in work.columns:
        work = work.sort_values(['_abs', '年份', '月份'], ascending=[False, False, False])
    else:
        work = work.sort_values('_abs', ascending=False)
    return work.drop(columns=['_abs']).head(top_n).reset_index(drop=True)


def build_group_breakdown(record: dict, side: str, source_groups: Dict[str, List[str]]) -> pd.DataFrame:
    rows = []
    values = record['balance_sheet'][side]
    total = sum(float(values.get(item, 0) or 0) for items in source_groups.values() for item in items)
    for group, items in source_groups.items():
        amount = sum(float(values.get(item, 0) or 0) for item in items)
        rows.append({'大项': group, '金额': amount, '占比': safe_ratio(amount, total)})
    return pd.DataFrame(rows).sort_values('金额', ascending=False).reset_index(drop=True)


def build_item_breakdown(record: dict, side: str, group_name: str, source_groups: Dict[str, List[str]]) -> pd.DataFrame:
    values = record['balance_sheet'][side]
    items = source_groups.get(group_name, [])
    total = sum(float(values.get(item, 0) or 0) for item in items)
    rows = []
    for item in items:
        val = float(values.get(item, 0) or 0)
        rows.append({'子项': item, '金额': val, '占比': safe_ratio(val, total)})
    return pd.DataFrame(rows).sort_values('金额', ascending=False).reset_index(drop=True)


def build_current_group_factor_df(
    record: dict,
    side: str,
    group_name: str,
    cf_df_window: pd.DataFrame,
    source_groups: Dict[str, List[str]],
    group_cashflow_link_hints: Dict[Tuple[str, str], List[str]],
    cashflow_leaf_items: List[Tuple[str, str, str]],
) -> tuple[pd.DataFrame, str]:
    item_df = build_item_breakdown(record, side, group_name, source_groups)
    total = float(item_df['金额'].sum()) if not item_df.empty else 0.0
    if item_df.empty:
        return pd.DataFrame(columns=['方向', '因子', '当前值', '当前占比', '现金流关联', '结构贡献分']), '暂无可分析的结构信息。'

    hints = group_cashflow_link_hints.get((side, group_name), [])
    link_texts = []
    rows = []
    for _, r in item_df.iterrows():
        item = r['子项']
        current_share = safe_ratio(r['金额'], total)
        linked = []
        for hint in hints:
            col = None
            for lvl1, lvl2, lvl3 in cashflow_leaf_items:
                if lvl3 == hint:
                    maybe = f'{lvl1}/{lvl2}/{lvl3}'
                    if maybe in cf_df_window.columns:
                        col = maybe
                        break
            if col and cf_df_window[col].abs().sum() > 0:
                linked.append((hint, float(cf_df_window[col].abs().mean())))
        linked = sorted(linked, key=lambda x: x[1], reverse=True)[:2]
        linked_text = '、'.join([x[0] for x in linked]) if linked else '暂无明显关联'
        rows.append({
            '方向': '正向资产结构' if side == 'assets' else '负向负债结构',
            '因子': item,
            '当前值': float(r['金额']),
            '当前占比': current_share,
            '现金流关联': linked_text,
            '结构贡献分': current_share,
        })
        if linked:
            link_texts.append(f'{item} 主要与 {linked_text} 相关')

    top_text = '、'.join([f"{r['因子']}（{r['当前占比']*100:.1f}%）" for _, r in pd.DataFrame(rows).head(3).iterrows()])
    advice = f'当前{group_name}内部主要由 {top_text or "暂无核心子项"} 构成。'
    if link_texts:
        advice += ' 结合现金流看，' + '；'.join(link_texts[:3]) + '。'
    return pd.DataFrame(rows).sort_values('结构贡献分', ascending=False).reset_index(drop=True), advice


def _factor_table_for_balance(
    df_window: pd.DataFrame,
    record: dict,
    source_groups: Dict[str, List[str]],
    side_key: str,
    direction_label: str,
    base_total_key: str,
    target_series: pd.Series,
) -> pd.DataFrame:
    rows = []
    for group, items in source_groups.items():
        for item in items:
            if item not in df_window.columns:
                continue
            current_value = float(record['balance_sheet'][side_key].get(item, 0) or 0)
            rows.append({
                '方向': direction_label,
                '一级分类': group,
                '因子': item,
                '列名': item,
                '当前值': current_value,
                '当前占比': safe_ratio(current_value, max(float(record['derived'].get(base_total_key, 0) or 0), 1e-9)),
                '相关系数': safe_corr(df_window[item], target_series),
                '展示名': item,
            })
    factor_df = pd.DataFrame(rows)
    if factor_df.empty:
        return pd.DataFrame(columns=['方向', '一级分类', '因子', '列名', '展示名', '当前值', '当前占比', '相关系数', '综合贡献分'])
    factor_df['综合贡献分'] = factor_df['当前占比'].abs() * 0.6 + factor_df['相关系数'].abs() * 0.4
    return factor_df.sort_values('综合贡献分', ascending=False).reset_index(drop=True)


def _build_month_refs(df_window: pd.DataFrame, factor_df: pd.DataFrame, top_n_factors: int = 6, top_n_months: int = 2) -> pd.DataFrame:
    refs = []
    for _, row in factor_df.head(top_n_factors).iterrows():
        factor = row['因子']
        col_name = row.get('列名', factor)
        if col_name not in df_window.columns:
            continue
        top_months = latest_nonzero_months(df_window, col_name, top_n=top_n_months)
        for _, m_row in top_months.iterrows():
            refs.append({
                '因子': factor,
                '期间': m_row.get('期间', ''),
                '贡献值': float(m_row.get(col_name, 0) or 0),
                '决策说明': m_row.get('决策说明', '') or '暂无',
                '波动说明': m_row.get('说明', '') or '暂无',
            })
    if not refs:
        return pd.DataFrame(columns=['因子', '期间', '贡献值', '决策说明', '波动说明'])
    return pd.DataFrame(refs)


def compute_balance_factor_analysis(
    balance_df_all: pd.DataFrame,
    record: dict,
    year: int,
    month: int,
    window_label: str,
    balance_assets: Dict[str, List[str]],
    balance_liabilities: Dict[str, List[str]],
    target_metric: str = '净资产',
) -> dict:
    periods = get_time_window_keys(balance_df_all, year, month, window_label)
    df_window = slice_df_by_periods(balance_df_all, periods)
    target_series = df_window[target_metric] if target_metric in df_window.columns else df_window['净资产']

    asset_group_df = build_group_breakdown(record, 'assets', balance_assets)
    liability_group_df = build_group_breakdown(record, 'liabilities', balance_liabilities)

    asset_factor_df = _factor_table_for_balance(df_window, record, balance_assets, 'assets', '正向资产', '总资产', target_series)
    debt_factor_df = _factor_table_for_balance(df_window, record, balance_liabilities, 'liabilities', '负向负债', '总负债', target_series)
    asset_month_ref_df = _build_month_refs(df_window, asset_factor_df)
    debt_month_ref_df = _build_month_refs(df_window, debt_factor_df)

    positive = asset_factor_df.head(3)
    negative = debt_factor_df.head(3)
    pos_text = '、'.join([f"{r['因子']}（占比{r['当前占比']*100:.1f}%）" for _, r in positive.iterrows()]) if not positive.empty else '暂无明显正向资产因子'
    neg_text = '、'.join([f"{r['因子']}（占比{r['当前占比']*100:.1f}%）" for _, r in negative.iterrows()]) if not negative.empty else '暂无明显负向负债因子'
    summary = f'以下时间性影响因子基于“{window_label}”窗口计算。当前{target_metric}更受 {pos_text} 支撑，同时要关注 {neg_text} 带来的拖累。'

    advice = []
    if not positive.empty:
        advice.append('正向资产因子建议优先保护占比高且在当前窗口内更稳定的资产项目。')
    if not negative.empty:
        advice.append('负向负债因子建议结合现金流支出一起看，优先压降高频利息和可替代负债。')
    if not asset_month_ref_df.empty:
        refs = [f"{row['期间']}关于{row['因子']}的记录：{row['决策说明']}" for _, row in asset_month_ref_df.head(2).iterrows()]
        advice.append('资产侧可参考：' + '；'.join(refs) + '。')
    if not debt_month_ref_df.empty:
        refs = [f"{row['期间']}关于{row['因子']}的记录：{row['决策说明']}" for _, row in debt_month_ref_df.head(2).iterrows()]
        advice.append('负债侧可参考：' + '；'.join(refs) + '。')

    return {
        'asset_group_df': asset_group_df,
        'liability_group_df': liability_group_df,
        'asset_factor_df': asset_factor_df,
        'debt_factor_df': debt_factor_df,
        'asset_month_ref_df': asset_month_ref_df,
        'debt_month_ref_df': debt_month_ref_df,
        'summary': summary,
        'advice': ' '.join(advice),
        'periods': periods,
        'window_df': df_window,
    }


def compute_cashflow_factor_analysis(
    cashflow_df_all: pd.DataFrame,
    record: dict,
    year: int,
    month: int,
    window_label: str,
    cashflow_leaf_items: List[Tuple[str, str, str]],
    target_metric: str = '储蓄',
) -> dict:
    periods = get_time_window_keys(cashflow_df_all, year, month, window_label)
    df_window = slice_df_by_periods(cashflow_df_all, periods)
    target_series = df_window[target_metric] if target_metric in df_window.columns else df_window['储蓄']

    rows = []
    for lvl1, lvl2, lvl3 in cashflow_leaf_items:
        col = f'{lvl1}/{lvl2}/{lvl3}'
        if col not in df_window.columns:
            continue
        current_value = float(record['cashflow'][lvl1][lvl2].get(lvl3, 0) or 0)
        if lvl1 in ['工作收入', '理财收入']:
            sign = '正向收入/增益'
            base_total = float(record['derived'].get(lvl1, 0) or 0)
        else:
            sign = '负向支出/消耗'
            base_total = float(record['derived'].get(lvl1, 0) or 0)
        rows.append({
            '方向': sign,
            '一级分类': lvl1,
            '二级分类': lvl2,
            '因子': lvl3,
            '列名': col,
            '当前值': current_value,
            '当前占比': safe_ratio(current_value, max(base_total, 1e-9)),
            '相关系数': safe_corr(df_window[col], target_series),
            '展示名': f'{lvl1}-{lvl3}',
        })
    factor_df = pd.DataFrame(rows)
    if factor_df.empty:
        factor_df = pd.DataFrame(columns=['方向', '一级分类', '二级分类', '因子', '列名', '展示名', '当前值', '当前占比', '相关系数', '综合贡献分'])
    else:
        factor_df['综合贡献分'] = factor_df['当前占比'].abs() * 0.6 + factor_df['相关系数'].abs() * 0.4
        factor_df = factor_df.sort_values('综合贡献分', ascending=False).reset_index(drop=True)

    income_df = factor_df[factor_df['方向'] == '正向收入/增益'].copy().head(8)
    expense_df = factor_df[factor_df['方向'] == '负向支出/消耗'].copy().head(8)
    income_month_ref_df = _build_month_refs(df_window, income_df)
    expense_month_ref_df = _build_month_refs(df_window, expense_df)

    pos_text = '、'.join([f"{r['因子']}（占{r['一级分类']}{r['当前占比']*100:.1f}%）" for _, r in income_df.head(3).iterrows()]) if not income_df.empty else '暂无明显正向收入因子'
    neg_text = '、'.join([f"{r['因子']}（占{r['一级分类']}{r['当前占比']*100:.1f}%）" for _, r in expense_df.head(3).iterrows()]) if not expense_df.empty else '暂无明显负向支出因子'
    summary = f'以下现金流影响因子基于“{window_label}”窗口计算。当前{target_metric}主要由 {pos_text} 拉动，同时被 {neg_text} 消耗。'

    advice = []
    if not income_df.empty:
        advice.append('收入侧建议优先保护稳定工资、利息类和持续性更强的来源。')
    if not expense_df.empty:
        advice.append('支出侧建议先盯住占比高且当前窗口波动也大的项目，优先压降可替代支出。')
    if not income_month_ref_df.empty:
        refs = [f"{row['期间']}关于{row['因子']}的记录：{row['决策说明']}" for _, row in income_month_ref_df.head(2).iterrows()]
        advice.append('收入侧可参考：' + '；'.join(refs) + '。')
    if not expense_month_ref_df.empty:
        refs = [f"{row['期间']}关于{row['因子']}的记录：{row['决策说明']}" for _, row in expense_month_ref_df.head(2).iterrows()]
        advice.append('支出侧可参考：' + '；'.join(refs) + '。')

    return {
        'factor_df': factor_df,
        'income_df': income_df,
        'expense_df': expense_df,
        'income_month_ref_df': income_month_ref_df,
        'expense_month_ref_df': expense_month_ref_df,
        'summary': summary,
        'advice': ' '.join(advice),
        'periods': periods,
        'window_df': df_window,
    }
