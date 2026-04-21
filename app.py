import copy
import io
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st

# =========================
# 基础配置
# =========================
BASE_DIR = Path(__file__).parent
RENDER_MOUNT_PATH = Path("/opt/render/project/src/finance_data_store")
SEED_DIR = BASE_DIR / "seed_data"
SEED_DB_FILE = SEED_DIR / "finance_db.json"


def resolve_data_dir() -> Path:
    env_path = os.getenv("RENDER_DISK_PATH", "").strip()
    if env_path:
        return Path(env_path)
    if str(BASE_DIR) == "/opt/render/project/src" or RENDER_MOUNT_PATH.exists():
        return RENDER_MOUNT_PATH
    return BASE_DIR / "finance_data_store"


DATA_DIR = resolve_data_dir()
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_FILE = DATA_DIR / "finance_db.json"
CONFIG_FILE = BASE_DIR / "config.json"

YEARS = list(range(25, 81))
MONTHS = list(range(1, 13))
MONTH_LABELS = [f"{m}月" for m in MONTHS]
YEAR_MONTH_KEYS = [f"{y}年{m}月" for y in YEARS for m in MONTHS]


def get_user_start_age(user: dict) -> int:
    profile = user.get("profile", {})
    return int(profile.get("start_age", 25) or 25)


def get_user_end_age(user: dict) -> int:
    profile = user.get("profile", {})
    end_age = int(profile.get("end_age", 35) or 35)
    return min(max(end_age, 35), 80)


def get_user_years(user: dict) -> List[int]:
    return list(range(get_user_start_age(user), get_user_end_age(user) + 1))

# =========================
# 现金流量表结构
# =========================
CASHFLOW_STRUCTURE = {
    "工作收入": {
        "主动收入": ["工资薪金", "劳务报酬", "经营所得", "养老金", "其他主动收入"],
        "转移收入": ["礼物礼金", "补贴补助", "继承收入", "其他转移收入"],
        "其他收入": ["退税收入", "偶然所得", "其他所得"],
    },
    "生活支出": {
        "生活消费": ["饮食开支", "居住成本", "通勤交通", "衣着形象", "日常用品", "医疗支出", "其他生活消费"],
        "娱乐消费": ["旅行度假", "奢侈体验", "游戏娱乐", "其他娱乐消费"],
        "个人成长": ["学习提升", "健康投资", "兴趣培养", "其他个人成长消费"],
        "家庭成员": ["赡养老人", "子女抚养", "宠物养护", "其他家庭成员消费"],
        "人际关系": ["人情往来", "商务应酬", "其他人际关系消费"],
        "其他支出": ["税费罚金", "意外损失", "其他无法归类支出"],
    },
    "理财收入": {
        "被动收入": ["理赔收入", "租金收入", "利息收入", "版权收入", "金融资产变现损益", "实物资产变现损益", "二手物品出售", "其他被动收入"],
    },
    "理财支出": {
        "财务支出": ["保费支出", "利息费用", "其他财务支出"],
    },
}

TOP_LEVEL_ORDER = ["工作收入", "生活支出", "工作储蓄", "理财收入", "理财支出", "理财储蓄", "储蓄"]

# =========================
# 资产负债表结构
# =========================
BALANCE_ASSETS = {
    "流动性资产": ["现金", "货币基金/活期", "其他流动性存款"],
    "投资性资产": ["定期存款", "外币存款", "股票投资", "债券投资", "基金投资", "投资性房地产", "其他投资性资产"],
    "自用性资产": ["自用房产", "自用汽车", "其他自用性资产"],
    "其他资产": ["其他资产"],
}
BALANCE_LIABILITIES = {
    "流动性负债": ["信用卡负债", "小额消费信贷", "其他流动性负债"],
    "投资性负债": ["金融投资借款", "实业投资借款", "投资性房地产贷款", "其他投资性负债"],
    "自用性负债": ["自住房按揭贷款", "自用车按揭贷款", "其他自用性负债"],
    "其他负债": ["其他负债"],
}
BALANCE_STRUCTURE_ORDER = [
    "流动性资产", "流动性负债",
    "投资性资产", "投资性负债",
    "自用性资产", "自用性负债",
    "其他资产", "其他负债",
]

# 现金流 -> 资产负债表映射
CF_TO_BALANCE_MAP = {
    "工资薪金": ("assets", "现金"),
    "劳务报酬": ("assets", "现金"),
    "经营所得": ("assets", "现金"),
    "养老金": ("assets", "现金"),
    "其他主动收入": ("assets", "现金"),
    "礼物礼金": ("assets", "现金"),
    "补贴补助": ("assets", "现金"),
    "继承收入": ("assets", "现金"),
    "其他转移收入": ("assets", "现金"),
    "退税收入": ("assets", "现金"),
    "偶然所得": ("assets", "现金"),
    "其他所得": ("assets", "现金"),
    "理赔收入": ("assets", "现金"),
    "租金收入": ("assets", "现金"),
    "利息收入": ("assets", "现金"),
    "版权收入": ("assets", "现金"),
    "金融资产变现损益": ("assets", "现金"),
    "实物资产变现损益": ("assets", "现金"),
    "二手物品出售": ("assets", "现金"),
    "其他被动收入": ("assets", "现金"),
    "饮食开支": ("assets", "现金"),
    "居住成本": ("assets", "现金"),
    "通勤交通": ("assets", "现金"),
    "衣着形象": ("assets", "现金"),
    "日常用品": ("assets", "现金"),
    "医疗支出": ("assets", "现金"),
    "其他生活消费": ("assets", "现金"),
    "旅行度假": ("assets", "现金"),
    "奢侈体验": ("assets", "现金"),
    "游戏娱乐": ("assets", "现金"),
    "其他娱乐消费": ("assets", "现金"),
    "学习提升": ("assets", "现金"),
    "健康投资": ("assets", "现金"),
    "兴趣培养": ("assets", "现金"),
    "其他个人成长消费": ("assets", "现金"),
    "赡养老人": ("assets", "现金"),
    "子女抚养": ("assets", "现金"),
    "宠物养护": ("assets", "现金"),
    "其他家庭成员消费": ("assets", "现金"),
    "人情往来": ("assets", "现金"),
    "商务应酬": ("assets", "现金"),
    "其他人际关系消费": ("assets", "现金"),
    "税费罚金": ("assets", "现金"),
    "意外损失": ("assets", "现金"),
    "其他无法归类支出": ("assets", "现金"),
    "保费支出": ("assets", "现金"),
    "利息费用": ("assets", "现金"),
    "其他财务支出": ("assets", "现金"),
}

CASHFLOW_OPTIONAL_ITEMS = [
    "工资薪金", "劳务报酬", "经营所得", "礼物礼金", "退税收入",
    "饮食开支", "居住成本", "通勤交通", "医疗支出", "旅行度假",
    "理赔收入", "租金收入", "利息收入", "保费支出",
    "子女抚养", "赡养老人", "人情往来",
]

BALANCE_OPTIONAL_ITEMS = [
    "现金", "货币基金/活期", "其他流动性存款", "定期存款", "外币存款",
    "股票投资", "债券投资", "基金投资", "投资性房地产", "其他投资性资产",
    "自用房产", "自用汽车", "其他自用性资产", "其他资产",
    "信用卡负债", "小额消费信贷", "其他流动性负债",
    "金融投资借款", "实业投资借款", "投资性房地产贷款", "其他投资性负债",
    "自住房按揭贷款", "自用车按揭贷款", "其他自用性负债", "其他负债",
]


# =========================
# 工具函数
# =========================
def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def bootstrap_seed_data():
    try:
        if not DB_FILE.exists() and SEED_DB_FILE.exists():
            DB_FILE.write_text(SEED_DB_FILE.read_text(encoding="utf-8"), encoding="utf-8")
    except Exception:
        pass


def load_db() -> dict:
    return load_json(DB_FILE, {"users": [], "transfers": []})


def save_db(db: dict):
    save_json(DB_FILE, db)


def load_config() -> dict:
    return load_json(CONFIG_FILE, {})


bootstrap_seed_data()


def get_deepseek_api_key() -> str:
    env_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    if env_key:
        return env_key
    config = load_config()
    return str(config.get("deepseek_api_key", "")).strip()


def call_deepseek(prompt: str, system_prompt: str, timeout: int = 120) -> str:
    api_key = get_deepseek_api_key()
    if not api_key:
        raise ValueError("未配置 DeepSeek API Key，请在环境变量或 config.json 中配置。")
    url = "https://api.deepseek.com/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.7,
    }
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def tolerant_json_loads(text: str):
    raw = (text or "").strip()
    if not raw:
        raise ValueError("输入为空。")
    raw = raw.replace("```json", "").replace("```JSON", "").replace("```", "").strip()
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start:end + 1])
    raise ValueError("不是有效 JSON。")


def fmt_money(v) -> str:
    try:
        return f"{float(v):,.2f}"
    except Exception:
        return "0.00"


def month_key(year: int, month: int) -> str:
    return f"{year}年{month}月"


def next_year_month(year: int, month: int) -> Tuple[Optional[int], Optional[int]]:
    idx = YEAR_MONTH_KEYS.index(month_key(year, month))
    if idx >= len(YEAR_MONTH_KEYS) - 1:
        return None, None
    next_key = YEAR_MONTH_KEYS[idx + 1]
    y = int(next_key.split("年")[0])
    m = int(next_key.split("年")[1].replace("月", ""))
    return y, m


def prev_year_month(year: int, month: int) -> Tuple[Optional[int], Optional[int]]:
    idx = YEAR_MONTH_KEYS.index(month_key(year, month))
    if idx <= 0:
        return None, None
    prev_key = YEAR_MONTH_KEYS[idx - 1]
    y = int(prev_key.split("年")[0])
    m = int(prev_key.split("年")[1].replace("月", ""))
    return y, m


def flatten_cashflow_items() -> List[Tuple[str, str, str]]:
    rows = []
    for lvl1, lvl2_map in CASHFLOW_STRUCTURE.items():
        for lvl2, lvl3_items in lvl2_map.items():
            for lvl3 in lvl3_items:
                rows.append((lvl1, lvl2, lvl3))
    return rows


def all_cashflow_leaf_items() -> List[str]:
    return [lvl3 for _, _, lvl3 in flatten_cashflow_items()]


def empty_cashflow() -> dict:
    return {
        lvl1: {
            lvl2: {lvl3: 0.0 for lvl3 in lvl3_items}
            for lvl2, lvl3_items in lvl2_map.items()
        }
        for lvl1, lvl2_map in CASHFLOW_STRUCTURE.items()
    }


def empty_balance_sheet() -> dict:
    assets = {item: 0.0 for group in BALANCE_ASSETS.values() for item in group}
    liabilities = {item: 0.0 for group in BALANCE_LIABILITIES.values() for item in group}
    return {"assets": assets, "liabilities": liabilities}


def empty_month_record(year: int, month: int) -> dict:
    return {
        "year": year,
        "month": month,
        "month_label": month_key(year, month),
        "note": "",
        "decision_summary": "",
        "cashflow": empty_cashflow(),
        "balance_sheet": empty_balance_sheet(),
        "derived": {
            "工作收入": 0.0,
            "生活支出": 0.0,
            "工作储蓄": 0.0,
            "理财收入": 0.0,
            "理财支出": 0.0,
            "理财储蓄": 0.0,
            "储蓄": 0.0,
            "总资产": 0.0,
            "总负债": 0.0,
            "净资产": 0.0,
        },
    }


def empty_user(name: str) -> dict:
    return {
        "id": f"user_{int(datetime.now().timestamp() * 1000)}",
        "name": name,
        "profile": {
            "name": name,
            "start_age": 25,
            "end_age": 35,
            "age": 25,
            "job": "",
            "city_level": "",
            "marital_status": "",
            "background": "",
            "basic_goal": "",
            "prediction_enabled": False,
            "event_settings": {
                "child_junior_high": False,
                "retirement": False,
                "illness": False,
                "illness_start_age": 48,
                "illness_end_age": 52,
                "illness_severity": "中等",
            },
        },
        "accounts": ["默认账户"],
        "timeline": {
            str(y): {str(m): empty_month_record(y, m) for m in MONTHS}
            for y in YEARS
        },
    }


def user_map(db: dict) -> Dict[str, dict]:
    return {u["id"]: u for u in db.get("users", [])}


def ensure_user_timeline(user: dict):
    if "timeline" not in user:
        user["timeline"] = {str(y): {str(m): empty_month_record(y, m) for m in MONTHS} for y in YEARS}
    user.setdefault("profile", {})
    user["profile"].setdefault("start_age", 25)
    user["profile"].setdefault("end_age", 35)
    user["profile"].setdefault("prediction_enabled", False)
    user["profile"].setdefault("event_settings", {
        "child_junior_high": False,
        "retirement": False,
        "illness": False,
        "illness_start_age": 48,
        "illness_end_age": 52,
        "illness_severity": "中等",
    })
    for y in YEARS:
        user["timeline"].setdefault(str(y), {})
        for m in MONTHS:
            user["timeline"][str(y)].setdefault(str(m), empty_month_record(y, m))
            rec = user["timeline"][str(y)][str(m)]
            rec.setdefault("year", y)
            rec.setdefault("month", m)
            rec.setdefault("month_label", month_key(y, m))
            rec.setdefault("note", "")
            rec.setdefault("decision_summary", "")
            rec.setdefault("cashflow", empty_cashflow())
            rec.setdefault("balance_sheet", empty_balance_sheet())
            rec.setdefault("derived", {
                "工作收入": 0.0,
                "生活支出": 0.0,
                "工作储蓄": 0.0,
                "理财收入": 0.0,
                "理财支出": 0.0,
                "理财储蓄": 0.0,
                "储蓄": 0.0,
                "总资产": 0.0,
                "总负债": 0.0,
                "净资产": 0.0,
            })


def get_record(user: dict, year: int, month: int) -> dict:
    ensure_user_timeline(user)
    return user["timeline"][str(year)][str(month)]


def sum_lvl1(record: dict, lvl1: str) -> float:
    total = 0.0
    for lvl2, lvl3_map in record["cashflow"][lvl1].items():
        total += sum(float(v or 0) for v in lvl3_map.values())
    return total


def compute_balance_totals(balance_sheet: dict) -> dict:
    totals = {}
    for group, items in BALANCE_ASSETS.items():
        totals[group] = sum(float(balance_sheet["assets"].get(i, 0) or 0) for i in items)
    for group, items in BALANCE_LIABILITIES.items():
        totals[group] = sum(float(balance_sheet["liabilities"].get(i, 0) or 0) for i in items)
    totals["总资产"] = sum(totals[g] for g in BALANCE_ASSETS)
    totals["总负债"] = sum(totals[g] for g in BALANCE_LIABILITIES)
    totals["净资产"] = totals["总资产"] - totals["总负债"]
    return totals


def recompute_record(record: dict, prev_record: Optional[dict] = None):
    work_income = sum_lvl1(record, "工作收入")
    living_expense = sum_lvl1(record, "生活支出")
    invest_income = sum_lvl1(record, "理财收入")
    invest_expense = sum_lvl1(record, "理财支出")
    work_saving = work_income - living_expense
    invest_saving = invest_income - invest_expense
    saving = work_saving + invest_saving

    if prev_record is not None:
        prev_bs = prev_record["balance_sheet"]
        record["balance_sheet"] = copy.deepcopy(prev_bs)

    # 先根据现金流同步到资产负债表
    bs = record["balance_sheet"]
    for _, _, item in flatten_cashflow_items():
        amount = 0.0
        for lvl1, lvl2_map in CASHFLOW_STRUCTURE.items():
            for lvl2, lvl3_items in lvl2_map.items():
                if item in lvl3_items:
                    amount = float(record["cashflow"][lvl1][lvl2].get(item, 0) or 0)
                    break
        side_target = CF_TO_BALANCE_MAP.get(item)
        if not side_target or amount == 0:
            continue
        side, target = side_target
        if side == "assets":
            # 收入加现金，支出减现金
            if item in all_income_items():
                bs["assets"][target] = max(0.0, float(bs["assets"].get(target, 0) or 0) + amount)
            else:
                bs["assets"][target] = max(0.0, float(bs["assets"].get(target, 0) or 0) - amount)

    # 特殊规则：若存在利息费用，则优先压降信用卡或小额贷的现金流压力只体现在现金减少，不自动减少本金
    # 特殊规则：若存在“其他负债调整/人工编辑”，保留人工编辑后的值

    record["derived"] = {
        "工作收入": work_income,
        "生活支出": living_expense,
        "工作储蓄": work_saving,
        "理财收入": invest_income,
        "理财支出": invest_expense,
        "理财储蓄": invest_saving,
        "储蓄": saving,
    }
    record["derived"].update(compute_balance_totals(bs))


def all_income_items() -> List[str]:
    income_roots = ["工作收入", "理财收入"]
    items = []
    for root in income_roots:
        for _, lvl3_items in CASHFLOW_STRUCTURE[root].items():
            items.extend(lvl3_items)
    return items


def propagate_user_from(user: dict, start_year: int, start_month: int):
    ensure_user_timeline(user)
    current_year, current_month = start_year, start_month
    prev_y, prev_m = prev_year_month(start_year, start_month)
    prev_record = get_record(user, prev_y, prev_m) if prev_y is not None else None

    while current_year is not None and current_month is not None and current_year <= get_user_end_age(user):
        record = get_record(user, current_year, current_month)
        recompute_record(record, prev_record)
        prev_record = copy.deepcopy(record)
        current_year, current_month = next_year_month(current_year, current_month)


def year_month_dataframe(user: dict) -> pd.DataFrame:
    rows = []
    ensure_user_timeline(user)
    for y in get_user_years(user):
        for m in MONTHS:
            rec = get_record(user, y, m)
            row = {
                "年份": y,
                "月份": m,
                "期间": month_key(y, m),
                "工作收入": rec["derived"].get("工作收入", 0.0),
                "生活支出": rec["derived"].get("生活支出", 0.0),
                "工作储蓄": rec["derived"].get("工作储蓄", 0.0),
                "理财收入": rec["derived"].get("理财收入", 0.0),
                "理财支出": rec["derived"].get("理财支出", 0.0),
                "理财储蓄": rec["derived"].get("理财储蓄", 0.0),
                "储蓄": rec["derived"].get("储蓄", 0.0),
                "总资产": rec["derived"].get("总资产", 0.0),
                "总负债": rec["derived"].get("总负债", 0.0),
                "净资产": rec["derived"].get("净资产", 0.0),
                "说明": rec.get("note", ""),
                "决策说明": rec.get("decision_summary", ""),
            }
            # 三级项平铺
            for lvl1, lvl2, lvl3 in flatten_cashflow_items():
                row[f"{lvl1}/{lvl2}/{lvl3}"] = float(rec["cashflow"][lvl1][lvl2].get(lvl3, 0) or 0)
            rows.append(row)
    return pd.DataFrame(rows)


def yearly_summary_dataframe(user: dict) -> pd.DataFrame:
    df = cached_year_month_dataframe(get_user_snapshot(user))
    numeric_cols = [c for c in df.columns if c not in ["期间", "说明", "决策说明"]]
    agg = df.groupby("年份")[numeric_cols[1:]].sum(numeric_only=True).reset_index()
    return agg


def five_year_blocks_for_user(user: dict) -> List[str]:
    start = get_user_start_age(user)
    end = get_user_end_age(user)
    blocks = []
    current = start
    while current <= end:
        block_end = min(current + 4, end)
        blocks.append(f"{current}-{block_end}")
        current += 5
    return blocks


def build_five_year_df(user: dict) -> pd.DataFrame:
    ydf = cached_yearly_summary_dataframe(get_user_snapshot(user))
    if ydf.empty:
        return pd.DataFrame()

    def block(y):
        base = get_user_start_age(user) + ((int(y) - get_user_start_age(user)) // 5) * 5
        block_end = min(base + 4, get_user_end_age(user))
        return f"{base}-{block_end}"

    ydf["五年区间"] = ydf["年份"].apply(block)
    numeric_cols = [c for c in ydf.columns if c not in ["年份", "五年区间"]]
    return ydf.groupby("五年区间", as_index=False)[numeric_cols].sum(numeric_only=True)


def get_user_snapshot(user: dict) -> str:
    return json.dumps(user, ensure_ascii=False, sort_keys=True)


@st.cache_data(show_spinner=False)
def cached_year_month_dataframe(user_snapshot: str) -> pd.DataFrame:
    return year_month_dataframe(json.loads(user_snapshot))


@st.cache_data(show_spinner=False)
def cached_yearly_summary_dataframe(user_snapshot: str) -> pd.DataFrame:
    return yearly_summary_dataframe(json.loads(user_snapshot))


@st.cache_data(show_spinner=False)
def cached_five_year_df(user_snapshot: str) -> pd.DataFrame:
    return build_five_year_df(json.loads(user_snapshot))


@st.cache_data(show_spinner=False)
def build_user_excel_bytes(user_snapshot: str) -> bytes:
    user = json.loads(user_snapshot)
    monthly_df = cached_year_month_dataframe(get_user_snapshot(user))
    yearly_df = yearly_summary_dataframe(user)
    five_year_df = build_five_year_df(user)
    bs_rows = []
    std_cf_rows = []
    std_bs_rows = []
    for y in get_user_years(user):
        for m in MONTHS:
            rec = get_record(user, y, m)
            row = {"年份": y, "月份": m, "期间": month_key(y, m)}
            row.update(rec["balance_sheet"]["assets"])
            row.update(rec["balance_sheet"]["liabilities"])
            row.update(compute_balance_totals(rec["balance_sheet"]))
            bs_rows.append(row)

            cf_df = build_cashflow_statement_rows(rec).copy()
            cf_df.insert(0, "期间", rec["month_label"])
            std_cf_rows.append(cf_df)

            bs_std_df = build_balance_statement_rows(rec).copy()
            bs_std_df.insert(0, "期间", rec["month_label"])
            std_bs_rows.append(bs_std_df)

    bs_df = pd.DataFrame(bs_rows)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        monthly_df.to_excel(writer, sheet_name="月度汇总", index=False)
        yearly_df.to_excel(writer, sheet_name="年度汇总", index=False)
        five_year_df.to_excel(writer, sheet_name="五年汇总", index=False)
        bs_df.to_excel(writer, sheet_name="资产负债表", index=False)
        pd.concat(std_cf_rows, ignore_index=True).to_excel(writer, sheet_name="标准现金流量表", index=False)
        pd.concat(std_bs_rows, ignore_index=True).to_excel(writer, sheet_name="标准资产负债表", index=False)
    output.seek(0)
    return output.getvalue()


def compare_months_by_item(user: dict, start_key: str, end_key: str, items: Optional[List[str]] = None) -> pd.DataFrame:
    sy = int(start_key.split("年")[0])
    sm = int(start_key.split("年")[1].replace("月", ""))
    ey = int(end_key.split("年")[0])
    em = int(end_key.split("年")[1].replace("月", ""))
    start_rec = get_record(user, sy, sm)
    end_rec = get_record(user, ey, em)
    rows = []
    for lvl1, lvl2, lvl3 in flatten_cashflow_items():
        if items and lvl3 not in items:
            continue
        s = float(start_rec["cashflow"][lvl1][lvl2].get(lvl3, 0) or 0)
        e = float(end_rec["cashflow"][lvl1][lvl2].get(lvl3, 0) or 0)
        diff = e - s
        rows.append({
            "一级分类": lvl1,
            "二级分类": lvl2,
            "三级分类": lvl3,
            start_key: s,
            end_key: e,
            "差额": diff,
            "方向": "增加" if diff > 0 else ("减少" if diff < 0 else "持平")
        })
    return pd.DataFrame(rows)


def compare_lvl2_totals(user: dict, start_key: str, end_key: str) -> pd.DataFrame:
    sy = int(start_key.split("年")[0])
    sm = int(start_key.split("年")[1].replace("月", ""))
    ey = int(end_key.split("年")[0])
    em = int(end_key.split("年")[1].replace("月", ""))
    start_rec = get_record(user, sy, sm)
    end_rec = get_record(user, ey, em)
    rows = []
    for lvl1, lvl2_map in CASHFLOW_STRUCTURE.items():
        for lvl2, lvl3_items in lvl2_map.items():
            s = sum(float(start_rec["cashflow"][lvl1][lvl2].get(i, 0) or 0) for i in lvl3_items)
            e = sum(float(end_rec["cashflow"][lvl1][lvl2].get(i, 0) or 0) for i in lvl3_items)
            diff = e - s
            rows.append({
                "一级分类": lvl1,
                "二级分类": lvl2,
                start_key: s,
                end_key: e,
                "差额": diff,
                "方向": "增加" if diff > 0 else ("减少" if diff < 0 else "持平")
            })
    return pd.DataFrame(rows)


def style_diff_df(df: pd.DataFrame):
    def color_diff(v):
        try:
            v = float(v)
        except Exception:
            return ""
        if v > 0:
            return "color:red;"
        if v < 0:
            return "color:blue;"
        return ""
    style_cols = [c for c in df.columns if "差额" in c or c == "差额"]
    return df.style.applymap(color_diff, subset=style_cols)


def make_line_chart(df: pd.DataFrame, x_col: str, y_cols: List[str], title: str):
    fig = go.Figure()
    for col in y_cols:
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df[col], mode="lines+markers", name=col,
            hovertemplate=f"%{{x}}<br>{col}: %{{y:,.2f}}<extra></extra>"
        ))
    fig.update_layout(title=title, xaxis_title=x_col, yaxis_title="金额")
    return fig


def months_in_last_six(year: int, month: int) -> List[str]:
    idx = YEAR_MONTH_KEYS.index(month_key(year, month))
    start_idx = max(0, idx - 5)
    return YEAR_MONTH_KEYS[start_idx:idx + 1]


def slice_item_trend(user: dict, item_name: str, keys: List[str]) -> pd.DataFrame:
    rows = []
    for key in keys:
        y = int(key.split("年")[0])
        m = int(key.split("年")[1].replace("月", ""))
        rec = get_record(user, y, m)
        value = 0.0
        found = False
        for lvl1, lvl2, lvl3 in flatten_cashflow_items():
            if lvl3 == item_name:
                value = float(rec["cashflow"][lvl1][lvl2].get(lvl3, 0) or 0)
                found = True
                break
        if not found and item_name in rec["derived"]:
            value = float(rec["derived"].get(item_name, 0) or 0)
        rows.append({"期间": key, item_name: value})
    return pd.DataFrame(rows)


def export_user_excel(user: dict, filepath: Path):
    filepath.write_bytes(build_user_excel_bytes(get_user_snapshot(user)))


def add_user(db: dict, name: str):
    user = empty_user(name)
    db["users"].append(user)
    save_db(db)
    return user["id"]


def apply_transfer(db: dict, from_user: dict, to_user: dict, year: int, month: int, amount: float, note: str, from_account: str, to_account: str):
    if amount <= 0:
        raise ValueError("转账金额必须大于0。")
    from_rec = get_record(from_user, year, month)
    to_rec = get_record(to_user, year, month)

    from_rec["balance_sheet"]["assets"]["现金"] = max(0.0, float(from_rec["balance_sheet"]["assets"].get("现金", 0) or 0) - amount)
    to_rec["balance_sheet"]["assets"]["现金"] = float(to_rec["balance_sheet"]["assets"].get("现金", 0) or 0) + amount

    from_rec["note"] = (from_rec.get("note", "") + f"\n[{month_key(year, month)}] 向 {to_user['name']} 转账 {amount:.2f} 元，账户：{from_account}，备注：{note}").strip()
    to_rec["note"] = (to_rec.get("note", "") + f"\n[{month_key(year, month)}] 收到 {from_user['name']} 转账 {amount:.2f} 元，账户：{to_account}，备注：{note}").strip()

    propagate_user_from(from_user, year, month)
    propagate_user_from(to_user, year, month)

    db.setdefault("transfers", []).append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "period": month_key(year, month),
        "from_user": from_user["name"],
        "to_user": to_user["name"],
        "from_account": from_account,
        "to_account": to_account,
        "amount": amount,
        "note": note,
    })
    save_db(db)


def build_ai_prompt(background: str, goals: List[str], selected_cashflow_items: List[str], selected_balance_items: List[str]) -> str:
    cashflow_text = "、".join(selected_cashflow_items) if selected_cashflow_items else "系统自动判断"
    balance_text = "、".join(selected_balance_items) if selected_balance_items else "系统自动判断"
    goal_text = "；".join(goals) if goals else "优先保证月度现金流稳定"
    return f"""
你是一名家庭财务建模助手。请输出一个严格 JSON，用于模拟一个用户从25岁到35岁、共11年132个月的家庭财务数据。
要求：
1. JSON 顶层包含 profile 和 timeline 两部分。
2. timeline 按 25 到 35 岁、每年 1 到 12 月逐月生成。
3. 每个月必须有 note 和 decision_summary，说明本月决策与波动原因。
4. 只生成符合常规逻辑的数据，不要夸张。
5. 没有必要的字段可以为0。
6. 用户勾选的现金流小类和资产负债表项目优先生成，但即使勾选也不是每月都必须有。
7. 工作储蓄=工作收入-生活支出；理财储蓄=理财收入-理财支出；储蓄=工作储蓄+理财储蓄。
8. 尽量让房贷逐步下降，现金、储蓄、消费有连续性。

人物背景：{background}
理财目标偏好：{goal_text}
重点关注现金流小类：{cashflow_text}
重点关注资产负债表项目：{balance_text}

profile 示例字段：
{{
  "name": "用户A",
  "start_age": 25,
  "end_age": 35,
  "age": 25,
  "job": "运营/白领/教师等",
  "city_level": "一线/二线/三线",
  "marital_status": "未婚/已婚",
  "background": "...",
  "basic_goal": "..."
}}

timeline 结构示例：
{{
  "25": {{
    "1": {{
      "note": "...",
      "decision_summary": "...",
      "cashflow": {{
        "工作收入": {{"主动收入": {{"工资薪金": 12000}}}},
        "生活支出": {{"生活消费": {{"饮食开支": 2200}}}},
        "理财收入": {{"被动收入": {{"利息收入": 100}}}},
        "理财支出": {{"财务支出": {{"保费支出": 300}}}}
      }},
      "balance_sheet": {{
        "assets": {{"现金": 50000, "货币基金/活期": 3000, "自用房产": 1200000}},
        "liabilities": {{"自住房按揭贷款": 680000, "信用卡负债": 1000}}
      }}
    }}
  }}
}}

只输出 JSON，不要输出解释。
"""


def generate_mock_timeline(background: str, goals: List[str], selected_cashflow_items: List[str], selected_balance_items: List[str], progress_callback=None) -> dict:
    import random

    rng = random.Random(20260421)
    profile = {
        "name": "测试用户A",
        "start_age": 25,
        "end_age": 35,
        "age": 25,
        "job": "企业职员",
        "city_level": "二线",
        "marital_status": "已婚",
        "background": background.strip() or "已婚工薪家庭，收入相对稳定，关注现金流和负债控制。",
        "basic_goal": "；".join(goals) if goals else "优先稳定月度现金流",
        "prediction_enabled": False,
        "event_settings": {
            "child_junior_high": False,
            "retirement": False,
            "illness": False,
            "illness_start_age": 48,
            "illness_end_age": 52,
            "illness_severity": "中等",
        },
    }

    selected_cashflow_items = selected_cashflow_items or ["工资薪金", "饮食开支", "居住成本", "利息收入"]
    selected_balance_items = selected_balance_items or ["现金", "定期存款", "自用房产", "自住房按揭贷款"]

    timeline = {}
    cash_balance = 85000.0
    mortgage = 620000.0 if "自住房按揭贷款" in selected_balance_items else 0.0
    house_value = 1200000.0 if "自用房产" in selected_balance_items else 0.0
    fund_balance = 8000.0 if "货币基金/活期" in selected_balance_items else 0.0
    deposit_balance = 20000.0 if "定期存款" in selected_balance_items else 0.0
    stock_balance = 10000.0 if "股票投资" in selected_balance_items else 0.0
    bond_balance = 6000.0 if "债券投资" in selected_balance_items else 0.0
    fund_invest_balance = 12000.0 if "基金投资" in selected_balance_items else 0.0
    cc_debt = 2500.0 if "信用卡负债" in selected_balance_items else 0.0
    small_loan = 0.0 if "小额消费信贷" in selected_balance_items else 0.0

    total_months = len(YEARS) * len(MONTHS)
    month_counter = 0
    spring_festival = {1, 2}
    summer = {7, 8}
    year_end = {12}

    for y in YEARS:
        timeline[str(y)] = {}
        salary_base = 12000 + (y - 25) * 700 + rng.randint(-300, 300)
        for m in MONTHS:
            month_counter += 1
            cf = empty_cashflow()
            bs = empty_balance_sheet()

            salary = max(8000, salary_base + rng.randint(-500, 500))
            living_food = rng.randint(1800, 3200)
            housing_cost = rng.randint(2200, 4800)
            transport = rng.randint(300, 900)
            medical = rng.randint(0, 500)
            interest_income = int((deposit_balance + fund_balance) * rng.uniform(0.0015, 0.0035))
            insurance = rng.randint(200, 600)

            if "工资薪金" in selected_cashflow_items:
                cf["工作收入"]["主动收入"]["工资薪金"] = float(salary)
            if "劳务报酬" in selected_cashflow_items and rng.random() < 0.12:
                cf["工作收入"]["主动收入"]["劳务报酬"] = float(rng.randint(800, 3500))
            if "经营所得" in selected_cashflow_items and rng.random() < 0.08:
                cf["工作收入"]["主动收入"]["经营所得"] = float(rng.randint(1000, 4000))
            if "礼物礼金" in selected_cashflow_items and m in spring_festival and rng.random() < 0.5:
                cf["工作收入"]["转移收入"]["礼物礼金"] = float(rng.randint(500, 3000))
            if "退税收入" in selected_cashflow_items and m == 3 and rng.random() < 0.4:
                cf["工作收入"]["其他收入"]["退税收入"] = float(rng.randint(300, 1800))

            if "饮食开支" in selected_cashflow_items:
                cf["生活支出"]["生活消费"]["饮食开支"] = float(living_food)
            if "居住成本" in selected_cashflow_items:
                cf["生活支出"]["生活消费"]["居住成本"] = float(housing_cost)
            if "通勤交通" in selected_cashflow_items:
                cf["生活支出"]["生活消费"]["通勤交通"] = float(transport)
            if "医疗支出" in selected_cashflow_items and medical > 0:
                cf["生活支出"]["生活消费"]["医疗支出"] = float(medical)
            if "旅行度假" in selected_cashflow_items and m in summer and rng.random() < 0.35:
                cf["生活支出"]["娱乐消费"]["旅行度假"] = float(rng.randint(1500, 8000))
            if "子女抚养" in selected_cashflow_items and rng.random() < 0.55:
                cf["生活支出"]["家庭成员"]["子女抚养"] = float(rng.randint(800, 2600))
            if "赡养老人" in selected_cashflow_items and rng.random() < 0.28:
                cf["生活支出"]["家庭成员"]["赡养老人"] = float(rng.randint(500, 1800))
            if "人情往来" in selected_cashflow_items and rng.random() < 0.2:
                cf["生活支出"]["人际关系"]["人情往来"] = float(rng.randint(300, 1500))

            if "利息收入" in selected_cashflow_items:
                cf["理财收入"]["被动收入"]["利息收入"] = float(interest_income)
            if "租金收入" in selected_cashflow_items and rng.random() < 0.08:
                cf["理财收入"]["被动收入"]["租金收入"] = float(rng.randint(1500, 4000))
            if "理赔收入" in selected_cashflow_items and rng.random() < 0.03:
                cf["理财收入"]["被动收入"]["理赔收入"] = float(rng.randint(1000, 5000))
            if "保费支出" in selected_cashflow_items:
                cf["理财支出"]["财务支出"]["保费支出"] = float(insurance)

            work_income = sum(sum(v.values()) for v in cf["工作收入"].values())
            living_expense = sum(sum(v.values()) for v in cf["生活支出"].values())
            invest_income = sum(sum(v.values()) for v in cf["理财收入"].values())
            invest_expense = sum(sum(v.values()) for v in cf["理财支出"].values())
            monthly_saving = work_income + invest_income - living_expense - invest_expense

            if mortgage > 0:
                mortgage_payment = min(mortgage, rng.randint(1800, 2600))
                mortgage -= mortgage_payment
                cash_balance -= mortgage_payment
            if cc_debt > 0 and rng.random() < 0.4:
                repay = min(cc_debt, rng.randint(300, 1200))
                cc_debt -= repay
                cash_balance -= repay
            if small_loan > 0 and rng.random() < 0.35:
                repay = min(small_loan, rng.randint(300, 1500))
                small_loan -= repay
                cash_balance -= repay

            cash_balance += monthly_saving

            if "货币基金/活期" in selected_balance_items and cash_balance > 30000 and rng.random() < 0.35:
                move = min(rng.randint(1000, 6000), cash_balance * 0.2)
                cash_balance -= move
                fund_balance += move
            if "定期存款" in selected_balance_items and m in {6, 12} and cash_balance > 40000:
                move = min(rng.randint(3000, 12000), cash_balance * 0.25)
                cash_balance -= move
                deposit_balance += move
            if "股票投资" in selected_balance_items and rng.random() < 0.18 and cash_balance > 25000:
                move = min(rng.randint(1000, 5000), cash_balance * 0.18)
                cash_balance -= move
                stock_balance += move * rng.uniform(0.98, 1.08)
            if "债券投资" in selected_balance_items and rng.random() < 0.12 and cash_balance > 22000:
                move = min(rng.randint(1000, 4000), cash_balance * 0.15)
                cash_balance -= move
                bond_balance += move * rng.uniform(0.995, 1.03)
            if "基金投资" in selected_balance_items and rng.random() < 0.2 and cash_balance > 26000:
                move = min(rng.randint(1000, 4500), cash_balance * 0.16)
                cash_balance -= move
                fund_invest_balance += move * rng.uniform(0.985, 1.06)
            if "小额消费信贷" in selected_balance_items and rng.random() < 0.05:
                small_loan += rng.randint(1000, 5000)

            cash_balance = max(5000.0, cash_balance)

            if "现金" in selected_balance_items:
                bs["assets"]["现金"] = round(cash_balance, 2)
            if "货币基金/活期" in selected_balance_items:
                bs["assets"]["货币基金/活期"] = round(fund_balance, 2)
            if "定期存款" in selected_balance_items:
                bs["assets"]["定期存款"] = round(deposit_balance, 2)
            if "股票投资" in selected_balance_items:
                bs["assets"]["股票投资"] = round(stock_balance, 2)
            if "债券投资" in selected_balance_items:
                bs["assets"]["债券投资"] = round(bond_balance, 2)
            if "基金投资" in selected_balance_items:
                bs["assets"]["基金投资"] = round(fund_invest_balance, 2)
            if "自用房产" in selected_balance_items:
                bs["assets"]["自用房产"] = round(house_value * (1 + 0.002 * (y - 25)), 2)
            if "其他资产" in selected_balance_items and rng.random() < 0.08:
                bs["assets"]["其他资产"] = float(rng.randint(1000, 8000))

            if "信用卡负债" in selected_balance_items:
                bs["liabilities"]["信用卡负债"] = round(cc_debt, 2)
            if "小额消费信贷" in selected_balance_items:
                bs["liabilities"]["小额消费信贷"] = round(small_loan, 2)
            if "自住房按揭贷款" in selected_balance_items:
                bs["liabilities"]["自住房按揭贷款"] = round(mortgage, 2)
            if "其他负债" in selected_balance_items and rng.random() < 0.04:
                bs["liabilities"]["其他负债"] = float(rng.randint(500, 5000))

            if m in year_end:
                decision = "年末集中做收支复盘，适度增加储蓄和稳健配置。"
            elif m in spring_festival:
                decision = "节假日家庭支出偏高，但仍尽量维持现金流稳定。"
            elif m in summer:
                decision = "暑期存在阶段性消费提升，同时继续控制大额负债。"
            else:
                decision = "本月以稳定工资收入、控制生活支出和逐步优化资产配置为主。"
            note = f"本月工资和日常开支正常波动，月末现金约为{round(cash_balance, 2)}元，储蓄策略保持稳健。"

            timeline[str(y)][str(m)] = {
                "note": note,
                "decision_summary": decision,
                "cashflow": cf,
                "balance_sheet": bs,
            }

            if progress_callback:
                progress_callback(month_counter, total_months, f"正在生成 {y}岁{m}月")

    return {"profile": profile, "timeline": timeline}
def import_ai_timeline_into_user(user: dict, ai_json: dict):
    if "profile" in ai_json and isinstance(ai_json["profile"], dict):
        user["profile"].update(ai_json["profile"])
    if "timeline" not in ai_json:
        raise ValueError("JSON 中缺少 timeline。")
    ensure_user_timeline(user)
    for y_str, months_map in ai_json["timeline"].items():
        if str(y_str) not in user["timeline"]:
            continue
        if not isinstance(months_map, dict):
            continue
        for m_str, payload in months_map.items():
            if str(m_str) not in user["timeline"][str(y_str)]:
                continue
            rec = get_record(user, int(y_str), int(m_str))
            if not isinstance(payload, dict):
                continue
            rec["note"] = payload.get("note", rec.get("note", ""))
            rec["decision_summary"] = payload.get("decision_summary", rec.get("decision_summary", ""))
            for lvl1, lvl2_map in payload.get("cashflow", {}).items():
                if lvl1 not in rec["cashflow"]:
                    continue
                for lvl2, lvl3_map in lvl2_map.items():
                    if lvl2 not in rec["cashflow"][lvl1]:
                        continue
                    for lvl3, value in lvl3_map.items():
                        if lvl3 in rec["cashflow"][lvl1][lvl2]:
                            rec["cashflow"][lvl1][lvl2][lvl3] = float(value or 0)
            bs = payload.get("balance_sheet", {})
            for item, value in bs.get("assets", {}).items():
                if item in rec["balance_sheet"]["assets"]:
                    rec["balance_sheet"]["assets"][item] = float(value or 0)
            for item, value in bs.get("liabilities", {}).items():
                if item in rec["balance_sheet"]["liabilities"]:
                    rec["balance_sheet"]["liabilities"][item] = float(value or 0)
    propagate_user_from(user, 25, 1)


# =========================
# 页面样式
# =========================
def inject_styles():
    st.markdown(
        """
        <style>
        .small-muted {color:#6b7280;font-size:13px;}
        .block-card {padding:12px 14px;border:1px solid #e5e7eb;border-radius:12px;background:#fff;margin-bottom:10px;}
        .compare-box {padding:10px 12px;border-left:4px solid #6366f1;background:#f8faff;border-radius:8px;margin-bottom:10px;}
        .red-tip {color:#dc2626;font-weight:600;}
        .blue-tip {color:#2563eb;font-weight:600;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def show_balance_summary(record: dict):
    totals = compute_balance_totals(record["balance_sheet"])
    c1, c2, c3 = st.columns(3)
    c1.metric("总资产", fmt_money(totals["总资产"]))
    c2.metric("总负债", fmt_money(totals["总负债"]))
    c3.metric("净资产", fmt_money(totals["净资产"]))


def render_cashflow_editor(record: dict):
    st.subheader(f"{record['month_label']} 现金流量录入")
    for lvl1, lvl2_map in CASHFLOW_STRUCTURE.items():
        with st.expander(lvl1, expanded=(lvl1 in ["工作收入", "生活支出"])):
            for lvl2, lvl3_items in lvl2_map.items():
                st.markdown(f"**{lvl2}**")
                cols = st.columns(3)
                for i, lvl3 in enumerate(lvl3_items):
                    current_val = float(record["cashflow"][lvl1][lvl2].get(lvl3, 0) or 0)
                    record["cashflow"][lvl1][lvl2][lvl3] = cols[i % 3].number_input(
                        lvl3, min_value=0.0, value=current_val, step=100.0, key=f"cf_{record['month_label']}_{lvl1}_{lvl2}_{lvl3}"
                    )
    record["decision_summary"] = st.text_area("本月决策说明", value=record.get("decision_summary", ""), height=100)
    record["note"] = st.text_area("本月波动说明", value=record.get("note", ""), height=120)


def render_balance_editor(record: dict):
    st.subheader(f"{record['month_label']} 资产负债表修正")
    with st.expander("资产端", expanded=True):
        for group, items in BALANCE_ASSETS.items():
            st.markdown(f"**{group}**")
            cols = st.columns(3)
            for i, item in enumerate(items):
                current_val = float(record["balance_sheet"]["assets"].get(item, 0) or 0)
                record["balance_sheet"]["assets"][item] = cols[i % 3].number_input(
                    item, min_value=0.0, value=current_val, step=1000.0, key=f"bs_a_{record['month_label']}_{item}"
                )
    with st.expander("负债端", expanded=True):
        for group, items in BALANCE_LIABILITIES.items():
            st.markdown(f"**{group}**")
            cols = st.columns(3)
            for i, item in enumerate(items):
                current_val = float(record["balance_sheet"]["liabilities"].get(item, 0) or 0)
                record["balance_sheet"]["liabilities"][item] = cols[i % 3].number_input(
                    item, min_value=0.0, value=current_val, step=1000.0, key=f"bs_l_{record['month_label']}_{item}"
                )



def build_cashflow_statement_rows(record: dict) -> pd.DataFrame:
    rows = []
    for lvl1, lvl2_map in CASHFLOW_STRUCTURE.items():
        lvl1_total = sum_lvl1(record, lvl1)
        rows.append({"AFP家财分类": lvl1, "一级分类": "", "二级分类": "", "金额": lvl1_total, "层级": 1})
        for lvl2, lvl3_items in lvl2_map.items():
            lvl2_total = sum(float(record["cashflow"][lvl1][lvl2].get(i, 0) or 0) for i in lvl3_items)
            rows.append({"AFP家财分类": "", "一级分类": lvl2, "二级分类": "", "金额": lvl2_total, "层级": 2})
            for lvl3 in lvl3_items:
                value = float(record["cashflow"][lvl1][lvl2].get(lvl3, 0) or 0)
                rows.append({"AFP家财分类": "", "一级分类": "", "二级分类": lvl3, "金额": value, "层级": 3})
    rows.append({"AFP家财分类": "工作储蓄", "一级分类": "", "二级分类": "", "金额": float(record["derived"].get("工作储蓄", 0) or 0), "层级": 1})
    rows.append({"AFP家财分类": "理财储蓄", "一级分类": "", "二级分类": "", "金额": float(record["derived"].get("理财储蓄", 0) or 0), "层级": 1})
    rows.append({"AFP家财分类": "储蓄", "一级分类": "", "二级分类": "", "金额": float(record["derived"].get("储蓄", 0) or 0), "层级": 1})
    return pd.DataFrame(rows)


def build_balance_statement_rows(record: dict) -> pd.DataFrame:
    totals = compute_balance_totals(record["balance_sheet"])
    left_rows = []
    for group, items in BALANCE_ASSETS.items():
        left_rows.append({"名称": group, "金额": totals.get(group, 0), "层级": 1})
        for item in items:
            left_rows.append({"名称": item, "金额": float(record["balance_sheet"]["assets"].get(item, 0) or 0), "层级": 2})

    right_rows = []
    for group, items in BALANCE_LIABILITIES.items():
        right_rows.append({"名称": group, "金额": totals.get(group, 0), "层级": 1})
        for item in items:
            right_rows.append({"名称": item, "金额": float(record["balance_sheet"]["liabilities"].get(item, 0) or 0), "层级": 2})
    right_rows.append({"名称": "净资产", "金额": totals.get("净资产", 0), "层级": 1})

    max_len = max(len(left_rows), len(right_rows))
    while len(left_rows) < max_len:
        left_rows.append({"名称": "", "金额": None, "层级": 0})
    while len(right_rows) < max_len:
        right_rows.append({"名称": "", "金额": None, "层级": 0})

    merged = []
    for l, r in zip(left_rows, right_rows):
        merged.append({
            "资产": l["名称"],
            "资产金额": l["金额"],
            "负债及净资产": r["名称"],
            "负债及净资产金额": r["金额"],
            "左层级": l["层级"],
            "右层级": r["层级"],
        })
    return pd.DataFrame(merged)


def style_standard_cashflow_df(df: pd.DataFrame):
    def row_style(row):
        level = df.loc[row.name, "层级"]
        if level == 1:
            return ['font-weight:700;background-color:#eef2ff;'] * len(row)
        if level == 2:
            return ['font-weight:600;background-color:#f8fafc;'] * len(row)
        return [''] * len(row)
    show_df = df[["AFP家财分类", "一级分类", "二级分类", "金额"]].copy()
    return show_df.style.apply(row_style, axis=1).format({"金额": lambda v: fmt_money(v) if pd.notna(v) else '--'})


def style_standard_balance_df(df: pd.DataFrame):
    def row_style(row):
        left_lv = df.loc[row.name, "左层级"]
        right_lv = df.loc[row.name, "右层级"]
        if left_lv == 1 or right_lv == 1:
            return ['font-weight:700;background-color:#eef2ff;'] * len(row)
        return [''] * len(row)
    show_df = df[["资产", "资产金额", "负债及净资产", "负债及净资产金额"]].copy()
    return show_df.style.apply(row_style, axis=1).format({
        "资产金额": lambda v: fmt_money(v) if pd.notna(v) else '--',
        "负债及净资产金额": lambda v: fmt_money(v) if pd.notna(v) else '--',
    })


def render_standard_statements(record: dict):
    st.subheader("标准展示区：统一现金流量表与资产负债表")
    st.caption("这一块单独用于标准展示，和左侧录入区、右侧修正区分开，更适合直接查看完整表。")
    tab_cf, tab_bs = st.tabs(["标准现金流量表", "标准资产负债表"])
    with tab_cf:
        cf_df = build_cashflow_statement_rows(record)
        st.dataframe(style_standard_cashflow_df(cf_df), use_container_width=True, hide_index=True)
    with tab_bs:
        bs_df = build_balance_statement_rows(record)
        st.dataframe(style_standard_balance_df(bs_df), use_container_width=True, hide_index=True)
        totals = compute_balance_totals(record["balance_sheet"])
        c1, c2, c3 = st.columns(3)
        c1.metric("总资产", fmt_money(totals["总资产"]))
        c2.metric("总负债", fmt_money(totals["总负债"]))
        c3.metric("净资产", fmt_money(totals["净资产"]))

def render_month_overview(user: dict, year: int):
    st.subheader(f"{year}岁全年月度概览")
    rows = []
    for m in MONTHS:
        rec = get_record(user, year, m)
        rows.append({
            "月份": f"{m}月",
            "工作收入": rec["derived"].get("工作收入", 0.0),
            "生活支出": rec["derived"].get("生活支出", 0.0),
            "工作储蓄": rec["derived"].get("工作储蓄", 0.0),
            "理财收入": rec["derived"].get("理财收入", 0.0),
            "理财支出": rec["derived"].get("理财支出", 0.0),
            "理财储蓄": rec["derived"].get("理财储蓄", 0.0),
            "储蓄": rec["derived"].get("储蓄", 0.0),
            "净资产": rec["derived"].get("净资产", 0.0),
            "说明": rec.get("decision_summary", "") or rec.get("note", ""),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.plotly_chart(make_line_chart(df, "月份", ["工作收入", "生活支出", "储蓄", "净资产"], f"{year}岁月度趋势"), use_container_width=True)


def render_month_compare(user: dict, year: int, month: int):
    prev_y, prev_m = prev_year_month(year, month)
    if prev_y is None:
        st.info("这是第一期，没有上期可比。")
        return
    cur_key = month_key(year, month)
    prev_key = month_key(prev_y, prev_m)
    st.subheader(f"{cur_key} vs {prev_key} 差额对比")
    st.markdown(f'<div class="compare-box">红色表示较上期增加，蓝色表示较上期减少。</div>', unsafe_allow_html=True)
    lvl3_df = compare_months_by_item(user, prev_key, cur_key)
    lvl2_df = compare_lvl2_totals(user, prev_key, cur_key)
    st.markdown("**三级结构差额**")
    st.dataframe(style_diff_df(lvl3_df), use_container_width=True, hide_index=True)
    st.markdown("**一级/二级结构差额**")
    st.dataframe(style_diff_df(lvl2_df), use_container_width=True, hide_index=True)

    chart_df = lvl3_df[["三级分类", "差额"]].copy()
    chart_df = chart_df[chart_df["差额"] != 0]
    if not chart_df.empty:
        st.plotly_chart(make_line_chart(chart_df.rename(columns={"三级分类": "项目"}), "项目", ["差额"], f"{prev_key} 至 {cur_key} 三级项目差额"), use_container_width=True)


def render_six_month_compare(user: dict, year: int, month: int):
    st.subheader("近6个月区间对比")
    keys = months_in_last_six(year, month)
    item_options = all_cashflow_leaf_items() + ["工作收入", "生活支出", "工作储蓄", "理财收入", "理财支出", "理财储蓄", "储蓄", "净资产", "总资产", "总负债"]
    selected_item = st.selectbox("选择单项进行区间对比", options=item_options, index=item_options.index("子女抚养") if "子女抚养" in item_options else 0)
    trend_df = slice_item_trend(user, selected_item, keys)
    st.dataframe(trend_df, use_container_width=True, hide_index=True)
    st.plotly_chart(make_line_chart(trend_df, "期间", [selected_item], f"{selected_item} 近6个月趋势"), use_container_width=True)


def render_yearly_and_five_year_compare(user: dict):
    st.subheader("年度汇总")
    ydf = cached_yearly_summary_dataframe(get_user_snapshot(user))
    st.dataframe(ydf, use_container_width=True, hide_index=True)
    st.plotly_chart(make_line_chart(ydf, "年份", ["工作收入", "生活支出", "储蓄", "净资产"], "年度趋势"), use_container_width=True)

    st.subheader("5年区间对比")
    fdf = cached_five_year_df(get_user_snapshot(user))
    st.dataframe(fdf, use_container_width=True, hide_index=True)
    st.plotly_chart(make_line_chart(fdf, "五年区间", ["工作收入", "生活支出", "储蓄", "净资产"], "五年区间趋势"), use_container_width=True)


def render_savings_chart(user: dict):
    st.subheader("第三张图：储蓄联动图")
    df = cached_year_month_dataframe(get_user_snapshot(user))
    range_options = {
        "近3月": 3,
        "近1年": 12,
        "近3年": 36,
        "成立以来": len(df),
        "近年来": 60,
    }
    selected_range = st.selectbox("时间范围", options=list(range_options.keys()))
    n = range_options[selected_range]
    view_df = df.tail(n).copy()
    st.plotly_chart(
        make_line_chart(view_df, "期间", ["工作储蓄", "理财储蓄", "储蓄"], f"{selected_range} 储蓄联动图"),
        use_container_width=True
    )
    st.dataframe(view_df[["期间", "工作储蓄", "理财储蓄", "储蓄"]], use_container_width=True, hide_index=True)


def render_ai_generator(user: dict):
    st.subheader("AI数据生成与导入")
    template = """示例模板：
30岁，在二线城市工作生活，已婚，拥有一套自住房产，目前承担相应的住房贷款。月收入来源稳定，属于工薪阶层。日常消费以生活必需品、家庭开销为主，风格务实，属于日常型消费，没有进行过高风险或特别激进的投资活动。当前财务关注点在于夯实基础，管理好每月收支。"""
    st.code(template, language="text")

    bg = st.text_area("单独输入人物背景", value=user["profile"].get("background", ""), height=140)
    goals = st.multiselect(
        "理财目标偏好",
        options=["先建立3-6个月应急资金", "优先稳定月度现金流", "逐步压降负债规模", "控制消费贷比例", "适度配置稳健理财"],
        default=["先建立3-6个月应急资金", "优先稳定月度现金流", "逐步压降负债规模"],
    )

    cf_col, bs_col = st.columns(2)
    with cf_col:
        selected_cashflow_items = st.multiselect(
            "现金流量表生成小类",
            options=CASHFLOW_OPTIONAL_ITEMS,
            default=["工资薪金", "饮食开支", "居住成本", "利息收入", "保费支出"],
            key="ai_cashflow_items"
        )
    with bs_col:
        selected_balance_items = st.multiselect(
            "资产负债表生成项目",
            options=BALANCE_OPTIONAL_ITEMS,
            default=["现金", "货币基金/活期", "定期存款", "股票投资", "基金投资", "自用房产", "信用卡负债", "自住房按揭贷款"],
            key="ai_balance_items"
        )

    mode = st.radio(
        "生成模式",
        options=["随机测试数据（本地快速）", "DeepSeek完整生成（较慢）"],
        horizontal=True,
        help="先用随机测试数据验证图表和联动，确认没问题后再切到 DeepSeek。"
    )

    logic_preview = (
        "系统会先生成 profile，然后从25岁1月到35岁12月逐月生成；\n"
        "每个月都会包含 note 和 decision_summary；\n"
        "现金流小类和资产负债表项目分开勾选，便于分别控制生成范围；\n"
        "随机测试模式会在合理区间内生成固定风格数据，速度更快，适合先测绘图；\n"
        "导入后会自动重算工作储蓄、理财储蓄、储蓄，以及资产负债表联动结果。"
    )
    st.text_area("生成逻辑预览", value=logic_preview, height=140)

    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    if "finance_ai_progress" not in st.session_state:
        st.session_state["finance_ai_progress"] = 0.0
    progress_placeholder.progress(st.session_state["finance_ai_progress"], text="等待开始生成")

    def progress_callback(current_step: int, total_steps: int, label: str):
        value = min(max(current_step / total_steps, 0.0), 1.0)
        st.session_state["finance_ai_progress"] = value
        progress_placeholder.progress(value, text=f"{label}（{current_step}/{total_steps}）")
        status_placeholder.info(f"当前进度：{current_step}/{total_steps}，约 {value * 100:.1f}%")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("开始生成 25-35 岁完整数据"):
            try:
                st.session_state["finance_ai_progress"] = 0.0
                progress_placeholder.progress(0.0, text="开始生成...")
                status_placeholder.info("正在准备生成任务...")
                if mode == "随机测试数据（本地快速）":
                    obj = generate_mock_timeline(bg, goals, selected_cashflow_items, selected_balance_items, progress_callback=progress_callback)
                    st.session_state["finance_ai_json"] = json.dumps(obj, ensure_ascii=False, indent=2)
                    progress_placeholder.progress(1.0, text="随机测试数据已生成完成")
                    status_placeholder.success("随机测试数据已生成完成，可以直接测试图表和导入。")
                else:
                    progress_callback(1, 132, "正在请求 DeepSeek，长周期生成可能较慢")
                    prompt = build_ai_prompt(bg, goals, selected_cashflow_items, selected_balance_items)
                    raw = call_deepseek(prompt, system_prompt="你是家庭财务长期轨迹建模助手，只输出 JSON。")
                    st.session_state["finance_ai_json"] = raw
                    progress_placeholder.progress(1.0, text="DeepSeek 返回完成")
                    status_placeholder.success("DeepSeek 已生成完成，可在下方检查并导入。")
            except Exception as e:
                status_placeholder.error(f"生成失败：{e}")
    with c2:
        if st.button("导入下方 JSON 到当前用户"):
            try:
                content = st.session_state.get("finance_ai_json", "") or st.session_state.get("finance_ai_manual_json", "")
                obj = tolerant_json_loads(content)
                import_ai_timeline_into_user(user, obj)
                if bg.strip():
                    user["profile"]["background"] = bg.strip()
                if goals:
                    user["profile"]["basic_goal"] = "；".join(goals)
                save_db(st.session_state["db"])
                st.success("导入成功。")
            except Exception as e:
                st.error(f"导入失败：{e}")

    show_ai_json = st.checkbox("显示并编辑 AI 原始 JSON", key="show_ai_json_editor")
    if show_ai_json:
        st.text_area("AI返回的 JSON（可修改后再导入）", value=st.session_state.get("finance_ai_json", ""), key="finance_ai_manual_json", height=380)
    else:
        st.caption("默认不展开大体积 JSON，减少页面打开和重跑时的内存占用；需要时再勾选显示。")
def local_finance_advice(user: dict, current_age: int, income_guess: float) -> str:
    profile = user.get("profile", {})
    end_age = get_user_end_age(user)
    guidance = []
    if current_age < 35:
        guidance.append("当前阶段更适合把现金流打稳，把应急金和基础保障先补足。")
    elif current_age < 50:
        guidance.append("这一阶段通常是家庭责任和收入能力都较高的时期，核心是平衡教育、健康和资产配置。")
    elif current_age < 60:
        guidance.append("距离退休更近，建议逐步降低高波动配置，把未来3到5年的支出安全垫提前准备好。")
    else:
        guidance.append("退休后要把重心放在持续现金流、医疗准备和资产波动控制上。")

    if income_guess < 10000:
        guidance.append("当前月收入不算高，建议优先看现金结余率和必要支出占比，避免过早承担太多长期负债。")
    elif income_guess < 25000:
        guidance.append("当前收入有一定缓冲空间，可以把储蓄分为日常周转、应急金和中长期理财三层。")
    else:
        guidance.append("当前收入较强，重点不是单纯多投资，而是把大额资产配置、保险保障和家庭目标统一起来。")

    derived_df = cached_year_month_dataframe(get_user_snapshot(user))
    recent = derived_df[derived_df["年份"] == current_age]
    if not recent.empty:
        avg_save = float(recent["储蓄"].mean())
        if avg_save < 0:
            guidance.append("最近这一年龄段平均储蓄为负，说明现金流承压，建议先处理支出结构和高成本债务。")
        else:
            guidance.append("最近这一年龄段仍有正向储蓄，说明基础较稳，可以逐步提高长期资金的占比。")
    if end_age >= 60:
        guidance.append("系统已经支持看到80岁，建议重点观察60岁前后的收入切换和医疗支出变化。")
    return "\n\n".join(guidance)


def _safe_positive(value: float, floor: float = 0.0) -> float:
    return max(floor, round(float(value), 2))


def _series_trend(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return float(values[0]), 0.0
    x = list(range(len(values)))
    x_mean = sum(x) / len(x)
    y_mean = sum(values) / len(values)
    denom = sum((i - x_mean) ** 2 for i in x) or 1.0
    slope = sum((i - x_mean) * (y - y_mean) for i, y in zip(x, values)) / denom
    return float(values[-1]), float(slope)


def _build_prediction_notes(age: int, month: int, event_flags: dict, event_texts: List[str], cash_balance: float) -> Tuple[str, str]:
    season_text = ""
    if month in {1, 2}:
        season_text = "节假日消费偏高，家庭现金支出阶段性上升。"
    elif month in {7, 8}:
        season_text = "暑期支出和家庭活动增多，月内消费略有放大。"
    elif month == 12:
        season_text = "年末复盘和集中配置较多，资金调度更频繁。"
    else:
        season_text = "本月整体延续既有收支节奏，波动保持在正常区间。"

    decision_parts = [season_text] + event_texts
    decision = "；".join([p for p in decision_parts if p])
    note = f"预测阶段下，本月月末现金约为{cash_balance:,.0f}元。" + ("；" + "；".join(event_texts) if event_texts else "")
    return decision, note


def generate_prediction_timeline(user: dict, event_flags: dict, progress_callback=None):
    import random
    ensure_user_timeline(user)
    rng = random.Random(20260422)
    history_df = cached_year_month_dataframe(get_user_snapshot(user))
    base_df = history_df[history_df["年份"] <= 35].copy()
    if base_df.empty:
        raise ValueError("请先生成或录入25-35岁的基础数据，再进行35-80岁预测。")

    start_age = 36
    end_age = 80
    leaf_items = flatten_cashflow_items()
    annual_cf = {}
    for _, _, item in leaf_items:
        col = None
        for c in base_df.columns:
            if c.endswith('/' + item):
                col = c
                break
        annual = []
        if col:
            annual_series = base_df.groupby("年份")[col].mean()
            annual = [float(annual_series.get(y, 0.0)) for y in sorted(annual_series.index.tolist())]
        annual_cf[item] = _series_trend(annual)

    annual_bs = {}
    for item in list(BALANCE_ASSETS["流动性资产"]) + list(BALANCE_ASSETS["投资性资产"]) + list(BALANCE_ASSETS["自用性资产"]) + list(BALANCE_ASSETS["其他资产"]) + list(BALANCE_LIABILITIES["流动性负债"]) + list(BALANCE_LIABILITIES["投资性负债"]) + list(BALANCE_LIABILITIES["自用性负债"]) + list(BALANCE_LIABILITIES["其他负债"]):
        hist = []
        for y in range(25, 36):
            vals = []
            for m in MONTHS:
                rec = get_record(user, y, m)
                if item in rec["balance_sheet"]["assets"]:
                    vals.append(float(rec["balance_sheet"]["assets"].get(item, 0) or 0))
                elif item in rec["balance_sheet"]["liabilities"]:
                    vals.append(float(rec["balance_sheet"]["liabilities"].get(item, 0) or 0))
            hist.append(sum(vals) / len(vals) if vals else 0.0)
        annual_bs[item] = _series_trend(hist)

    prev_rec = get_record(user, 35, 12)
    prev_cash = float(prev_rec["balance_sheet"]["assets"].get("现金", 0) or 0)
    prev_fund = float(prev_rec["balance_sheet"]["assets"].get("货币基金/活期", 0) or 0)
    prev_deposit = float(prev_rec["balance_sheet"]["assets"].get("定期存款", 0) or 0)
    prev_stock = float(prev_rec["balance_sheet"]["assets"].get("股票投资", 0) or 0)
    prev_bond = float(prev_rec["balance_sheet"]["assets"].get("债券投资", 0) or 0)
    prev_fund_inv = float(prev_rec["balance_sheet"]["assets"].get("基金投资", 0) or 0)
    prev_house = float(prev_rec["balance_sheet"]["assets"].get("自用房产", 0) or 0)
    prev_cc = float(prev_rec["balance_sheet"]["liabilities"].get("信用卡负债", 0) or 0)
    prev_small = float(prev_rec["balance_sheet"]["liabilities"].get("小额消费信贷", 0) or 0)
    prev_mortgage = float(prev_rec["balance_sheet"]["liabilities"].get("自住房按揭贷款", 0) or 0)

    total_steps = (end_age - start_age + 1) * 12
    step = 0
    severity_map = {"轻度": 0.08, "中等": 0.16, "较重": 0.28}
    illness_hit = severity_map.get(event_flags.get("illness_severity", "中等"), 0.16)

    for age in range(start_age, end_age + 1):
        for month in MONTHS:
            step += 1
            rec = get_record(user, age, month)
            cf = empty_cashflow()
            bs = empty_balance_sheet()
            event_texts = []

            salary_base, salary_slope = annual_cf.get("工资薪金", (12000.0, 200.0))
            child_cost_base, _ = annual_cf.get("子女抚养", (800.0, 50.0))
            medical_base, _ = annual_cf.get("医疗支出", (300.0, 20.0))
            food_base, _ = annual_cf.get("饮食开支", (2500.0, 30.0))
            house_cost_base, _ = annual_cf.get("居住成本", (3200.0, 40.0))
            transport_base, _ = annual_cf.get("通勤交通", (700.0, 10.0))
            interest_base, _ = annual_cf.get("利息收入", (120.0, 8.0))
            insurance_base, _ = annual_cf.get("保费支出", (400.0, 10.0))

            year_offset = age - 35
            salary = max(0.0, salary_base + salary_slope * year_offset + rng.randint(-600, 600))
            if event_flags.get("retirement") and age >= 60:
                pension = max(2500.0, salary * rng.uniform(0.45, 0.6))
                cf["工作收入"]["主动收入"]["养老金"] = pension
                salary *= rng.uniform(0.05, 0.2)
                event_texts.append("进入退休阶段，工资性收入明显下降，养老金成为主要来源之一")
            if salary > 0:
                cf["工作收入"]["主动收入"]["工资薪金"] = salary

            if rng.random() < 0.12:
                cf["工作收入"]["主动收入"]["劳务报酬"] = max(0.0, salary * rng.uniform(0.03, 0.12))

            food = max(1500.0, food_base * (1 + 0.012 * year_offset) + rng.randint(-300, 300))
            housing = max(1500.0, house_cost_base * (1 + 0.008 * year_offset) + rng.randint(-300, 300))
            transport = max(100.0, transport_base * (1 - 0.01 * max(age - 55, 0)) + rng.randint(-100, 120))
            medical = max(0.0, medical_base * (1 + 0.025 * year_offset) + rng.randint(-100, 260))
            child_cost = max(0.0, child_cost_base)

            if event_flags.get("child_junior_high") and 40 <= age <= 42:
                child_cost += rng.randint(1000, 2200)
                cf["生活支出"]["个人成长"]["学习提升"] = float(rng.randint(300, 1000))
                event_texts.append("孩子进入初中阶段，教育和培训相关开支明显提高")
            elif age < 45 and child_cost > 0:
                child_cost = child_cost * max(0.3, 1 - 0.08 * (age - 36))
            else:
                child_cost = 0.0

            if event_flags.get("illness") and event_flags.get("illness_start_age", 48) <= age <= event_flags.get("illness_end_age", 52):
                medical += max(800.0, medical * illness_hit + rng.randint(600, 2800))
                if cf["工作收入"]["主动收入"]["工资薪金"] > 0:
                    cf["工作收入"]["主动收入"]["工资薪金"] *= (1 - illness_hit * 0.45)
                event_texts.append("健康事件带来了额外医疗支出，并对阶段性收入造成了一定影响")

            cf["生活支出"]["生活消费"]["饮食开支"] = food
            cf["生活支出"]["生活消费"]["居住成本"] = housing
            cf["生活支出"]["生活消费"]["通勤交通"] = transport
            cf["生活支出"]["生活消费"]["医疗支出"] = medical
            if child_cost > 0:
                cf["生活支出"]["家庭成员"]["子女抚养"] = child_cost
            if rng.random() < 0.25:
                cf["生活支出"]["家庭成员"]["赡养老人"] = float(rng.randint(500, 1800))
            if month in {1, 2, 12}:
                cf["生活支出"]["人际关系"]["人情往来"] = float(rng.randint(300, 1500))
            if month in {7, 8} and rng.random() < 0.25:
                cf["生活支出"]["娱乐消费"]["旅行度假"] = float(rng.randint(1500, 7000))

            invest_base = max(0.0, (prev_fund + prev_deposit) * rng.uniform(0.002, 0.005))
            cf["理财收入"]["被动收入"]["利息收入"] = max(0.0, interest_base + invest_base)
            if prev_house > 0 and rng.random() < 0.08:
                cf["理财收入"]["被动收入"]["租金收入"] = float(rng.randint(1200, 4500))
            cf["理财支出"]["财务支出"]["保费支出"] = max(200.0, insurance_base * (1 + 0.012 * year_offset))
            if prev_cc > 0 or prev_small > 0:
                cf["理财支出"]["财务支出"]["利息费用"] = float((prev_cc + prev_small) * rng.uniform(0.008, 0.02))

            work_income = sum(sum(v.values()) for v in cf["工作收入"].values())
            living_expense = sum(sum(v.values()) for v in cf["生活支出"].values())
            invest_income = sum(sum(v.values()) for v in cf["理财收入"].values())
            invest_expense = sum(sum(v.values()) for v in cf["理财支出"].values())
            net_saving = work_income + invest_income - living_expense - invest_expense

            mortgage_pay = 0.0
            if prev_mortgage > 0 and not (event_flags.get("retirement") and age >= 65):
                mortgage_pay = min(prev_mortgage, max(1000.0, prev_mortgage * 0.004))
                prev_mortgage = max(0.0, prev_mortgage - mortgage_pay)
            prev_cc = max(0.0, prev_cc + rng.uniform(-800, 1200))
            prev_small = max(0.0, prev_small + rng.uniform(-600, 800))
            prev_cash = max(8000.0, prev_cash + net_saving - mortgage_pay)

            if prev_cash > 50000:
                move_fund = prev_cash * rng.uniform(0.03, 0.08)
                prev_cash -= move_fund
                prev_fund += move_fund
            if month in {6, 12} and prev_cash > 60000:
                move_deposit = prev_cash * rng.uniform(0.04, 0.09)
                prev_cash -= move_deposit
                prev_deposit += move_deposit
            if age < 60 and prev_cash > 70000:
                prev_stock = max(0.0, prev_stock * rng.uniform(0.97, 1.08) + prev_cash * rng.uniform(0.0, 0.04))
                prev_bond = max(0.0, prev_bond * rng.uniform(0.995, 1.03))
                prev_fund_inv = max(0.0, prev_fund_inv * rng.uniform(0.98, 1.05) + prev_cash * rng.uniform(0.0, 0.03))
            else:
                prev_stock = max(0.0, prev_stock * rng.uniform(0.96, 1.04))
                prev_bond = max(0.0, prev_bond * rng.uniform(0.998, 1.025))
                prev_fund_inv = max(0.0, prev_fund_inv * rng.uniform(0.99, 1.03))

            prev_house = max(0.0, prev_house * (1 + (0.002 if age < 55 else 0.001)))

            bs["assets"]["现金"] = _safe_positive(prev_cash, 3000.0)
            bs["assets"]["货币基金/活期"] = _safe_positive(prev_fund)
            bs["assets"]["定期存款"] = _safe_positive(prev_deposit)
            bs["assets"]["股票投资"] = _safe_positive(prev_stock)
            bs["assets"]["债券投资"] = _safe_positive(prev_bond)
            bs["assets"]["基金投资"] = _safe_positive(prev_fund_inv)
            bs["assets"]["自用房产"] = _safe_positive(prev_house)
            bs["liabilities"]["信用卡负债"] = _safe_positive(prev_cc)
            bs["liabilities"]["小额消费信贷"] = _safe_positive(prev_small)
            bs["liabilities"]["自住房按揭贷款"] = _safe_positive(prev_mortgage)

            decision, note = _build_prediction_notes(age, month, event_flags, event_texts, prev_cash)
            rec["decision_summary"] = decision
            rec["note"] = note
            rec["cashflow"] = cf
            rec["balance_sheet"] = bs
            recompute_record(rec, get_record(user, age, month - 1) if month > 1 else (get_record(user, age - 1, 12) if age > 25 else None))

            if progress_callback:
                progress_callback(step, total_steps, f"正在预测 {age}岁{month}月")

    user["profile"]["prediction_enabled"] = True
    user["profile"]["end_age"] = 80
    user["profile"]["event_settings"] = event_flags
    return user


def render_prediction_tab(user: dict, db: dict):
    st.subheader("生命周期预测：35岁以后到80岁")
    st.caption("这一页会在25-35岁基础数据之上，继续生成36-80岁的月度预测，并把事件影响写回每个月说明。")

    st.markdown("**预测开关**")
    enable_prediction = st.checkbox("启用35-80岁预测视图", value=bool(user.get("profile", {}).get("prediction_enabled", False)))
    current_settings = user.get("profile", {}).get("event_settings", {})

    c1, c2, c3 = st.columns(3)
    child_junior_high = c1.checkbox("40岁孩子上初中事件", value=bool(current_settings.get("child_junior_high", False)))
    retirement = c2.checkbox("60岁退休事件", value=bool(current_settings.get("retirement", False)))
    illness = c3.checkbox("40-60岁中间可能生病", value=bool(current_settings.get("illness", False)))

    c4, c5 = st.columns(2)
    illness_start_age = c4.slider("疾病影响起始年龄", min_value=40, max_value=60, value=int(current_settings.get("illness_start_age", 48)))
    illness_end_age = c5.slider("疾病影响结束年龄", min_value=40, max_value=60, value=int(current_settings.get("illness_end_age", 52)))
    illness_severity = st.select_slider("疾病影响强度", options=["轻度", "中等", "较重"], value=current_settings.get("illness_severity", "中等"))

    event_flags = {
        "child_junior_high": child_junior_high,
        "retirement": retirement,
        "illness": illness,
        "illness_start_age": min(illness_start_age, illness_end_age),
        "illness_end_age": max(illness_start_age, illness_end_age),
        "illness_severity": illness_severity,
    }

    st.markdown("**影响说明预览**")
    preview = []
    if child_junior_high:
        preview.append("40-42岁会提高子女抚养与学习提升相关支出。")
    if retirement:
        preview.append("60岁以后工资性收入明显下降，同时引入养老金收入。")
    if illness:
        preview.append(f"{event_flags['illness_start_age']}-{event_flags['illness_end_age']}岁会提高医疗支出，并对阶段性收入形成影响。")
    if not preview:
        preview.append("如果不勾选事件，将按较平稳的自然生命周期轨迹预测。")
    st.info("\n".join(preview))

    progress_placeholder = st.empty()
    status_placeholder = st.empty()
    progress_placeholder.progress(0.0, text="等待开始预测")

    def progress_callback(current_step: int, total_steps: int, label: str):
        value = min(max(current_step / total_steps, 0.0), 1.0)
        progress_placeholder.progress(value, text=f"{label}（{current_step}/{total_steps}）")
        status_placeholder.info(f"当前进度：{current_step}/{total_steps}，约 {value * 100:.1f}%")

    c1, c2 = st.columns(2)
    with c1:
        if st.button("开始生成 36-80 岁预测数据"):
            try:
                if not enable_prediction:
                    user["profile"]["prediction_enabled"] = False
                    user["profile"]["end_age"] = 35
                    save_db(db)
                    st.warning("已关闭预测视图，目前仅保留25-35岁范围。")
                else:
                    generate_prediction_timeline(user, event_flags, progress_callback=progress_callback)
                    save_db(db)
                    progress_placeholder.progress(1.0, text="预测完成")
                    status_placeholder.success("36-80岁预测已完成，现在月度、年度和对比页面都可以访问到80岁。")
            except Exception as e:
                status_placeholder.error(f"预测失败：{e}")
    with c2:
        if st.button("仅保存预测开关与事件设置"):
            user["profile"]["prediction_enabled"] = enable_prediction
            user["profile"]["end_age"] = 80 if enable_prediction else 35
            user["profile"]["event_settings"] = event_flags
            save_db(db)
            st.success("预测设置已保存。")

    st.divider()
    st.markdown("**AI/规则建议**")
    year_options = get_user_years(user)
    advice_age = st.selectbox("选择建议年龄段", options=year_options, index=min(len(year_options)-1, max(0, user.get('profile', {}).get('age', 25) - year_options[0])))
    age_df = cached_year_month_dataframe(get_user_snapshot(user))
    advice_income = 0.0
    if not age_df.empty:
        tmp = age_df[age_df["年份"] == advice_age]
        if not tmp.empty:
            advice_income = float(tmp["工作收入"].mean())
    st.markdown(local_finance_advice(user, advice_age, advice_income))
def render_transfer_tab(db: dict, current_user: dict):
    st.subheader("不同用户间转账")
    users = db.get("users", [])
    target_candidates = [u for u in users if u["id"] != current_user["id"]]
    if not target_candidates:
        st.info("请至少创建两个用户后再使用转账功能。")
        return
    target_user_name_to_id = {u["name"]: u["id"] for u in target_candidates}
    c1, c2, c3 = st.columns(3)
    year_options = get_user_years(current_user)
    year = c1.selectbox("转账年份", year_options, index=0, key="transfer_year")
    month = c2.selectbox("转账月份", MONTHS, index=0, key="transfer_month")
    target_name = c3.selectbox("收款用户", list(target_user_name_to_id.keys()))
    amount = st.number_input("转账金额", min_value=0.0, value=1000.0, step=100.0)
    from_account = st.selectbox("转出账户", current_user.get("accounts", ["默认账户"]))
    target_user = next(u for u in target_candidates if u["id"] == target_user_name_to_id[target_name])
    to_account = st.selectbox("转入账户", target_user.get("accounts", ["默认账户"]))
    note = st.text_input("转账备注", value="家庭内部调拨")

    if st.button("执行转账"):
        try:
            apply_transfer(db, current_user, target_user, year, month, amount, note, from_account, to_account)
            st.success("转账已记录并同步更新两位用户的资产负债表。")
        except Exception as e:
            st.error(f"转账失败：{e}")

    transfers = pd.DataFrame(db.get("transfers", []))
    if not transfers.empty:
        st.markdown("**转账记录**")
        st.dataframe(transfers, use_container_width=True, hide_index=True)


def render_profile_editor(user: dict):
    st.subheader("用户背景")
    profile = user["profile"]
    c1, c2 = st.columns(2)
    profile["name"] = c1.text_input("姓名/代号", value=profile.get("name", ""))
    profile["age"] = c2.number_input("当前年龄", min_value=25, max_value=80, value=int(profile.get("age", 25) or 25), step=1)
    c3, c4 = st.columns(2)
    profile["job"] = c3.text_input("职业", value=profile.get("job", ""))
    profile["city_level"] = c4.text_input("城市级别", value=profile.get("city_level", ""))
    c5, c6 = st.columns(2)
    profile["marital_status"] = c5.text_input("婚姻状态", value=profile.get("marital_status", ""))
    profile["basic_goal"] = c6.text_input("基础理财目标", value=profile.get("basic_goal", ""))
    profile["background"] = st.text_area("背景描述", value=profile.get("background", ""), height=120)
    account_text = st.text_input("账户列表（用中文逗号分隔）", value="，".join(user.get("accounts", ["默认账户"])))
    user["accounts"] = [x.strip() for x in account_text.replace(",", "，").split("，") if x.strip()] or ["默认账户"]


# =========================
# 主页面
# =========================
st.set_page_config(page_title="家庭财务预测系统 V2", layout="wide")
inject_styles()
st.title("家庭财务预测系统 V2")
st.caption("支持25-35岁基础记录、36-80岁生命周期预测、事件影响注入、AI生成、月度/年度访问、对比分析、储蓄联动与用户转账。")

if "db" not in st.session_state:
    st.session_state["db"] = load_db()
db = st.session_state["db"]

with st.sidebar:
    st.header("用户管理")
    all_users = db.get("users", [])
    user_options = {u["id"]: u["name"] for u in all_users}
    selected_user_id = None
    if user_options:
        selected_user_id = st.selectbox("选择用户", options=list(user_options.keys()), format_func=lambda x: user_options[x], key="sidebar_selected_user")
    new_user_name = st.text_input("新增用户名称")
    if st.button("创建新用户"):
        if new_user_name.strip():
            uid = add_user(db, new_user_name.strip())
            st.session_state["db"] = db
            st.success(f"已创建用户：{new_user_name}")
            st.rerun()
        else:
            st.warning("请先输入名称。")

    st.divider()
    st.subheader("功能分类")
    function_category = st.radio("切换工作台", ["基础账本", "生命周期预测"], horizontal=False, key="function_category")

if not db.get("users"):
    st.info("请先创建一个用户。")
    st.stop()

if not selected_user_id:
    selected_user_id = db["users"][0]["id"]
current_user = user_map(db)[selected_user_id]
ensure_user_timeline(current_user)

if function_category == "基础账本":
    main_tabs = st.tabs(["月度访问", "年度与五年访问", "对比分析", "储蓄联动图", "人物背景", "AI生成/导入", "用户转账", "导出"])

    with main_tabs[0]:
        year_options = get_user_years(current_user)
        c1, c2 = st.columns([1, 1])
        year = c1.selectbox("选择年龄/年份", year_options, index=0)
        month = c2.selectbox("选择月份", MONTHS, index=0)
        current_record = get_record(current_user, year, month)

        show_balance_summary(current_record)
        st.markdown(f"**决策说明：** {current_record.get('decision_summary', '') or '暂无'}")
        st.markdown(f"**波动说明：** {current_record.get('note', '') or '暂无'}")
        render_standard_statements(current_record)

        left, right = st.columns([1.15, 1])
        with left:
            render_cashflow_editor(current_record)
        with right:
            render_balance_editor(current_record)

        if st.button("保存当前月份并向后联动重算"):
            propagate_user_from(current_user, year, month)
            save_db(db)
            st.success("已保存，并完成后续月份联动重算。")

        st.divider()
        render_month_overview(current_user, year)

    with main_tabs[1]:
        render_yearly_and_five_year_compare(current_user)

    with main_tabs[2]:
        year_options = get_user_years(current_user)
        c1, c2 = st.columns(2)
        compare_year = c1.selectbox("对比查看年份", year_options, index=0, key="compare_year")
        compare_month = c2.selectbox("对比查看月份", MONTHS, index=0, key="compare_month")
        render_month_compare(current_user, compare_year, compare_month)
        st.divider()
        render_six_month_compare(current_user, compare_year, compare_month)

    with main_tabs[3]:
        render_savings_chart(current_user)

    with main_tabs[4]:
        render_profile_editor(current_user)
        if st.button("保存人物背景与账户"):
            save_db(db)
            st.success("已保存。")

    with main_tabs[5]:
        render_ai_generator(current_user)

    with main_tabs[6]:
        render_transfer_tab(db, current_user)

    with main_tabs[7]:
        st.subheader("数据导出")
        export_json = json.dumps(current_user, ensure_ascii=False, indent=2)
        st.download_button("下载当前用户 JSON", export_json, file_name=f"{current_user['name']}_finance_timeline.json")

        excel_state_key = f"excel_bytes_{current_user['id']}_basic"
        if st.button("生成当前用户 Excel", key=f"build_excel_basic_{current_user['id']}"):
            with st.spinner("正在生成 Excel，请稍候..."):
                st.session_state[excel_state_key] = build_user_excel_bytes(get_user_snapshot(current_user))
            st.success("Excel 已生成，可直接下载。")

        if excel_state_key in st.session_state:
            st.download_button(
                "下载当前用户 Excel",
                data=st.session_state[excel_state_key],
                file_name=f"{current_user['name']}_finance_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_excel_basic_{current_user['id']}"
            )

        all_json = json.dumps(db, ensure_ascii=False, indent=2)
        st.download_button("下载全部数据库 JSON", all_json, file_name="finance_db_all_users.json")
else:
    pred_tabs = st.tabs(["生命周期预测", "月度访问", "年度与五年访问", "对比分析", "储蓄联动图", "人物背景", "AI生成/导入", "用户转账", "导出"])

    with pred_tabs[0]:
        render_prediction_tab(current_user, db)

    with pred_tabs[1]:
        year_options = get_user_years(current_user)
        default_year = 35 if 35 in year_options else year_options[0]
        c1, c2 = st.columns([1, 1])
        year = c1.selectbox("选择年龄/年份", year_options, index=year_options.index(default_year), key="pred_year_select")
        month = c2.selectbox("选择月份", MONTHS, index=0, key="pred_month_select")
        current_record = get_record(current_user, year, month)
        show_balance_summary(current_record)
        st.markdown(f"**决策说明：** {current_record.get('decision_summary', '') or '暂无'}")
        st.markdown(f"**波动说明：** {current_record.get('note', '') or '暂无'}")
        render_standard_statements(current_record)
        render_month_overview(current_user, year)

    with pred_tabs[2]:
        render_yearly_and_five_year_compare(current_user)

    with pred_tabs[3]:
        year_options = get_user_years(current_user)
        default_year = 40 if 40 in year_options else year_options[0]
        c1, c2 = st.columns(2)
        compare_year = c1.selectbox("对比查看年份", year_options, index=year_options.index(default_year), key="pred_compare_year")
        compare_month = c2.selectbox("对比查看月份", MONTHS, index=0, key="pred_compare_month")
        render_month_compare(current_user, compare_year, compare_month)
        st.divider()
        render_six_month_compare(current_user, compare_year, compare_month)

    with pred_tabs[4]:
        render_savings_chart(current_user)

    with pred_tabs[5]:
        render_profile_editor(current_user)
        if st.button("保存人物背景与账户", key="pred_save_profile"):
            save_db(db)
            st.success("已保存。")

    with pred_tabs[6]:
        render_ai_generator(current_user)

    with pred_tabs[7]:
        render_transfer_tab(db, current_user)

    with pred_tabs[8]:
        st.subheader("数据导出")
        export_json = json.dumps(current_user, ensure_ascii=False, indent=2)
        st.download_button("下载当前用户 JSON", export_json, file_name=f"{current_user['name']}_finance_timeline.json")

        excel_state_key = f"excel_bytes_{current_user['id']}_pred"
        if st.button("生成当前用户 Excel", key=f"build_excel_pred_{current_user['id']}"):
            with st.spinner("正在生成 Excel，请稍候..."):
                st.session_state[excel_state_key] = build_user_excel_bytes(get_user_snapshot(current_user))
            st.success("Excel 已生成，可直接下载。")

        if excel_state_key in st.session_state:
            st.download_button(
                "下载当前用户 Excel",
                data=st.session_state[excel_state_key],
                file_name=f"{current_user['name']}_finance_export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_excel_pred_{current_user['id']}"
            )

        all_json = json.dumps(db, ensure_ascii=False, indent=2)
        st.download_button("下载全部数据库 JSON", all_json, file_name="finance_db_all_users.json")
