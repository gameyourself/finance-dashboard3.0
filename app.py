import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# =========================
# Streamlit page config
# 必须尽量靠前，避免部分部署环境前端/后端初始化异常
# =========================
st.set_page_config(page_title="本地家庭财务云", layout="wide")

# =========================
# 数据目录
# 说明：
# 1) 本地运行默认写到当前项目 data_store
# 2) Render 如需持久化，请把 APP_DATA_DIR 设为你的持久磁盘挂载路径，例如 /var/data/finance_app
# =========================
APP_DATA_DIR = Path(os.getenv("APP_DATA_DIR", Path(__file__).parent / "data_store"))
APP_DATA_DIR.mkdir(parents=True, exist_ok=True)
USERS_FILE = APP_DATA_DIR / "users.json"
TRANSFERS_FILE = APP_DATA_DIR / "transfers.json"

# =========================
# 现金流分类结构（按你给的 AFP 家财分类）
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

# 默认映射到账户，用于“记一笔现金流时同步到资产负债表”
DEFAULT_ACCOUNT_MAP = {
    # 工作收入
    "工资薪金": "现金", "劳务报酬": "现金", "经营所得": "现金", "养老金": "现金", "其他主动收入": "现金",
    "礼物礼金": "现金", "补贴补助": "现金", "继承收入": "现金", "其他转移收入": "现金",
    "退税收入": "现金", "偶然所得": "现金", "其他所得": "现金",
    # 生活支出
    "饮食开支": "现金", "居住成本": "现金", "通勤交通": "现金", "衣着形象": "现金", "日常用品": "现金", "医疗支出": "现金", "其他生活消费": "现金",
    "旅行度假": "现金", "奢侈体验": "现金", "游戏娱乐": "现金", "其他娱乐消费": "现金",
    "学习提升": "现金", "健康投资": "现金", "兴趣培养": "现金", "其他个人成长消费": "现金",
    "赡养老人": "现金", "子女抚养": "现金", "宠物养护": "现金", "其他家庭成员消费": "现金",
    "人情往来": "现金", "商务应酬": "现金", "其他人际关系消费": "现金",
    "税费罚金": "现金", "意外损失": "现金", "其他无法归类支出": "现金",
    # 理财收入
    "理赔收入": "现金", "租金收入": "现金", "利息收入": "现金", "版权收入": "现金", "金融资产变现损益": "现金", "实物资产变现损益": "现金", "二手物品出售": "现金", "其他被动收入": "现金",
    # 理财支出
    "保费支出": "现金", "利息费用": "现金", "其他财务支出": "现金",
}

# =========================
# 资产负债表结构
# =========================
ASSET_STRUCTURE = {
    "流动性资产": ["现金", "货币基金/活期", "其他流动性存款"],
    "投资性资产": ["定期存款", "外币存款", "股票投资", "债券投资", "基金投资", "投资性房地产", "其他投资性资产"],
    "自用性资产": ["自用房产", "自用汽车", "其他自用性资产"],
    "其他资产": ["其他资产"],
}

LIABILITY_STRUCTURE = {
    "流动性负债": ["信用卡负债", "小额消费信贷", "其他流动性负债"],
    "投资性负债": ["金融投资借款", "实业投资借款", "投资性房地产贷款", "其他投资性负债"],
    "自用性负债": ["自住房按揭贷款", "自用车按揭贷款", "其他自用性负债"],
    "其他负债": ["其他负债"],
}

ACCOUNT_TO_SIDE = {}
for g, items in ASSET_STRUCTURE.items():
    for item in items:
        ACCOUNT_TO_SIDE[item] = ("assets", g)
for g, items in LIABILITY_STRUCTURE.items():
    for item in items:
        ACCOUNT_TO_SIDE[item] = ("liabilities", g)

ALL_CF_LEVEL2 = []
ALL_CF_LEVEL3 = []
LEVEL3_TO_LEVEL1 = {}
LEVEL3_TO_LEVEL2 = {}
for lvl1, lvl2_map in CASHFLOW_STRUCTURE.items():
    for lvl2, lvl3_items in lvl2_map.items():
        ALL_CF_LEVEL2.append(lvl2)
        for item in lvl3_items:
            ALL_CF_LEVEL3.append(item)
            LEVEL3_TO_LEVEL1[item] = lvl1
            LEVEL3_TO_LEVEL2[item] = lvl2

STRUCTURE_ORDER = [
    "流动性资产", "流动性负债",
    "投资性资产", "投资性负债",
    "自用性资产", "自用性负债",
    "其他资产", "其他负债",
]

YEARS = list(range(25, 36))
MONTHS = list(range(1, 13))


# =========================
# 基础数据函数
# =========================
def empty_cashflow_items() -> Dict[str, float]:
    return {k: 0.0 for k in ALL_CF_LEVEL3}


def empty_balance_assets() -> Dict[str, float]:
    return {k: 0.0 for group in ASSET_STRUCTURE.values() for k in group}


def empty_balance_liabilities() -> Dict[str, float]:
    return {k: 0.0 for group in LIABILITY_STRUCTURE.values() for k in group}


def empty_month_record(age_year: int, month: int) -> Dict:
    return {
        "age_year": age_year,
        "month": month,
        "month_key": f"{age_year}岁-{month}月",
        "note": "",
        "cashflow": empty_cashflow_items(),
        "assets": empty_balance_assets(),
        "liabilities": empty_balance_liabilities(),
    }


def build_default_user(name: str, uid: Optional[str] = None) -> Dict:
    if uid is None:
        uid = f"user_{int(datetime.now().timestamp())}"
    monthly_data = []
    for y in YEARS:
        for m in MONTHS:
            monthly_data.append(empty_month_record(y, m))
    return {
        "id": uid,
        "name": name,
        "profile": {
            "name": name,
            "start_age": 25,
            "end_age": 35,
            "job": "上班族",
            "city_level": "二线城市",
            "marital_status": "未填写",
            "background": "",
            "basic_goal": "优先稳定现金流；逐步建立储蓄",
        },
        "monthly_data": monthly_data,
    }


def load_json_file(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return default
    return default


def save_json_file(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_users():
    data = load_json_file(USERS_FILE, {"users": []})
    if "users" not in data:
        data = {"users": []}
    return data


def save_users(data):
    save_json_file(USERS_FILE, data)


def load_transfers():
    data = load_json_file(TRANSFERS_FILE, {"transfers": []})
    if "transfers" not in data:
        data = {"transfers": []}
    return data


def save_transfers(data):
    save_json_file(TRANSFERS_FILE, data)


def build_users_index(users_data):
    return {u["id"]: u for u in users_data.get("users", [])}


def ensure_full_timeline(user: Dict):
    existing = {(r["age_year"], r["month"]): r for r in user.get("monthly_data", [])}
    new_records = []
    for y in YEARS:
        for m in MONTHS:
            record = existing.get((y, m), empty_month_record(y, m))
            record.setdefault("note", "")
            record.setdefault("cashflow", empty_cashflow_items())
            record.setdefault("assets", empty_balance_assets())
            record.setdefault("liabilities", empty_balance_liabilities())
            record["month_key"] = f"{y}岁-{m}月"
            new_records.append(record)
    user["monthly_data"] = new_records


def get_month_record(user: Dict, age_year: int, month: int) -> Dict:
    for r in user["monthly_data"]:
        if r["age_year"] == age_year and r["month"] == month:
            return r
    raise KeyError("未找到月份记录")


def previous_month(age_year: int, month: int):
    if month > 1:
        return age_year, month - 1
    if age_year > YEARS[0]:
        return age_year - 1, 12
    return None


def apply_cashflow_to_balance(record: Dict, level3_name: str, amount: float, account_name: Optional[str] = None):
    account = account_name or DEFAULT_ACCOUNT_MAP.get(level3_name, "现金")
    side_info = ACCOUNT_TO_SIDE.get(account)
    if not side_info:
        return
    bucket, _ = side_info
    level1 = LEVEL3_TO_LEVEL1.get(level3_name, "")
    # 收入增加账户余额；支出减少账户余额
    if level1 in ["工作收入", "理财收入"]:
        record[bucket][account] = float(record[bucket].get(account, 0) or 0) + float(amount)
    elif level1 in ["生活支出", "理财支出"]:
        record[bucket][account] = float(record[bucket].get(account, 0) or 0) - float(amount)


def transfer_between_accounts(record: Dict, from_account: str, to_account: str, amount: float, note: str = ""):
    if amount <= 0:
        return
    if from_account not in ACCOUNT_TO_SIDE or to_account not in ACCOUNT_TO_SIDE:
        return
    from_bucket, _ = ACCOUNT_TO_SIDE[from_account]
    to_bucket, _ = ACCOUNT_TO_SIDE[to_account]
    record[from_bucket][from_account] = float(record[from_bucket].get(from_account, 0) or 0) - amount
    record[to_bucket][to_account] = float(record[to_bucket].get(to_account, 0) or 0) + amount
    if note:
        record["note"] = (record.get("note", "") + f"\n{note}").strip()


def compute_cashflow_summary(record: Dict) -> Dict[str, float]:
    cf = record["cashflow"]
    work_income = sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["工作收入"].values() for x in lvl2)
    living_expense = sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["生活支出"].values() for x in lvl2)
    finance_income = sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["理财收入"].values() for x in lvl2)
    finance_expense = sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["理财支出"].values() for x in lvl2)
    work_saving = work_income - living_expense
    finance_saving = finance_income - finance_expense
    saving = work_saving + finance_saving
    return {
        "工作收入": work_income,
        "生活支出": living_expense,
        "工作储蓄": work_saving,
        "理财收入": finance_income,
        "理财支出": finance_expense,
        "理财储蓄": finance_saving,
        "储蓄": saving,
    }


def compute_balance_summary(record: Dict) -> Dict[str, float]:
    totals = {}
    for group, items in ASSET_STRUCTURE.items():
        totals[group] = sum(float(record["assets"].get(i, 0) or 0) for i in items)
    for group, items in LIABILITY_STRUCTURE.items():
        totals[group] = sum(float(record["liabilities"].get(i, 0) or 0) for i in items)
    totals["总资产"] = totals["流动性资产"] + totals["投资性资产"] + totals["自用性资产"] + totals["其他资产"]
    totals["总负债"] = totals["流动性负债"] + totals["投资性负债"] + totals["自用性负债"] + totals["其他负债"]
    totals["净值"] = totals["总资产"] - totals["总负债"]
    return totals


def aggregate_year(user: Dict, age_year: int) -> Dict:
    rows = [r for r in user["monthly_data"] if r["age_year"] == age_year]
    cf = {k: 0.0 for k in ALL_CF_LEVEL3}
    notes = []
    for r in rows:
        for k, v in r["cashflow"].items():
            cf[k] += float(v or 0)
        if r.get("note"):
            notes.append(f"{r['month']}月：{r['note']}")
    last_month = rows[-1]
    return {
        "age_year": age_year,
        "cashflow": cf,
        "cashflow_summary": {
            "工作收入": sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["工作收入"].values() for x in lvl2),
            "生活支出": sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["生活支出"].values() for x in lvl2),
            "理财收入": sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["理财收入"].values() for x in lvl2),
            "理财支出": sum(cf.get(x, 0) for lvl2 in CASHFLOW_STRUCTURE["理财支出"].values() for x in lvl2),
        },
        "balance_summary": compute_balance_summary(last_month),
        "year_end_assets": last_month["assets"],
        "year_end_liabilities": last_month["liabilities"],
        "notes": notes,
    }


def compare_records(cur: Dict, prev: Dict, fields: List[str]) -> pd.DataFrame:
    rows = []
    for item in fields:
        cur_v = float(cur.get(item, 0) or 0)
        prev_v = float(prev.get(item, 0) or 0)
        rows.append({
            "项目": item,
            "本期": cur_v,
            "上期": prev_v,
            "差额": cur_v - prev_v,
            "方向": "增加" if cur_v > prev_v else ("减少" if cur_v < prev_v else "持平")
        })
    return pd.DataFrame(rows)


def month_records_df(user: Dict) -> pd.DataFrame:
    rows = []
    for r in user["monthly_data"]:
        cf_summary = compute_cashflow_summary(r)
        bal_summary = compute_balance_summary(r)
        row = {
            "年龄": r["age_year"],
            "月份": r["month"],
            "月份标签": r["month_key"],
            **cf_summary,
            **bal_summary,
        }
        for k, v in r["cashflow"].items():
            row[k] = float(v or 0)
        rows.append(row)
    return pd.DataFrame(rows)


def year_summary_df(user: Dict) -> pd.DataFrame:
    rows = []
    for y in YEARS:
        agg = aggregate_year(user, y)
        work_income = agg["cashflow_summary"]["工作收入"]
        living_expense = agg["cashflow_summary"]["生活支出"]
        finance_income = agg["cashflow_summary"]["理财收入"]
        finance_expense = agg["cashflow_summary"]["理财支出"]
        rows.append({
            "年龄": y,
            "工作收入": work_income,
            "生活支出": living_expense,
            "工作储蓄": work_income - living_expense,
            "理财收入": finance_income,
            "理财支出": finance_expense,
            "理财储蓄": finance_income - finance_expense,
            "储蓄": (work_income - living_expense) + (finance_income - finance_expense),
            **agg["balance_summary"],
        })
    return pd.DataFrame(rows)


def init_demo_data_if_empty(users_data: Dict):
    if users_data.get("users"):
        return users_data
    user = build_default_user("示例用户A")
    # 简单生成一条完整但合理的轨迹
    cash = 50000.0
    fixed = 10000.0
    mortgage = 350000.0
    fund = 0.0
    for r in user["monthly_data"]:
        y = r["age_year"]
        m = r["month"]
        salary = 9000 + (y - 25) * 400
        if m in [2]:
            salary += 3000
        if m in [6, 12]:
            salary += 5000
        food = 1800 + (y - 25) * 70
        housing = 2600
        transport = 500 + (50 if y >= 28 else 0)
        daily = 600
        child = 0 if y < 30 else 1200
        game = 200 if m not in [2, 8, 10] else 500
        trip = 0 if m not in [5, 10] else 2500
        interest_income = fixed * 0.0018
        insurance = 200
        mortgage_interest = 800
        principal_pay = 1200

        r["cashflow"]["工资薪金"] = salary
        r["cashflow"]["饮食开支"] = food
        r["cashflow"]["居住成本"] = housing
        r["cashflow"]["通勤交通"] = transport
        r["cashflow"]["日常用品"] = daily
        r["cashflow"]["游戏娱乐"] = game
        r["cashflow"]["旅行度假"] = trip
        r["cashflow"]["子女抚养"] = child
        r["cashflow"]["利息收入"] = round(interest_income, 2)
        r["cashflow"]["保费支出"] = insurance
        r["cashflow"]["利息费用"] = mortgage_interest

        # 同步到账户
        for k, v in r["cashflow"].items():
            if v:
                apply_cashflow_to_balance(r, k, v)

        cash = r["assets"]["现金"] + cash
        # 每月固定转入定期/基金
        to_fixed = 800 if cash > 15000 else 0
        to_fund = 500 if y >= 27 and cash > 18000 else 0
        if to_fixed > 0:
            cash -= to_fixed
            fixed += to_fixed
        if to_fund > 0:
            cash -= to_fund
            fund += to_fund

        mortgage -= principal_pay
        cash -= principal_pay

        r["assets"]["现金"] = round(cash, 2)
        r["assets"]["定期存款"] = round(fixed, 2)
        r["assets"]["基金投资"] = round(fund, 2)
        r["assets"]["自用房产"] = 700000 + (y - 25) * 3000
        r["liabilities"]["自住房按揭贷款"] = round(max(mortgage, 0), 2)

        note_parts = ["工资到账后先覆盖日常支出"]
        if to_fixed:
            note_parts.append(f"转入定期存款{to_fixed:.0f}元")
        if to_fund:
            note_parts.append(f"小额定投基金{to_fund:.0f}元")
        if trip > 0:
            note_parts.append("本月有旅行支出")
        if m in [6, 12]:
            note_parts.append("阶段性奖金使现金结余上升")
        if y >= 30 and child > 0:
            note_parts.append("家庭阶段变化带来子女抚养支出")
        note_parts.append("按揭本金继续缓慢下降")
        r["note"] = "；".join(note_parts)

    users_data["users"] = [user]
    save_users(users_data)
    return users_data


# =========================
# UI 工具
# =========================
def style_diff_df(df: pd.DataFrame):
    def color_diff(v):
        try:
            v = float(v)
        except Exception:
            return ""
        if v > 0:
            return "color: red;"
        if v < 0:
            return "color: blue;"
        return ""
    return df.style.map(color_diff, subset=["差额"])


def export_excel_bytes(sheets: Dict[str, pd.DataFrame]) -> bytes:
    from io import BytesIO
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_name = str(sheet_name)[:31]
            df.to_excel(writer, sheet_name=safe_name, index=False)
    return output.getvalue()


def plot_compare_line(df: pd.DataFrame, x_col: str, y_cols: List[str], title: str):
    fig = go.Figure()
    for col in y_cols:
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df[col], mode="lines+markers",
            name=col,
            hovertemplate="%{x}<br>%{y:,.2f}<extra>" + col + "</extra>"
        ))
    fig.update_layout(title=title, xaxis_title=x_col, yaxis_title="金额")
    st.plotly_chart(fig, use_container_width=True)


def plot_saving_line(df: pd.DataFrame, x_col: str):
    fig = go.Figure()
    for col in ["工作储蓄", "理财储蓄", "储蓄"]:
        fig.add_trace(go.Scatter(
            x=df[x_col], y=df[col], mode="lines+markers", name=col,
            hovertemplate="%{x}<br>%{y:,.2f}<extra>" + col + "</extra>"
        ))
    fig.update_layout(title="储蓄变化", xaxis_title=x_col, yaxis_title="金额")
    st.plotly_chart(fig, use_container_width=True)


def record_selector(user: Dict):
    age_year = st.selectbox("选择年龄年度", YEARS, index=0)
    month = st.selectbox("选择月份", MONTHS, index=0)
    record = get_month_record(user, age_year, month)
    prev = previous_month(age_year, month)
    prev_record = get_month_record(user, prev[0], prev[1]) if prev else None
    return age_year, month, record, prev_record


# =========================
# 会话初始化
# =========================
def init_session_state():
    defaults = {
        "generated_json_text": "",
        "manual_json_text": "",
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


init_session_state()

# =========================
# 加载数据
# =========================
users_data = load_users()
users_data = init_demo_data_if_empty(users_data)
user_index = build_users_index(users_data)
transfers_data = load_transfers()

# =========================
# 页面标题
# =========================
st.title("家庭财务预测与访问系统")
st.caption(f"当前数据目录：{APP_DATA_DIR}")

with st.expander("部署与存储说明", expanded=False):
    st.markdown(
        """
- 本地运行时，数据默认保存在项目目录下 `data_store`
- Render 默认不是“读取你电脑本地文件”，而是运行在云端自己的文件系统里
- 如果需要云端长期保存，请把环境变量 `APP_DATA_DIR` 指向 Render Persistent Disk 挂载目录
- 如果只是把 `users.json` 提交到 GitHub，云端首次部署会读到仓库里的那个文件；但运行后新增的数据不会自动回写 GitHub
        """
    )

with st.sidebar:
    st.header("用户管理")
    user_names = {u["id"]: u["name"] for u in users_data.get("users", [])}
    selected_id = st.selectbox("选择用户", list(user_names.keys()), format_func=lambda x: user_names[x])
    new_name = st.text_input("新增用户名称")
    if st.button("创建新用户"):
        if new_name.strip():
            users_data["users"].append(build_default_user(new_name.strip()))
            save_users(users_data)
            st.success("已创建")
            st.rerun()
        else:
            st.warning("请先输入名称")

user = user_index[selected_id]
ensure_full_timeline(user)

# =========================
# Tabs
# =========================
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "月份访问", "年度/五年对比", "录入与联动", "账户转账", "AI导入", "数据导出"
])

with tab1:
    age_year, month, record, prev_record = record_selector(user)
    st.subheader(f"{record['month_key']} 数据访问")

    c1, c2, c3 = st.columns(3)
    bal = compute_balance_summary(record)
    c1.metric("总资产", f"{bal['总资产']:,.2f}")
    c2.metric("总负债", f"{bal['总负债']:,.2f}")
    c3.metric("净值", f"{bal['净值']:,.2f}")

    st.markdown("### 本月说明")
    st.info(record.get("note") or "暂无说明")

    # 现金流明细
    st.markdown("### 现金流明细")
    cf_rows = []
    for item in ALL_CF_LEVEL3:
        value = float(record["cashflow"].get(item, 0) or 0)
        if value != 0:
            cf_rows.append({
                "一级分类": LEVEL3_TO_LEVEL1[item],
                "二级分类": LEVEL3_TO_LEVEL2[item],
                "三级分类": item,
                "金额": value,
            })
    cf_df = pd.DataFrame(cf_rows) if cf_rows else pd.DataFrame(columns=["一级分类", "二级分类", "三级分类", "金额"])
    st.dataframe(cf_df, use_container_width=True, hide_index=True)

    # 比对表
    st.markdown("### 与上月差值对比")
    compare_scope = st.radio("对比维度", ["三级分类", "一级分类", "储蓄指标"], horizontal=True)
    if prev_record is None:
        st.info("这是起始月份，没有上月数据。")
    else:
        if compare_scope == "三级分类":
            fields = ALL_CF_LEVEL3
            cur_map = record["cashflow"]
            prev_map = prev_record["cashflow"]
        elif compare_scope == "一级分类":
            cur_map = {}
            prev_map = {}
            for lvl1 in ["工作收入", "生活支出", "理财收入", "理财支出"]:
                items = [x for lvl2 in CASHFLOW_STRUCTURE[lvl1].values() for x in lvl2]
                cur_map[lvl1] = sum(record["cashflow"].get(x, 0) for x in items)
                prev_map[lvl1] = sum(prev_record["cashflow"].get(x, 0) for x in items)
            fields = list(cur_map.keys())
        else:
            cur_map = compute_cashflow_summary(record)
            prev_map = compute_cashflow_summary(prev_record)
            fields = ["工作收入", "生活支出", "工作储蓄", "理财收入", "理财支出", "理财储蓄", "储蓄"]

        compare_df = compare_records(cur_map, prev_map, fields)
        st.dataframe(style_diff_df(compare_df), use_container_width=True, hide_index=True)
        plot_compare_line(compare_df.reset_index().assign(横轴=compare_df["项目"]), "横轴", ["本期", "上期", "差额"], f"{record['month_key']} 与上月对比")

    # 六个月窗口与单项趋势
    st.markdown("### 六个月区间 / 单项比对")
    selected_item = st.selectbox("选择单项三级分类", ALL_CF_LEVEL3, index=ALL_CF_LEVEL3.index("子女抚养") if "子女抚养" in ALL_CF_LEVEL3 else 0)
    mdf = month_records_df(user)
    current_index = mdf[(mdf["年龄"] == age_year) & (mdf["月份"] == month)].index[0]
    window_df = mdf.iloc[max(0, current_index - 5): current_index + 1].copy()
    plot_compare_line(window_df, "月份标签", [selected_item], f"近六个月：{selected_item}")

with tab2:
    st.subheader("年度访问与五年对比")
    ydf = year_summary_df(user)
    selected_year = st.selectbox("查看单年汇总", YEARS, index=0, key="single_year")
    year_row = ydf[ydf["年龄"] == selected_year]
    st.dataframe(year_row, use_container_width=True, hide_index=True)

    st.markdown("### 五年对比")
    start_year = st.selectbox("起始年龄", YEARS[:-4], index=0)
    end_year = start_year + 4
    five_df = ydf[(ydf["年龄"] >= start_year) & (ydf["年龄"] <= end_year)].copy()
    metric_options = ["工作收入", "生活支出", "工作储蓄", "理财收入", "理财支出", "理财储蓄", "储蓄", "总资产", "总负债", "净值"]
    selected_metrics = st.multiselect("选择对比指标", metric_options, default=["储蓄", "总资产", "净值"])
    if selected_metrics:
        plot_compare_line(five_df, "年龄", selected_metrics, f"{start_year}岁-{end_year}岁五年对比")
        st.dataframe(five_df[["年龄"] + selected_metrics], use_container_width=True, hide_index=True)

    st.markdown("### 第三个图：储蓄公式趋势")
    range_mode = st.selectbox("查看范围", ["近3月", "近1年", "成立以来", "近5年"])
    mdf = month_records_df(user)
    if range_mode == "近3月":
        plot_df = mdf.tail(3)
    elif range_mode == "近1年":
        plot_df = mdf.tail(12)
    elif range_mode == "近5年":
        plot_df = mdf.tail(60)
    else:
        plot_df = mdf
    plot_saving_line(plot_df, "月份标签")

with tab3:
    st.subheader("手动录入 + 资产负债联动")
    age_year_edit = st.selectbox("录入年龄", YEARS, key="edit_year")
    month_edit = st.selectbox("录入月份", MONTHS, key="edit_month")
    rec = get_month_record(user, age_year_edit, month_edit)

    selected_level3 = st.selectbox("三级分类", ALL_CF_LEVEL3)
    default_account = DEFAULT_ACCOUNT_MAP.get(selected_level3, "现金")
    account_options = list(ACCOUNT_TO_SIDE.keys())
    selected_account = st.selectbox("同步到账户", account_options, index=account_options.index(default_account) if default_account in account_options else 0)
    amount = st.number_input("金额", min_value=0.0, step=100.0, value=0.0)
    note = st.text_area("本月说明")
    if st.button("新增一笔并同步"):
        rec["cashflow"][selected_level3] = float(rec["cashflow"].get(selected_level3, 0) or 0) + amount
        apply_cashflow_to_balance(rec, selected_level3, amount, selected_account)
        if note.strip():
            rec["note"] = note.strip()
        save_users(users_data)
        st.success("已录入并同步到资产负债表")
        st.rerun()

    st.markdown("### 资产间转换")
    from_account = st.selectbox("转出账户", list(ACCOUNT_TO_SIDE.keys()), key="from_account")
    to_account = st.selectbox("转入账户", list(ACCOUNT_TO_SIDE.keys()), key="to_account")
    transfer_amount = st.number_input("转换金额", min_value=0.0, step=100.0, value=0.0, key="transfer_amount_local")
    transfer_note = st.text_input("转换说明", value="资产间转换")
    if st.button("执行资产间转换"):
        transfer_between_accounts(rec, from_account, to_account, transfer_amount, transfer_note)
        save_users(users_data)
        st.success("已完成转换")
        st.rerun()

    st.markdown("### 当前月资产负债表")
    bal_rows = []
    for group, items in ASSET_STRUCTURE.items():
        for item in items:
            bal_rows.append({"类型": "资产", "一级": group, "项目": item, "金额": rec["assets"].get(item, 0)})
    for group, items in LIABILITY_STRUCTURE.items():
        for item in items:
            bal_rows.append({"类型": "负债", "一级": group, "项目": item, "金额": rec["liabilities"].get(item, 0)})
    st.dataframe(pd.DataFrame(bal_rows), use_container_width=True, hide_index=True)

with tab4:
    st.subheader("多用户账户间转账")
    other_user_ids = [uid for uid in user_index if uid != user["id"]]
    if not other_user_ids:
        st.info("至少需要两个用户才能转账。")
    else:
        target_user_id = st.selectbox("收款用户", other_user_ids, format_func=lambda x: user_index[x]["name"])
        transfer_year = st.selectbox("转账年龄", YEARS, key="tf_year")
        transfer_month = st.selectbox("转账月份", MONTHS, key="tf_month")
        tf_amount = st.number_input("转账金额", min_value=0.0, step=100.0, value=0.0, key="tf_amount")
        tf_note = st.text_input("转账备注", value="用户间转账")
        if st.button("执行转账"):
            source_record = get_month_record(user, transfer_year, transfer_month)
            target_record = get_month_record(user_index[target_user_id], transfer_year, transfer_month)
            source_record["assets"]["现金"] -= tf_amount
            target_record["assets"]["现金"] += tf_amount
            transfers_data["transfers"].append({
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "from_user": user["name"],
                "to_user": user_index[target_user_id]["name"],
                "age_year": transfer_year,
                "month": transfer_month,
                "amount": tf_amount,
                "note": tf_note,
            })
            save_users(users_data)
            save_transfers(transfers_data)
            st.success("转账完成")
            st.rerun()

    st.markdown("### 转账记录")
    st.dataframe(pd.DataFrame(transfers_data.get("transfers", [])), use_container_width=True, hide_index=True)

with tab5:
    st.subheader("AI数据导入")
    st.markdown("**人物背景模板示例**")
    st.code(
        "30岁，二线城市上班族，已婚，有房贷，收入稳定，消费偏日常型，希望优先稳住现金流，再逐步建立储蓄和小额理财。",
        language="text"
    )
    profile = user["profile"]
    profile["background"] = st.text_area("人物背景", value=str(profile.get("background", "")), height=120)
    profile["basic_goal"] = st.text_input("理财目标偏好", value=str(profile.get("basic_goal", "")))
    selected_gen_fields = st.multiselect("希望生成的三级分类", ALL_CF_LEVEL3, default=["工资薪金", "饮食开支", "居住成本", "通勤交通", "利息收入"])
    st.session_state["manual_json_text"] = st.text_area("粘贴AI返回JSON", value=st.session_state.get("manual_json_text", ""), height=320)
    if st.button("导入AI JSON"):
        try:
            payload = json.loads(st.session_state["manual_json_text"])
            if "monthly_data" in payload:
                month_map = {(r["age_year"], r["month"]): r for r in user["monthly_data"]}
                for mr in payload["monthly_data"]:
                    key = (int(mr["age_year"]), int(mr["month"]))
                    if key in month_map:
                        month_map[key]["note"] = mr.get("note", month_map[key].get("note", ""))
                        for k, v in mr.get("cashflow", {}).items():
                            if k in month_map[key]["cashflow"]:
                                month_map[key]["cashflow"][k] = float(v or 0)
                        for k, v in mr.get("assets", {}).items():
                            if k in month_map[key]["assets"]:
                                month_map[key]["assets"][k] = float(v or 0)
                        for k, v in mr.get("liabilities", {}).items():
                            if k in month_map[key]["liabilities"]:
                                month_map[key]["liabilities"][k] = float(v or 0)
            save_users(users_data)
            st.success("导入成功")
            st.rerun()
        except Exception as e:
            st.error(f"导入失败：{e}")

    st.markdown("### AI JSON 样例骨架")
    sample = {
        "monthly_data": [
            {
                "age_year": 25,
                "month": 1,
                "note": "工资到账后控制消费，留出部分现金",
                "cashflow": {k: 0 for k in selected_gen_fields[:5]},
                "assets": {"现金": 50000, "定期存款": 10000, "基金投资": 2000},
                "liabilities": {"自住房按揭贷款": 320000}
            }
        ]
    }
    st.code(json.dumps(sample, ensure_ascii=False, indent=2), language="json")

with tab6:
    st.subheader("数据导出")
    mdf = month_records_df(user)
    ydf = year_summary_df(user)
    st.download_button(
        "下载当前用户月度明细 Excel",
        data=export_excel_bytes({"月度明细": mdf, "年度汇总": ydf}),
        file_name=f"{user['name']}_财务数据.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    st.download_button(
        "下载当前用户 JSON",
        data=json.dumps(user, ensure_ascii=False, indent=2),
        file_name=f"{user['name']}.json",
        mime="application/json"
    )
    st.download_button(
        "下载全部用户 JSON",
        data=json.dumps(users_data, ensure_ascii=False, indent=2),
        file_name="all_users.json",
        mime="application/json"
    )
