"""Shared helpers for selecting the tracked SW2021 level-2 industries."""

from __future__ import annotations

from typing import Any


# 申万行业分类（2021）的 134 个二级行业名称。来源：东方财富官方数据文档。
# https://emt.18.cn/api/quant-help/data/stock.html
SW2021_LEVEL2_NAMES = frozenset({
    "种植业", "渔业", "林业Ⅱ", "饲料", "农产品加工", "养殖业", "动物保健Ⅱ",
    "农业综合Ⅱ", "化学原料", "化学制品", "化学纤维", "塑料", "橡胶", "农化制品",
    "非金属材料Ⅱ", "冶钢原料", "普钢", "特钢Ⅱ", "金属新材料", "工业金属",
    "贵金属", "小金属", "能源金属", "半导体", "元件", "光学光电子", "其他电子Ⅱ",
    "消费电子", "电子化学品Ⅱ", "汽车零部件", "汽车服务", "摩托车及其他", "乘用车",
    "商用车", "白色家电", "黑色家电", "小家电", "厨卫电器", "照明设备Ⅱ",
    "家电零部件Ⅱ", "其他家电Ⅱ", "食品加工", "白酒Ⅱ", "非白酒", "饮料乳品",
    "休闲食品", "调味发酵品Ⅱ", "纺织制造", "服装家纺", "饰品", "造纸", "包装印刷",
    "家居用品", "文娱用品", "化学制药", "中药Ⅱ", "生物制品", "医药商业", "医疗器械",
    "医疗服务", "电力", "燃气Ⅱ", "物流", "铁路公路", "航空机场", "航运港口",
    "房地产开发", "房地产服务", "贸易Ⅱ", "一般零售", "专业连锁Ⅱ", "互联网电商",
    "旅游零售Ⅱ", "体育Ⅱ", "本地生活服务Ⅱ", "专业服务", "酒店餐饮", "旅游及景区",
    "教育", "国有大型银行Ⅱ", "股份制银行Ⅱ", "城商行Ⅱ", "农商行Ⅱ", "其他银行Ⅱ",
    "证券Ⅱ", "保险Ⅱ", "多元金融", "综合Ⅱ", "水泥", "玻璃玻纤", "装修建材",
    "房屋建设Ⅱ", "装修装饰Ⅱ", "基础建设", "专业工程", "工程咨询服务Ⅱ", "电机Ⅱ",
    "其他电源设备Ⅱ", "光伏设备", "风电设备", "电池", "电网设备", "通用设备",
    "专用设备", "轨交设备Ⅱ", "工程机械", "自动化设备", "航天装备Ⅱ", "航空装备Ⅱ",
    "地面兵装Ⅱ", "航海装备Ⅱ", "军工电子Ⅱ", "计算机设备", "IT服务Ⅱ", "软件开发",
    "游戏Ⅱ", "广告营销", "影视院线", "数字媒体", "社交Ⅱ", "出版", "电视广播Ⅱ",
    "通信服务", "通信设备", "煤炭开采", "焦炭Ⅱ", "油气开采Ⅱ", "油服工程",
    "炼化及贸易", "环境治理", "环保设备Ⅱ", "个护用品", "化妆品", "医疗美容",
})


def as_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def filter_second_level_industries(
    sectors: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Keep official SW2021 level-2 industries; concept sectors pass through."""
    return [
        sector
        for sector in sectors
        if sector.get("sector_type") != "industry"
        or str(sector.get("name") or "").strip() in SW2021_LEVEL2_NAMES
    ]


def select_top_sectors(
    sectors: list[dict[str, Any]],
    top: int,
) -> list[dict[str, Any]]:
    """Deduplicate sectors and rank them by total market capitalization."""
    by_code: dict[str, dict[str, Any]] = {}
    for sector in sectors:
        code = str(sector.get("code") or "").upper()
        if not code:
            continue
        candidate = {
            **sector,
            "code": code,
            "market_cap": as_float(sector.get("market_cap")),
        }
        current = by_code.get(code)
        if current is None or candidate["market_cap"] > current["market_cap"]:
            by_code[code] = candidate

    ranked = sorted(
        by_code.values(),
        key=lambda item: (item["market_cap"], item["code"]),
        reverse=True,
    )
    return ranked[:top]
