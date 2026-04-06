"""리베이트 계산 엔진

CSV 데이터를 받아 거래처별 리베이트를 계산한다.
"""
import csv
import io
from typing import Optional
from services.rebate_config import load_rebate_settings


def parse_csv(csv_content: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(csv_content))
    return list(reader)


def classify_product_group(group_name: str, settings: dict) -> Optional[str]:
    g = group_name.strip()
    discount_groups = settings.get("discount_groups", {})
    for category, names in discount_groups.items():
        if g in names:
            return category
    return None


def calculate_rebates(csv_content: str, settings: dict = None) -> dict:
    if settings is None:
        settings = load_rebate_settings()

    rows = parse_csv(csv_content)
    if not rows:
        raise ValueError("CSV 데이터가 비어있습니다.")

    thresholds = settings["tier_thresholds"]
    tier_10_min = thresholds["tier_10_min"]
    tier_5_min = thresholds["tier_5_min"]
    discount_rates = settings["discount_rates"]
    excluded_groups = settings["excluded_product_groups"]
    excluded_customers = settings["excluded_customers"]
    exception_customers = {e["name"]: e for e in settings["exception_customers"]}

    use_allowed_list = settings.get("use_allowed_list", False)
    allowed_customers = set(settings.get("allowed_customers", []))
    aliases = settings.get("allowed_customer_aliases", {})

    first_date = rows[0].get("연/월/일", "")
    target_month = ""
    if first_date and len(first_date) >= 6:
        year = first_date[:4]
        month = first_date[4:6]
        target_month = f"{year}-{month}"

    customer_data = {}
    for row in rows:
        cust_name = row.get("거래처명", "").strip()
        group_name = row.get("품목그룹1명", "").strip()

        if not cust_name:
            continue
        if group_name in excluded_groups:
            continue
        if cust_name in excluded_customers:
            continue

        if use_allowed_list:
            if cust_name not in allowed_customers and cust_name not in aliases:
                continue
            if cust_name in aliases:
                cust_name = aliases[cust_name]

        try:
            supply_amt = float(row.get("공급가액", "0"))
        except (ValueError, TypeError):
            continue

        if cust_name not in customer_data:
            customer_data[cust_name] = {
                "total_sales": 0,
                "main": 0,
                "lanstar_3": 0,
                "lanstar_5": 0,
                "printer": 0,
                "returns_amount": 0,  # Feature 5: 반품/크레딧 노트
            }

        # Feature 5: 반품/크레딧 노트 별도 처리
        if supply_amt < 0:
            customer_data[cust_name]["returns_amount"] += supply_amt

        category = classify_product_group(group_name, settings)
        if category:
            customer_data[cust_name]["total_sales"] += supply_amt
            customer_data[cust_name][category] += supply_amt

    rate_upgrade_map = {}
    for entry in settings.get("rate_upgrade_customers", []):
        upgrades = {}
        for orig, upgraded in entry.get("upgrades", {}).items():
            upgrades[float(orig)] = float(upgraded)
        rate_upgrade_map[entry["name"]] = upgrades

    results = []
    for cust_name, data in customer_data.items():
        total_sales = int(data["total_sales"])

        is_exception = False
        if total_sales >= tier_10_min:
            tier = "10%"
        elif total_sales >= tier_5_min:
            tier = "5%"
        elif cust_name in exception_customers:
            tier = exception_customers[cust_name]["min_tier"]
            is_exception = True
        else:
            continue

        if cust_name in exception_customers:
            exc = exception_customers[cust_name]
            if exc["min_tier"] == "10%" and tier == "5%":
                tier = "10%"
                is_exception = True

        main_rate = discount_rates["main"][tier]
        lanstar3_rate = discount_rates["lanstar_3"][tier]
        lanstar5_rate = discount_rates["lanstar_5"][tier]
        printer_rate = discount_rates["printer"][tier]

        is_rate_upgrade = cust_name in rate_upgrade_map
        if is_rate_upgrade:
            upgrades = rate_upgrade_map[cust_name]
            main_rate = upgrades.get(main_rate, main_rate)
            lanstar3_rate = upgrades.get(lanstar3_rate, lanstar3_rate)
            lanstar5_rate = upgrades.get(lanstar5_rate, lanstar5_rate)
            printer_rate = upgrades.get(printer_rate, printer_rate)

        main_sales = int(data["main"])
        lanstar3_sales = int(data["lanstar_3"])
        lanstar5_sales = int(data["lanstar_5"])
        printer_sales = int(data["printer"])

        main_rebate = round(main_sales * main_rate)
        lanstar3_rebate = round(lanstar3_sales * lanstar3_rate)
        lanstar5_rebate = round(lanstar5_sales * lanstar5_rate)
        printer_rebate = round(printer_sales * printer_rate)
        total_rebate = main_rebate + lanstar3_rebate + lanstar5_rebate + printer_rebate

        customer_code = ""
        if cust_name in exception_customers:
            customer_code = exception_customers[cust_name].get("code", "")

        returns_amt = int(data.get("returns_amount", 0))

        results.append({
            "customer_name": cust_name,
            "customer_code": customer_code,
            "tier": tier,
            "total_sales": total_sales,
            "main_sales": main_sales,
            "lanstar3_sales": lanstar3_sales,
            "lanstar5_sales": lanstar5_sales,
            "printer_sales": printer_sales,
            "main_rebate": main_rebate,
            "lanstar3_rebate": lanstar3_rebate,
            "lanstar5_rebate": lanstar5_rebate,
            "printer_rebate": printer_rebate,
            "total_rebate": total_rebate,
            "is_exception": is_exception,
            "is_rate_upgrade": is_rate_upgrade,
            "is_excluded": False,
            "manual_adjustment": 0,
            "returns_amount": returns_amt,  # Feature 5: 반품/크레딧 노트
        })

    results.sort(key=lambda x: x["total_sales"], reverse=True)

    tier_10 = [r for r in results if r["tier"] == "10%" and not r["is_excluded"]]
    tier_5 = [r for r in results if r["tier"] == "5%" and not r["is_excluded"]]
    total_rebate_sum = sum(r["total_rebate"] for r in results if not r["is_excluded"])

    return {
        "target_month": target_month,
        "summary": {
            "total_customers": len(tier_10) + len(tier_5),
            "tier_10_count": len(tier_10),
            "tier_5_count": len(tier_5),
            "total_rebate": total_rebate_sum,
            "tier_10_rebate": sum(r["total_rebate"] for r in tier_10),
            "tier_5_rebate": sum(r["total_rebate"] for r in tier_5),
        },
        "customers": results,
    }


def update_customer_codes(results: dict, customer_master: dict) -> dict:
    for cust in results["customers"]:
        if not cust["customer_code"] and cust["customer_name"] in customer_master:
            cust["customer_code"] = customer_master[cust["customer_name"]]
    return results
