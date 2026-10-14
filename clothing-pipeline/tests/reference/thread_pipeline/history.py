"""Seeded multi-market history with independently accumulated business totals."""

import hashlib
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from .contracts import canonical_json
from .sample import endpoint, event


def generate_history(root, days=30, orders_per_day=500, sku_count=500, seed=42, daily_snapshots=False):
    if any(type(n) is not int or n <= 0 for n in (days, orders_per_day, sku_count)):
        raise ValueError("days, orders_per_day and sku_count must be positive integers")
    root = Path(root)
    if root.exists():
        raise FileExistsError("input destination already exists")
    rng = random.Random(seed)
    events, mappings = [], []
    business_movements = []
    purchase_expected = []
    statement_orders = {}
    stock, cash = defaultdict(int), defaultdict(lambda: Decimal("0"))
    line_count = 0
    locations = {"VN-WH-01": "LOC-0001", "VN-ST-01": "LOC-0002", "VN-ST-02": "LOC-0003", "VN-ST-03": "LOC-0004",
                 "SG-WH-01": "LOC-0005", "SG-ST-01": "LOC-0006", "SG-ST-02": "LOC-0007"}
    markets = {"VN": (7, "VND", ["VN-ST-01", "VN-ST-02", "VN-ST-03"]),
               "SG": (8, "SGD", ["SG-ST-01", "SG-ST-02"])}
    scopes = [("website", "vn_web"), ("website", "sg_web"), ("marketplace", "vn_shop"), ("marketplace", "sg_shop"),
              ("wms", "vn_inventory"), ("wms", "sg_inventory"), ("oms", "brand_oms"), ("procurement", "brand_procurement")]
    scopes += [("pos", location.lower()) for location in locations if "ST" in location]
    for i in range(sku_count):
        code = "TS%03d-C%d-S%d" % (i // 10 + 1, (i % 10) // 5, i % 5)
        sid = "SKU-%04d" % (i + 1)
        for source, scope in scopes:
            source_code = source.upper() + ":" + code
            mappings.append(dict(source=source, scope=scope, code=source_code, canonical_id=sid,
                                 valid_from="2026-01-01T00:00:00Z", valid_to=None))

    def emit(source, scope, eid, event_type, time, **payload):
        events.append(event(source, scope, eid, event_type, time.isoformat(), **payload))

    def move(eid, market, time, sid, code, qty, origin, destination, reference, document_type=None, document_id=None):
        document = {} if document_type is None else dict(document_type=document_type, document_source="wms",
                                                       document_scope=market.lower() + "_inventory", document_id=document_id)
        emit("wms", market.lower() + "_inventory", eid, "stock_move", time,
             sku="WMS:" + code, quantity=qty, reference=reference, **document, **{"from": origin, "to": destination})
        business_movements.append((time, market, sid, code, qty, origin, destination))

    def purchase(tag, market, time, sid, code, quantity, location, outstanding=0):
        scope = market.lower() + "_inventory"
        po, pol = tag + "-PO", tag + "-POL"
        purchase_expected.append(dict(source="procurement", scope="brand_procurement", source_purchase_line=pol,
                                      quantity=quantity + outstanding, received_quantity=quantity, outstanding_quantity=outstanding,
                                      accepted_quantity=quantity, defective_quantity=0, uninspected_quantity=0, over_received_quantity=0))
        currency = markets[market][1]
        emit("procurement", "brand_procurement", po, "purchase_order_line", time - timedelta(days=1),
             purchase_order=po, purchase_line=pol, supplier="SUP-" + market, sku="PROCUREMENT:" + code,
             location=location, quantity=quantity + outstanding, unit_cost="100000.00" if market == "VN" else "12.00",
             currency=currency, expected_at=time.isoformat())
        for part, qty in enumerate((quantity // 2, quantity - quantity // 2)):
            received_at = time + timedelta(seconds=part * 10)
            receipt, inspection = tag + "-RCV%d" % part, tag + "-QC%d" % part
            emit("wms", scope, receipt + "-DOC", "purchase_receipt", received_at, receipt=receipt,
                 purchase_source="procurement", purchase_scope="brand_procurement", purchase_line=pol,
                 sku="WMS:" + code, location=location, quantity=qty)
            move(receipt, market, received_at, sid, code, qty, endpoint("external:supplier"), endpoint(location, "inspection"),
                 pol, "purchase_receipt", receipt)
            emit("wms", scope, inspection + "-DOC", "purchase_inspection", received_at + timedelta(seconds=5),
                 inspection=inspection, receipt_source="wms", receipt_scope=scope, receipt=receipt,
                 sku="WMS:" + code, location=location, quantity=qty, condition="sellable")
            move(inspection, market, received_at + timedelta(seconds=5), sid, code, qty,
                 endpoint(location, "inspection"), endpoint(location), receipt, "purchase_inspection", inspection)

    start = datetime(2026, 9, 1)
    # Seed physical stock before sales. Oracle is updated from business actions, not by parsing events.
    for market, (offset, _, stores) in markets.items():
        time = (start - timedelta(days=1)).replace(hour=8, tzinfo=timezone(timedelta(hours=offset)))
        warehouse = market + "-WH-01"
        for i in range(sku_count):
            sid, code = "SKU-%04d" % (i + 1), "TS%03d-C%d-S%d" % (i // 10 + 1, (i % 10) // 5, i % 5)
            qty = max(100, (days * orders_per_day * 3 // sku_count) + 50)
            total = qty * (len(stores) + 1)
            purchase("INIT-%s-%d" % (market, i), market, time, sid, code, total, warehouse, outstanding=20)
            stock[(sid, locations[warehouse], "sellable")] += total
            for store in stores:
                transfer = "INIT-%s-%d" % (store, i)
                move(transfer + "-OUT", market, time + timedelta(minutes=1), sid, code, qty,
                     endpoint(warehouse), endpoint("transit:" + transfer), transfer)
                move(transfer + "-IN", market, time + timedelta(minutes=2), sid, code, qty,
                     endpoint("transit:" + transfer), endpoint(store), transfer)
                stock[(sid, locations[warehouse], "sellable")] -= qty
                stock[(sid, locations[store], "sellable")] += qty
    for day in range(days):
        for sequence in range(orders_per_day):
            number = day * orders_per_day + sequence
            market = "VN" if rng.random() < 0.65 else "SG"
            offset, currency, stores = markets[market]
            channel = rng.choices(["website", "marketplace", "pos"], weights=[40, 35, 25])[0]
            loc = rng.choice(stores) if channel == "pos" else market + "-WH-01"
            scope = loc.lower() if channel == "pos" else market.lower() + ("_web" if channel == "website" else "_shop")
            time = (start + timedelta(days=day)).replace(hour=9 + rng.randrange(11), minute=rng.randrange(60), tzinfo=timezone(timedelta(hours=offset)))
            order = "%s-%s-%06d" % (channel.upper(), market, number)
            ref = dict(order_source=channel, order_scope=scope, order=order)
            amount = Decimal("0")
            returns = []
            for line_no in range(rng.randint(1, 3)):
                i, qty = rng.randrange(sku_count), rng.randint(1, 2)
                sid, code = "SKU-%04d" % (i + 1), "TS%03d-C%d-S%d" % (i // 10 + 1, (i % 10) // 5, i % 5)
                # Restock if needed; never fabricate a sale against unavailable stock.
                if stock[(sid, locations[loc], "sellable")] < qty:
                    replenishment = 100
                    purchase(order + "-RESTOCK-%d" % line_no, market, time - timedelta(minutes=1), sid, code, replenishment, loc)
                    stock[(sid, locations[loc], "sellable")] += replenishment
                line = order + "-L%d" % line_no
                unit = Decimal(rng.choice([200000, 300000, 450000]) if market == "VN" else rng.choice([20, 30, 45]))
                discount = unit * qty * (Decimal("0.10") if rng.random() < 0.2 else Decimal("0"))
                total = unit * qty - discount
                amount += total
                emit(channel, scope, line + "-ORDER", "order", time, order=order, line=line,
                     sku=channel.upper() + ":" + code, quantity=qty, unit_price=format(unit, ".2f"),
                     discount=format(discount, ".2f"), tax="0.00", total=format(total, ".2f"), currency=currency)
                line_count += 1
                if channel != "pos":
                    emit("oms", "brand_oms", line + "-LINK", "order_link", time + timedelta(seconds=1),
                         order="OMS-" + order, line="OMS-" + line, origin_source=channel, origin_scope=scope,
                         origin_order=order, origin_line=line)
                    hold = dict(reservation=line + "-RS", line=line, sku="OMS:" + code, location=loc, quantity=qty, **ref)
                    emit("oms", "brand_oms", line + "-HOLD", "reservation", time + timedelta(seconds=2), action="reserve", **hold)
                    emit("oms", "brand_oms", line + "-CONSUME", "reservation", time + timedelta(minutes=10), action="consume", **hold)
                ship = line + "-SHIP"
                move(line + "-ISSUE", market, time + timedelta(minutes=10), sid, code, qty,
                     endpoint(loc), endpoint("external:customer" if channel == "pos" else "outbound:" + ship), line)
                stock[(sid, locations[loc], "sellable")] -= qty
                delivery_time = time + timedelta(minutes=10) if channel == "pos" else time + timedelta(days=1)
                if channel != "pos":
                    move(line + "-DELIVERED-MOVE", market, delivery_time, sid, code, qty,
                         endpoint("outbound:" + ship), endpoint("external:customer"), ship)
                emit("pos" if channel == "pos" else "carrier", scope if channel == "pos" else market.lower() + "_carrier",
                     line + "-DELIVERED", "delivery", delivery_time, shipment=ship, line=line, quantity=qty, status="delivered", **ref)
                if rng.random() < 0.08:
                    returns.append((line, sid, code, delivery_time + timedelta(days=3), total / qty))
            tx = order + "-PAY"
            pay_scope = market.lower() + "_payment_account"
            if rng.random() < 0.05:
                emit("payment", pay_scope, tx + "-FAILED", "payment", time + timedelta(seconds=3),
                     transaction=tx + "-FAILED", amount=format(amount, ".2f"), currency=currency, kind="charge", status="failed", **ref)
            emit("payment", pay_scope, tx, "payment", time + timedelta(seconds=4),
                 transaction=tx, amount=format(amount, ".2f"), currency=currency, kind="charge", status="succeeded", **ref)
            cash[("charge", currency)] += amount
            if channel == "marketplace":
                statement_orders[(market, order)] = dict(gross=amount, refunds=Decimal("0"), scope=scope, currency=currency)
            for line, sid, code, return_time, refund in returns:
                retline, receipt, inspection = line + "-RETL", line + "-RRC", line + "-RIN"
                emit("oms", "brand_oms", line + "-RETURN-REQUEST", "return_request", return_time - timedelta(days=1),
                     **{"return": line + "-RET", "return_line": retline, "line": line, "sku": "OMS:" + code,
                        "quantity": 1, "reason": "size", **ref})
                emit("wms", market.lower() + "_inventory", receipt + "-DOC", "return_receipt", return_time,
                     receipt=receipt, return_source="oms", return_scope="brand_oms", return_line=retline,
                     sku="WMS:" + code, location=loc, quantity=1)
                move(line + "-RETURN", market, return_time, sid, code, 1,
                     endpoint("external:customer"), endpoint(loc, "inspection"), line + "-RET", "return_receipt", receipt)
                condition = "defective" if rng.random() < 0.15 else "sellable"
                emit("wms", market.lower() + "_inventory", inspection + "-DOC", "return_inspection", return_time + timedelta(hours=1),
                     inspection=inspection, receipt_source="wms", receipt_scope=market.lower() + "_inventory", receipt=receipt,
                     sku="WMS:" + code, location=loc, quantity=1, condition=condition)
                move(line + "-QC", market, return_time + timedelta(hours=1), sid, code, 1,
                     endpoint(loc, "inspection"), endpoint(loc, condition), line + "-RETURN", "return_inspection", inspection)
                stock[(sid, locations[loc], condition)] += 1
                emit("payment", pay_scope, line + "-REFUND", "payment", return_time + timedelta(days=1),
                     transaction=line + "-REFUND", original_transaction=tx, amount=format(refund, ".2f"),
                     currency=currency, kind="refund", refund_reason="return", status="succeeded", **ref)
                emit("payment", pay_scope, line + "-ALLOC", "refund_allocation", return_time + timedelta(days=1),
                     allocation=line + "-ALLOC", transaction=line + "-REFUND", return_source="oms", return_scope="brand_oms",
                     return_line=retline, amount=format(refund, ".2f"), currency=currency)
                cash[("refund", currency)] += refund
                if channel == "marketplace":
                    statement_orders[(market, order)]["refunds"] += refund
    closing_date = max(datetime.fromisoformat(e["occurred_at"]) for e in events) + timedelta(days=1)
    for market, (offset, currency, _) in markets.items():
        selected_orders = [(order, row) for (m, order), row in sorted(statement_orders.items()) if m == market]
        if not selected_orders:
            continue
        closing = closing_date.astimezone(timezone(timedelta(hours=offset)))
        period = dict(period_start=start.replace(tzinfo=timezone(timedelta(hours=offset))).isoformat(), period_end=closing.isoformat())
        settlement = "FINAL-" + market
        scope = market.lower() + "_shop"
        totals = {field: Decimal("0") for field in ("gross", "refunds", "fees", "payout")}
        for order, row in selected_orders:
            fees = (row["gross"] * Decimal("0.05")).quantize(Decimal("0.01"))
            values = dict(gross=row["gross"], refunds=row["refunds"], fees=fees, payout=row["gross"] - row["refunds"] - fees)
            for field, value in values.items():
                totals[field] += value
            emit("marketplace", scope, settlement + "-" + order, "settlement_line", closing,
                 settlement=settlement, settlement_line=order, order_source="marketplace", order_scope=scope, order=order,
                 currency=currency, **period, **{f: format(v, ".2f") for f, v in values.items()})
        emit("marketplace", scope, settlement, "settlement", closing, settlement=settlement, currency=currency,
             line_count=len(selected_orders), **period, **{f: format(v, ".2f") for f, v in totals.items()})
        if totals["payout"] >= 0:
            emit("bank", market.lower() + "_bank", settlement + "-BANK", "payout_transfer", closing + timedelta(hours=1),
                 transaction=settlement + "-BANK", settlement_source="marketplace", settlement_scope=scope, settlement=settlement,
                 amount=format(totals["payout"], ".2f"), currency=currency, status="succeeded")
    if daily_snapshots:
        # Source-side stock book, independent of pipeline parsing/integration.
        source_book = defaultdict(int)
        source_codes = {}
        movements = sorted(business_movements, key=lambda row: row[0])
        cursor = 0
        last_date = max(e["occurred_at"][:10] for e in events)
        last_day = datetime.fromisoformat(last_date)
        boundaries = []
        for market, (offset, _, _) in markets.items():
            date = start
            while date <= last_day:
                boundary = (date + timedelta(days=1)).replace(tzinfo=timezone(timedelta(hours=offset)))
                boundaries.append((boundary, market))
                date += timedelta(days=1)
        for boundary, market in sorted(boundaries):
            while cursor < len(movements) and movements[cursor][0] <= boundary:
                _, _, sid, code, qty, origin, destination = movements[cursor]
                for end, sign in ((origin, -1), (destination, 1)):
                    if end["location"] in locations:
                        key = (sid, end["location"], end["condition"])
                        source_book[key] += sign * qty
                        source_codes[sid] = code
                cursor += 1
            for (sid, loc, condition), qty in sorted(source_book.items()):
                if not loc.startswith(market + "-"):
                    continue
                snap = "SNAP-" + market + "-" + boundary.date().isoformat()
                emit("wms", market.lower() + "_inventory", snap + "-" + sid + "-" + loc + "-" + condition,
                     "inventory_snapshot", boundary, snapshot=snap, sku="WMS:" + source_codes[sid],
                     location=loc, condition=condition, quantity=qty, as_of=boundary.isoformat())
    root.mkdir(parents=True)
    (root / "manifests").mkdir()
    metadata = dict(days=days, orders_per_day=orders_per_day, sku_count=sku_count, seed=seed,
                    start_date="2026-09-01", daily_snapshots=daily_snapshots, note="sales window; deliveries and returns extend beyond it")
    (root / "config.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (root / "catalog.json").write_text(json.dumps(dict(sku_mappings=mappings, locations=locations)), encoding="utf-8")
    expected = dict(order_count=days * orders_per_day, line_count=line_count, reserved=0,
                    money=[dict(kind=k[0], currency=k[1], amount=format(v, ".2f")) for k, v in sorted(cash.items()) if v],
                    inventory=[dict(sku_id=k[0], location=k[1], condition=k[2], quantity=v) for k, v in sorted(stock.items()) if v])
    (root / "expected-history.json").write_text(json.dumps(expected, indent=2), encoding="utf-8")
    purchase_expected.sort(key=lambda row: (row["source"], row["scope"], row["source_purchase_line"]))
    (root / "expected-purchases.json").write_text(json.dumps(purchase_expected, indent=2), encoding="utf-8")
    grouped = defaultdict(list)
    for e in events:
        time = datetime.fromisoformat(e["occurred_at"]).replace(minute=0, second=0, microsecond=0)
        grouped[(e["source"], e["scope"], time.isoformat())].append(e)
    for index, ((source, scope, time), rows) in enumerate(sorted(grouped.items())):
        bid = "H%06d" % index
        relative = Path("raw") / source / scope / bid / "part-001.jsonl"
        (root / relative).parent.mkdir(parents=True)
        content = ("\n".join(canonical_json(row) for row in rows) + "\n").encode("utf-8")
        (root / relative).write_bytes(content)
        start_time = datetime.fromisoformat(time)
        manifest = dict(batch_id=bid, source=source, scope=scope, schema_version=1,
                        data_kind="snapshot_rows" if all(e["type"] == "inventory_snapshot" for e in rows) else "events",
                        period_start=time, period_end=(start_time + timedelta(hours=1)).isoformat(),
                        received_at=(start_time + timedelta(hours=1, minutes=10)).isoformat(), record_count=len(rows),
                        files=[dict(path=relative.as_posix(), sha256=hashlib.sha256(content).hexdigest())])
        (root / "manifests" / (bid + ".json")).write_text(json.dumps(manifest), encoding="utf-8")
    return dict(path=str(root), events=len(events), batches=len(grouped), orders=days * orders_per_day)
