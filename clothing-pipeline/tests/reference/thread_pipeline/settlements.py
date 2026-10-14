"""Compare channel statements with collected/refunded money and actual bank transfers."""

from collections import defaultdict
from decimal import Decimal

from .contracts import ContractError, timestamp


def integrate_settlements(events, orders, payments, pending, checks, stable_id):
    headers, lines, transfers = {}, {}, {}
    reconciliation = []
    cash = defaultdict(lambda: {"gross": Decimal("0"), "refunds": Decimal("0")})
    for p in payments.values():
        if p["status"] == "succeeded":
            key = (p["order_source"], p["order_scope"], p["order"], p["currency"])
            cash[key]["gross" if p["kind"] == "charge" else "refunds"] += Decimal(p["amount"])

    def base(e):
        return dict(source=e["source"], scope=e["scope"], event_id=e["event_id"], source_version=e["source_version"],
                    raw_file=e["provenance"]["file"], batch=e["provenance"]["batch"], occurred_at=e["occurred_at"])

    for e in events:
        p = e["payload"]
        if e["type"] == "settlement":
            key = e["source"], e["scope"], p["settlement"]
            headers[key] = dict(**base(e), settlement_id=stable_id("SET-", key), **p)
    seen_orders = set()
    for e in events:
        p = e["payload"]
        if e["type"] != "settlement_line":
            continue
        key = e["source"], e["scope"], p["settlement"]
        order_key = p["order_source"], p["order_scope"], p["order"]
        parent, order = headers.get(key), orders.get(order_key)
        if parent is None or order is None:
            pending.append(dict(event=e, reason="missing settlement header or order"))
            continue
        if parent["currency"] != p["currency"] or order["currency"] != p["currency"] or parent["period_start"] != p["period_start"] or parent["period_end"] != p["period_end"]:
            checks.append(dict(check="settlement_identity", result="failed", event_id=e["event_id"]))
            continue
        duplicate = order_key
        if duplicate in seen_orders:
            raise ContractError("settlement contains multiple full-order lines for the same order")
        seen_orders.add(duplicate)
        line_key = key + (p["settlement_line"],)
        row = dict(**base(e), settlement_id=parent["settlement_id"], settlement_line_id=stable_id("SETL-", line_key),
                   order_id=order["order_id"], **p)
        lines[line_key] = row
        actual = cash[order_key + (p["currency"],)]
        for metric in ("gross", "refunds"):
            expected = Decimal(p[metric])
            delta = actual[metric] - expected
            comparison = dict(settlement_id=parent["settlement_id"], order_id=order["order_id"], check=metric,
                              expected=format(expected, ".2f"), actual=format(actual[metric], ".2f"),
                              delta=format(delta, ".2f"), result="passed" if delta == 0 else "failed")
            reconciliation.append(comparison)
            if delta:
                checks.append(dict(check="settlement_" + metric, result="failed", event_id=e["event_id"], delta=format(delta, ".2f")))
    for e in events:
        p = e["payload"]
        if e["type"] != "payout_transfer":
            continue
        key = p["settlement_source"], p["settlement_scope"], p["settlement"]
        parent = headers.get(key)
        if parent is None:
            pending.append(dict(event=e, reason="missing payout settlement"))
            continue
        if p["currency"] != parent["currency"]:
            checks.append(dict(check="payout_currency", result="failed", event_id=e["event_id"]))
            continue
        transfers[(e["source"], e["scope"], p["transaction"])] = dict(**base(e), settlement_id=parent["settlement_id"], **p)
    for key, h in sorted(headers.items()):
        entries = [row for row in lines.values() if row["settlement_id"] == h["settlement_id"]]
        if len(entries) != h["line_count"]:
            checks.append(dict(check="settlement_line_count", result="waiting" if len(entries) < h["line_count"] else "failed", settlement_id=h["settlement_id"]))
        for metric in ("gross", "refunds", "fees", "payout"):
            total = sum((Decimal(row[metric]) for row in entries), Decimal("0"))
            if total != Decimal(h[metric]):
                checks.append(dict(check="settlement_total_" + metric, result="waiting" if len(entries) < h["line_count"] else "failed", settlement_id=h["settlement_id"]))
        bank = sum((Decimal(p["amount"]) for p in transfers.values() if p["settlement_id"] == h["settlement_id"] and p["status"] == "succeeded"), Decimal("0"))
        expected = Decimal(h["payout"])
        # A negative settlement is a carry/debt, not an incoming bank transfer.
        status = "carry" if expected < 0 else "passed" if bank == expected else "waiting" if bank < expected else "failed"
        reconciliation.append(dict(settlement_id=h["settlement_id"], order_id=None, check="bank_payout", expected=format(expected, ".2f"),
                                   actual=format(bank, ".2f"), delta=format(bank - expected, ".2f"), result=status))
        if status in ("waiting", "failed"):
            checks.append(dict(check="bank_payout", result=status, settlement_id=h["settlement_id"]))
    return dict(settlements=list(headers.values()), settlement_lines=list(lines.values()), payout_transfers=list(transfers.values()),
                settlement_reconciliation=reconciliation)
