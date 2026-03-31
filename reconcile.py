import pandas as pd

EXCEL_FILE        = r"C:\Users\Kanishka Mittal\Downloads\payments_datasets.xlsx"
TXN_SHEET         = "Transactions"
SET_SHEET         = "Settlements"
ROUNDING_THRESHOLD = 1.0   

def load_data(filepath):
    txn_df = pd.read_excel(filepath, sheet_name=TXN_SHEET, header=2)
    set_df = pd.read_excel(filepath, sheet_name=SET_SHEET, header=2)

    
    txn_df.columns = txn_df.columns.str.strip()
    set_df.columns = set_df.columns.str.strip()

    txn_df = txn_df[
        txn_df["Transaction ID"].notna() &
        (txn_df["Transaction ID"].astype(str) != "TOTAL")
    ].copy()

    set_df = set_df[
        set_df["Settlement ID"].notna() &
        (set_df["Settlement ID"].astype(str) != "TOTAL")
    ].copy()

    txn_df["Amount (₹)"]          = pd.to_numeric(txn_df["Amount (₹)"],          errors="coerce")
    set_df["Settled Amount (₹)"]  = pd.to_numeric(set_df["Settled Amount (₹)"],  errors="coerce")

    print(f"Loaded {len(txn_df)} transactions  |  {len(set_df)} settlements\n")
    return txn_df, set_df


def detect_duplicates(txn_df):
   
    issues = []

    dup_mask = txn_df.duplicated(subset=["Transaction ID", "Amount (₹)"], keep=False)
    dup_ids  = txn_df.loc[dup_mask, "Transaction ID"].unique()

    for txn_id in dup_ids:
        group = txn_df[txn_df["Transaction ID"] == txn_id]
        issues.append({
            "txn_id"          : str(txn_id),
            "description"     : str(group["Description"].iloc[0]),
            "occurrences"     : int(len(group)),
            "amount_each"     : float(group["Amount (₹)"].iloc[0]),
            "total_in_txns"   : float(group["Amount (₹)"].sum()),
        })

    clean_df = txn_df.drop_duplicates(subset=["Transaction ID", "Amount (₹)"], keep="first").copy()

    return issues, clean_df

def detect_late_settlements(txn_df, set_df):
  
    txn_ids = set(txn_df["Transaction ID"].astype(str))
    set_refs = set(set_df["Transaction Ref"].astype(str))

    missing_ids = txn_ids - set_refs
    issues = []

    for tid in missing_ids:
        row = txn_df[txn_df["Transaction ID"].astype(str) == tid].iloc[0]
        if row["Type"] == "DEBIT":
            issues.append({
                "txn_id"      : str(tid),
                "date"        : str(row["Date"])[:10],
                "description" : str(row["Description"]),
                "amount"      : float(row["Amount (₹)"]),
                "status"      : str(row["Status"]),
            })

    return issues


def detect_rounding_differences(txn_df, set_df):
    
    txn_ids  = set(txn_df["Transaction ID"].astype(str))
    set_refs = set(set_df["Transaction Ref"].astype(str))
    matched_ids = txn_ids & set_refs

    issues = []

    for tid in matched_ids:
        txn_row = txn_df[txn_df["Transaction ID"].astype(str) == tid]
        set_row = set_df[set_df["Transaction Ref"].astype(str) == tid]

        if txn_row.empty or set_row.empty:
            continue

        txn_amt = float(txn_row["Amount (₹)"].iloc[0])
        set_amt = float(set_row["Settled Amount (₹)"].iloc[0])
        diff    = round(txn_amt - set_amt, 6)

        if diff != 0 and abs(diff) < ROUNDING_THRESHOLD:
            issues.append({
                "txn_id"         : str(tid),
                "description"    : str(txn_row["Description"].iloc[0]),
                "txn_amount"     : txn_amt,
                "settled_amount" : set_amt,
                "difference"     : diff,
            })

    return issues


def detect_orphan_refunds(txn_df):

    refunds = txn_df[txn_df["Amount (₹)"] < 0].copy()
    debits  = txn_df[txn_df["Amount (₹)"] > 0].copy()

    issues = []

    for _, refund_row in refunds.iterrows():
        
        refund_desc = str(refund_row["Description"]).lower()
        matched = debits[
            debits["Description"].str.lower().str.contains(
                "|".join([w for w in refund_desc.split() if len(w) > 3]),
                na=False
            )
        ]

        if matched.empty:
            issues.append({
                "txn_id"         : str(refund_row["Transaction ID"]),
                "description"    : str(refund_row["Description"]),
                "refund_amount"  : float(refund_row["Amount (₹)"]),
                "date"           : str(refund_row["Date"])[:10],
                "original_found" : False,
                "note"           : "No originating debit transaction found in system",
            })

    return issues


def find_clean_matches(txn_df, set_df):
    """Returns list of txn IDs that matched perfectly (same ID, same amount)."""
    txn_ids  = set(txn_df["Transaction ID"].astype(str))
    set_refs = set(set_df["Transaction Ref"].astype(str))
    matched_ids = txn_ids & set_refs

    clean = []
    for tid in matched_ids:
        txn_row = txn_df[txn_df["Transaction ID"].astype(str) == tid]
        set_row = set_df[set_df["Transaction Ref"].astype(str) == tid]
        if txn_row.empty or set_row.empty:
            continue
        txn_amt = float(txn_row["Amount (₹)"].iloc[0])
        set_amt = float(set_row["Settled Amount (₹)"].iloc[0])
        if round(txn_amt - set_amt, 6) == 0:
            clean.append({"txn_id": str(tid), "amount": txn_amt})

    return clean


def print_report(txn_df, set_df, duplicates, late, rounding, orphans, clean):
    txn_total = txn_df["Amount (₹)"].sum()
    set_total = set_df["Settled Amount (₹)"].sum()
    net_gap   = txn_total - set_total

    SEP = "─" * 60

    print(SEP)
    print("  PAYMENTS RECONCILIATION REPORT — January 2026")
    print(SEP)
    print(f"  Transactions total   : ₹{txn_total:>12,.3f}  ({len(txn_df)} records)")
    print(f"  Settlements total    : ₹{set_total:>12,.3f}  ({len(set_df)} records)")
    print(f"  NET DISCREPANCY      : ₹{net_gap:>12,.3f}")
    print(SEP)

  
    print(f"\n  [1] DUPLICATES  ({len(duplicates)} found)")
    print(f"  {'─'*56}")
    if duplicates:
        for d in duplicates:
            disc = d['total_in_txns'] - d['amount_each']
            print(f"  {d['txn_id']:<16}  {d['description']:<28}")
            print(f"    Appears {d['occurrences']}× in transactions → ₹{d['total_in_txns']:,.2f}")
            print(f"    Bank should settle for ₹{d['amount_each']:,.2f} (1×)")
            print(f"    Discrepancy: ₹{disc:,.2f}")
    else:
        print("  No duplicates found.")


    late_total = sum(x['amount'] for x in late)
    print(f"\n  [2] LATE SETTLEMENTS  ({len(late)} found · ₹{late_total:,.2f} unmatched)")
    print(f"  {'─'*56}")
    if late:
        for l in late:
            print(f"  {l['txn_id']:<16}  {l['description']:<28}  ₹{l['amount']:>10,.2f}")
            print(f"    Date: {l['date']}  |  Status: {l['status']}")
            print(f"    → No matching settlement found in Jan. Likely settles in Feb.")
    else:
        print("  All transactions settled on time.")


    round_total = sum(x['difference'] for x in rounding)
    print(f"\n  [3] ROUNDING DIFFERENCES  ({len(rounding)} found · net ₹{round_total:,.4f})")
    print(f"  {'─'*56}")
    if rounding:
        for r in rounding:
            print(f"  {r['txn_id']:<16}  {r['description']:<28}")
            print(f"    Platform: ₹{r['txn_amount']:<12}  Bank: ₹{r['settled_amount']:<12}  Diff: ₹{r['difference']:+.4f}")
    else:
        print("  No rounding differences found.")

    orphan_total = sum(x['refund_amount'] for x in orphans)
    print(f"\n  [4] ORPHAN REFUNDS  ({len(orphans)} found · ₹{orphan_total:,.2f})")
    print(f"  {'─'*56}")
    if orphans:
        for o in orphans:
            print(f"  {o['txn_id']:<16}  {o['description']:<28}  ₹{o['refund_amount']:>10,.2f}")
            print(f"    Date: {o['date']}")
            print(f"    ⚠  {o['note']}")
    else:
        print("  No orphan refunds found.")

    print(f"\n{SEP}")
    print("  SUMMARY")
    print(f"  {'─'*56}")
    print(f"  ✓  Clean matches       : {len(clean):>3} transactions")
    print(f"  !  Late settlements    : {len(late):>3} transactions   ₹{late_total:>10,.2f}")
    print(f"  ✗  Duplicates          : {len(duplicates):>3} transactions   ₹{sum(d['total_in_txns']-d['amount_each'] for d in duplicates):>10,.2f}")
    print(f"  ~  Rounding diffs      : {len(rounding):>3} transactions   ₹{round_total:>10,.4f}")
    print(f"  ⚠  Orphan refunds      : {len(orphans):>3} transactions   ₹{orphan_total:>10,.2f}")
    print(f"\n  Total unreconciled    :              ₹{net_gap:>10,.2f}")
    print(SEP)

if __name__ == "__main__":

    txn_df, set_df = load_data(EXCEL_FILE)

    duplicates, txn_clean = detect_duplicates(txn_df)
    late                  = detect_late_settlements(txn_clean, set_df)
    rounding              = detect_rounding_differences(txn_clean, set_df)
    orphans               = detect_orphan_refunds(txn_clean)
    clean                 = find_clean_matches(txn_clean, set_df)

    print_report(txn_df, set_df, duplicates, late, rounding, orphans, clean)
