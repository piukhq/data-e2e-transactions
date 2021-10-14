import os

import pandas as pd
import psycopg2

pg_host = os.getenv("PG_HOST")
pg_user = os.getenv("PG_USER")
pg_pass = os.getenv("PG_PASS")

pd.options.mode.chained_assignment = None  # default='warn'


def atlas(df, merchant):
    if len(df) == 0:
        return df
    if merchant == "harvey-nichols":
        response = "audit_data->'response'->'body'->'CustomerClaimTransactionResponse'->>'outcome' atlas_response"
    if merchant == "wasabi-club":
        response = "audit_data->'response'->'body'->>'Message' atlas_response"
    conn = psycopg2.connect(host=pg_host, database="atlas", user=pg_user, password=pg_pass, sslmode="require")
    atlas_query = f"""
        SELECT
            transaction_id,
            {response},
            MIN(created_date)
        FROM
            transactions_exporttransaction
        LEFT JOIN
            transactions_auditdata ON transactions_exporttransaction.audit_data_id = transactions_auditdata.id
        WHERE
            provider_slug = \'{merchant}\'
        GROUP BY
            transaction_id,
            atlas_response
    """
    atlas = pd.read_sql(atlas_query, conn).drop(["min"], axis=1)

    df = pd.merge(left=df, right=atlas, left_on="Transaction ID", right_on="transaction_id", how="left").drop(
        ["transaction_id"], axis=1
    )

    df.insert(df.columns.get_loc("atlas_response"), "Atlas", pd.notna(df["atlas_response"]))

    return df


def matched(df):
    conn = psycopg2.connect(host=pg_host, database="harmonia", user=pg_user, password=pg_pass, sslmode="require")

    matched_query = """
        SELECT
            transaction_id,
            created_at matched_created_date,
            status
        FROM
            matched_transaction
    """
    matched_tr = pd.read_sql(matched_query, conn)

    df = pd.merge(left=df, right=matched_tr, left_on="Transaction ID", right_on="transaction_id", how="left").drop(
        ["transaction_id"], axis=1
    )

    df.insert(df.columns.get_loc("matched_created_date"), "Matched", pd.notna(df["status"]))

    return df


def tlog(df, merchant):
    if len(df) == 0:
        return df
    max_date = str(pd.to_datetime(df["Date"]).max())
    min_date = str(pd.to_datetime(df["Date"]).min())
    timestamp = ""
    where = ""
    conn = psycopg2.connect(host=pg_host, database="harmonia", user=pg_user, password=pg_pass, sslmode="require")

    if merchant == "harvey-nichols":
        # max_date = max_date[0:max_date.find(' ')]
        # min_date = min_date[0:min_date.find(' ')]
        timestamp = "data->>'timestamp' TLOG_timestamp"
        where = f"((data->>'timestamp')::DATE >= '{min_date}' AND (data->>'timestamp')::DATE <= '{max_date}')"
    if merchant == "wasabi-club":
        timestamp = "data->>'Date' AS TLOG_date,\ndata->>'Time' AS TLOG_time"
        where = f"""
        (to_date(data->>'Date','DD/MM/YYYY') >= '{min_date}'
        AND to_date(data->>'Date','DD/MM/YYYY') <= '{max_date}')
        """
    df_tlog_query = f"""
            SELECT
                transaction_id,
                created_at TLOG_created_date,
                {timestamp}
            FROM
                import_transaction
            WHERE
                provider_slug = \'{merchant}\' AND
                {where}
    """
    df_tlog = pd.read_sql(df_tlog_query, conn)

    df = pd.merge(left=df, right=df_tlog, left_on="Transaction ID", right_on="transaction_id", how="left").drop(
        ["transaction_id"], axis=1
    )

    df.insert(df.columns.get_loc("tlog_created_date"), "T Log", pd.notna(df["tlog_created_date"]))

    return df


def ord(n):
    return str(n) + ("th" if 4 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"))


def wasabi(df):
    df = df[pd.to_datetime(df["Completion time"]).dt.isocalendar().week == (pd.Timestamp.today().week - 1)]
    df = df[
        [
            "Select your ID",
            "Card Type",
            "Payment Method",
            (
                "Receipt number - AS PRINTED ON RECEIPT\nPlease consult the annotated receipts you were provided "
                "with separately to locate the receipt number."
            ),
            (
                "Store *\nPlease input the store address as found on the receipt. Consult the annotated receipts "
                "you were provided with separately to locate the transaction ID."
            ),
            "Payment amount - AS PRINTED ON RECEIPT\nEnter only the number, including decimals (no £ sign).",
            "Date - AS PRINTED ON RECEIPT",
            "Time - AS PRINTED ON RECEIPT",
        ]
    ]
    df.columns = [
        "Tester ID",
        "Card Type",
        "Payment Method",
        "Transaction ID",
        "Store",
        "Payment amount",
        "Date",
        "Time",
    ]
    mids = pd.read_csv("Wasabi MIDs.csv", dtype={"Amex": str, "Visa": str, "Mastercard": str})
    mids = pd.melt(mids, id_vars=["Data Management"], var_name="Payment Scheme", value_name="MID")
    df = pd.merge(
        left=df, right=mids, how="left", left_on=["Store", "Card Type"], right_on=["Data Management", "Payment Scheme"]
    ).drop(["Data Management", "Payment Scheme"], axis=1)
    df = df[
        ["Transaction ID", "Card Type", "Payment Method", "Store", "MID", "Payment amount", "Date", "Time", "Tester ID"]
    ]

    return df


def hniceland(df):
    df = df[pd.to_datetime(df["Completion time"]).dt.isocalendar().week == (pd.Timestamp.today().week - 1)]
    df.columns = [
        "ID",
        "Start time",
        "Completion time",
        "Email",
        "Name",
        "OccupierID",
        "Tester ID",
        "Please upload a photo of your receipt",
        "Merchant",
        "Card Type",
        "Payment Method",
        "MID",
        "Payment amount",
        "Date",
        "Time",
        "Auth code",
        "Transaction ID",
    ]
    df = df[
        [
            "Transaction ID",
            "Merchant",
            "Card Type",
            "Payment Method",
            "MID",
            "Payment amount",
            "Date",
            "Time",
            "Auth code",
            "Tester ID",
        ]
    ]
    hn = df[df["Merchant"] == "Harvey Nichols"]
    iceland = df[df["Merchant"] == "Iceland "]
    return hn, iceland


def iceland(df):
    conn = psycopg2.connect(host=pg_host, database="harmonia", user=pg_user, password=pg_pass, sslmode="require")
    query = """
        SELECT
            transaction_id,
            transaction_date::DATE,
            spend_amount,
            mid,
            payment_provider.slug payment_scheme
        FROM
            matched_transaction
        INNER JOIN
            merchant_identifier ON matched_transaction.merchant_identifier_id = merchant_identifier.id
        INNER JOIN
            loyalty_scheme ON merchant_identifier.loyalty_scheme_id = loyalty_scheme.id
        INNER JOIN
            payment_provider ON merchant_identifier.payment_provider_id = payment_provider.id
        WHERE
            loyalty_scheme.slug = \'iceland-bonus-card\'
        """

    tr_df = pd.read_sql(query, conn)
    tr_df["mid"] = tr_df["mid"].str[-5:]
    tr_df["transaction_id"] = tr_df["transaction_id"].str[3:7]
    tr_df["transaction_date"] = pd.to_datetime(tr_df["transaction_date"])

    df["ID"] = df["MID"].astype(str) + df["Auth code"].astype(str) + df["Transaction ID"].astype(str)
    df["Payment amount"] = df["Payment amount"] * 100
    df["Payment amount"] = df["Payment amount"].astype(int)
    df["Card Type"] = df["Card Type"].str.lower()
    df["Transaction ID"] = df["Transaction ID"].apply(
        lambda x: "0" + x if len(x) == 3 else "00" + x if len(x) == 2 else "000" + x if len(x) == 1 else x
    )

    df = pd.merge(
        df,
        tr_df,
        how="left",
        left_on=["Transaction ID", "Card Type", "Date", "Payment amount", "MID"],
        right_on=["transaction_id", "payment_scheme", "transaction_date", "spend_amount", "mid"],
    )

    df["Matched"] = pd.notna(df["transaction_id"])
    df = df.drop(["transaction_id", "payment_scheme", "transaction_date", "spend_amount", "mid"], axis=1)
    df["Payment amount"] = df["Payment amount"] / 100
    df = df[
        [
            "ID",
            "Merchant",
            "Card Type",
            "Payment Method",
            "MID",
            "Date",
            "Time",
            "Payment amount",
            "Auth code",
            "Transaction ID",
            "Tester ID",
            "Matched",
        ]
    ]
    return df
