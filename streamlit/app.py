"""CDN operations dashboard - Streamlit in Snowflake.

Reads only the dbt marts (role CDN_REPORTER). Ratios are recomputed from
summed counts, never averaged, exactly as in the marts.

Run inside Snowflake: Snowsight > Streamlit > + Streamlit App, paste this file.
Run locally on the offline DuckDB build (make local-build first):
    CDN_DASHBOARD_DUCKDB=data/cdn_local.duckdb streamlit run streamlit/app.py
"""

from __future__ import annotations

import os

import altair as alt
import pandas as pd
import streamlit as st

DUCKDB_PATH = os.getenv("CDN_DASHBOARD_DUCKDB")
MARTS = "core_marts" if DUCKDB_PATH else "ANALYTICS.MARTS"
SLO_MAX_5XX = 0.01

st.set_page_config(page_title="CDN operations", layout="wide")


# --------------------------------------------------------------------- data
def run_query(sql: str) -> pd.DataFrame:
    if DUCKDB_PATH:
        import duckdb

        with duckdb.connect(DUCKDB_PATH, read_only=True) as con:
            df = con.sql(sql).df()
    else:
        from snowflake.snowpark.context import get_active_session

        df = get_active_session().sql(sql).to_pandas()
    df.columns = [c.lower() for c in df.columns]
    return df


@st.cache_data(ttl=600)
def load_hourly() -> pd.DataFrame:
    df = run_query(
        f"""
        select request_hour, pop_code, content_category, request_count,
               cacheable_request_count, cache_hit_count, server_error_count, bytes_served
        from {MARTS}.fct_cdn_hourly_metrics
        """
    )
    df["request_hour"] = pd.to_datetime(df["request_hour"])
    return df


@st.cache_data(ttl=600)
def load_daily() -> pd.DataFrame:
    df = run_query(
        f"""
        select date_day, pop_code, pop_name, region, request_count, gib_served,
               cache_hit_ratio, server_error_rate, worst_hourly_p95_ms,
               hours_breaching_slo, is_slo_met
        from {MARTS}.rpt_pop_daily_health
        """
    )
    df["date_day"] = pd.to_datetime(df["date_day"]).dt.date
    return df


def ratio(num: pd.Series, den: pd.Series) -> pd.Series:
    return num / den.where(den != 0)


hourly = load_hourly()
daily = load_daily()

if hourly.empty:
    st.warning("No data in the marts yet: run the Airflow DAG first.")
    st.stop()

# ------------------------------------------------------------------ filters
first_day = hourly["request_hour"].min().date()
last_day = hourly["request_hour"].max().date()
pops = sorted(hourly["pop_code"].unique())

with st.sidebar:
    st.header("Filters")
    picked = st.date_input("Date range (UTC)", (first_day, last_day), first_day, last_day)
    start_day, end_day = (picked[0], picked[-1]) if isinstance(picked, tuple) else (picked, picked)
    selected_pops = st.multiselect("Edge POPs", pops, default=pops)
    st.caption(f"Data through {hourly['request_hour'].max():%Y-%m-%d %H:00} UTC")

if not selected_pops:
    st.info("Select at least one POP.")
    st.stop()

h = hourly[
    hourly["request_hour"].dt.date.between(start_day, end_day) & hourly["pop_code"].isin(selected_pops)
]
d = daily[daily["date_day"].between(start_day, end_day) & daily["pop_code"].isin(selected_pops)]

# --------------------------------------------------------------------- KPIs
st.title("CDN operations")
st.caption("Edge performance from the dbt marts - refreshed hourly by Airflow")

requests = int(h["request_count"].sum())
hit_ratio = h["cache_hit_count"].sum() / max(h["cacheable_request_count"].sum(), 1)
err_rate = h["server_error_count"].sum() / max(requests, 1)
breaches = int(d["hours_breaching_slo"].sum())

k1, k2, k3, k4 = st.columns(4)
k1.metric("Requests", f"{requests:,}")
k2.metric("Cache hit ratio", f"{hit_ratio:.1%}")
k3.metric("5xx rate", f"{err_rate:.2%}")
k4.metric("POP-hours breaching SLO", breaches, help="5xx >= 1% or p95 >= 500 ms in that hour")

# ------------------------------------------------------------------- charts
by_pop = (
    h.groupby(["request_hour", "pop_code"], as_index=False)[
        ["request_count", "cacheable_request_count", "cache_hit_count", "server_error_count"]
    ]
    .sum()
    .assign(
        cache_hit_ratio=lambda x: ratio(x["cache_hit_count"], x["cacheable_request_count"]),
        server_error_rate=lambda x: ratio(x["server_error_count"], x["request_count"]),
    )
)

hour_axis = alt.X("request_hour:T", title=None, axis=alt.Axis(format="%d %b %Hh"))

st.subheader("5xx rate by POP and hour - SLO < 1%")
heat = by_pop.assign(hour_end=by_pop["request_hour"] + pd.to_timedelta(1, unit="h"))
st.altair_chart(
    alt.Chart(heat)
    .mark_rect()
    .encode(
        hour_axis,
        x2="hour_end:T",
        y=alt.Y("pop_code:N", title=None, sort=pops),
        color=alt.Color(
            "server_error_rate:Q",
            title="5xx rate",
            scale=alt.Scale(
                type="threshold",
                domain=[SLO_MAX_5XX / 2, SLO_MAX_5XX],
                range=["#d9ead3", "#f6b26b", "#cc0000"],
            ),
            legend=alt.Legend(format=".1%"),
        ),
        tooltip=[
            alt.Tooltip("request_hour:T", title="Hour (UTC)", format="%d %b %H:00"),
            alt.Tooltip("pop_code:N", title="POP"),
            alt.Tooltip("server_error_rate:Q", title="5xx rate", format=".2%"),
            alt.Tooltip("request_count:Q", title="Requests", format=","),
        ],
    )
    .properties(height=260),
    use_container_width=True,
)
st.caption("Green < 0.5% - orange 0.5-1% - red >= 1% (SLO breach). Red cells are the simulated incidents.")

left, right = st.columns(2)

with left:
    st.subheader("Cache hit ratio by POP")
    by_pop_total = (
        h.groupby("pop_code", as_index=False)[["cache_hit_count", "cacheable_request_count"]]
        .sum()
        .assign(cache_hit_ratio=lambda x: ratio(x["cache_hit_count"], x["cacheable_request_count"]))
    )
    # Dot plot on a zoomed axis: POPs sit within a few points of each other,
    # so bars from zero would all look identical.
    dots = alt.Chart(by_pop_total).encode(
        x=alt.X(
            "cache_hit_ratio:Q",
            title=None,
            axis=alt.Axis(format=".1%", tickCount=4),
            scale=alt.Scale(zero=False),
        ),
        y=alt.Y("pop_code:N", title=None, sort="-x"),
        tooltip=["pop_code", alt.Tooltip("cache_hit_ratio:Q", format=".1%")],
    )
    st.altair_chart(
        dots.mark_point(filled=True, size=140, color="#3d85c6")
        + dots.mark_text(align="left", dx=10).encode(text=alt.Text("cache_hit_ratio:Q", format=".1%")),
        use_container_width=True,
    )
    st.caption(f"Overall {hit_ratio:.1%} - zoomed axis")

with right:
    st.subheader("Traffic by content category")
    by_category = h.groupby(["request_hour", "content_category"], as_index=False)["request_count"].sum()
    st.altair_chart(
        alt.Chart(by_category)
        .mark_area()
        .encode(
            hour_axis,
            alt.Y("request_count:Q", title="Requests per hour", stack=True),
            alt.Color("content_category:N", title="Content", scale=alt.Scale(scheme="set2")),
            tooltip=["request_hour:T", "content_category", "request_count:Q"],
        ),
        use_container_width=True,
    )

# -------------------------------------------------------------- SLO report
st.subheader("Daily SLO report")
report = d.sort_values(["is_slo_met", "date_day", "pop_code"], ascending=[True, False, True]).rename(
    columns={
        "date_day": "Day",
        "pop_code": "POP",
        "pop_name": "City",
        "request_count": "Requests",
        "gib_served": "GiB served",
        "cache_hit_ratio": "Cache hit",
        "server_error_rate": "5xx rate",
        "worst_hourly_p95_ms": "Worst p95 (ms)",
        "hours_breaching_slo": "Hours off SLO",
        "is_slo_met": "SLO met",
    }
)
st.dataframe(
    report.drop(columns=["region"]).style.format(
        {
            "Requests": "{:,.0f}",
            "GiB served": "{:.2f}",
            "Cache hit": "{:.1%}",
            "5xx rate": "{:.2%}",
            "Worst p95 (ms)": "{:,.0f}",
            "Hours off SLO": "{:.0f}",
        }
    ),
    use_container_width=True,
    hide_index=True,
)
