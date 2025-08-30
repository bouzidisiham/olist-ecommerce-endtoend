import os
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Charger le .env automatiquement
load_dotenv()

# --- Connexion DB ---
PG_URL = (
    f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:"
    f"{os.environ['POSTGRES_PASSWORD']}@"
    f"{os.environ['POSTGRES_HOST']}:"
    f"{os.environ['POSTGRES_PORT']}/"
    f"{os.environ['POSTGRES_DB']}"
)

st.set_page_config(page_title="Olist – Delivery & Churn", layout="wide")
st.title("📦 Olist – Delivery Performance & Customer Churn")

@st.cache_data(ttl=300)
def load_df(sql: str) -> pd.DataFrame:
    engine = create_engine(PG_URL)
    with engine.connect() as conn:
        return pd.read_sql(sql, conn)

# ---------- KPIs ----------
kpi_sql = """
select
  count(distinct order_id) as orders,
  avg(delivery_days) as avg_days,
  avg(is_late)::float as late_rate
from public_mart.fct_delivery_kpis
"""
kpi = load_df(kpi_sql)
c1, c2, c3 = st.columns(3)
c1.metric("Commandes", int(kpi.loc[0,'orders']) if not kpi.empty else 0)
c2.metric("Délai moyen (jours)", round(float(kpi.loc[0,'avg_days']),2) if not kpi.empty and kpi.loc[0,'avg_days'] is not None else 0)
c3.metric("% Retards", f"{round(100*float(kpi.loc[0,'late_rate']),1)}%" if not kpi.empty and kpi.loc[0,'late_rate'] is not None else "0%")

st.markdown("---")

# ---------- Requêtes ----------
late_by_state_sql = """
select c.customer_state, avg(d.is_late)::float as late_rate, count(*) as n
from public_mart.fct_delivery_kpis d
join public_mart.dim_customer c using(customer_id)
where d.is_late is not null
group by 1
order by n desc
"""

delivery_days_sql = """
select delivery_days
from public_mart.fct_delivery_kpis
where delivery_days is not null
"""

rfm_sql = """
select frequency_band, recency_band, avg(is_churned)::float as churn_rate, count(*) as n
from public_mart.fct_customer_churn
group by 1,2
order by n desc
"""

recency_sql = """
select recency_days
from public_mart.fct_customer_churn
where recency_days is not null
"""

by_month_sql = """
select
  date_trunc('month', purchase_ts)::date as month,
  count(distinct order_id) as orders,
  avg(is_late)::float as late_rate,
  avg(delivery_days) as avg_days
from public_mart.fct_delivery_kpis
group by 1
order by 1
"""

# ---------- Ligne 1 : 2 graphiques ----------
left1, right1 = st.columns(2)

with left1:
    st.subheader("Retards par État client")
    df_state = load_df(late_by_state_sql)
    if not df_state.empty:
        fig = px.bar(df_state, x="customer_state", y="late_rate", hover_data=["n"], title="Taux de retard par État")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_state)
    else:
        st.info("Aucune donnée disponible pour les retards par État.")

with right1:
    st.subheader("Distribution des délais de livraison (jours)")
    df_delay = load_df(delivery_days_sql)
    if not df_delay.empty:
        fig = px.box(df_delay, y="delivery_days", title="Distribution des délais de livraison (box plot)")

        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(df_delay.describe().T)
    else:
        st.info("Pas de données de délais de livraison.")

# ---------- Ligne 2 : 2 graphiques ----------
left2, right2 = st.columns(2)

with left2:
    st.subheader("Churn par bandes RFM")
    rfm = load_df(rfm_sql)
    if not rfm.empty:
        fig2 = px.bar(
            rfm, x="frequency_band", y="churn_rate",
            color="recency_band", barmode="group",
            title="Churn moyen par RFM"
        )
        st.plotly_chart(fig2, use_container_width=True)
        st.dataframe(rfm)
    else:
        st.info("Pas encore de données churn (exécuter dbt).")

with right2:
    st.subheader("Distribution de la récence (jours depuis la dernière commande)")
    recency = load_df(recency_sql)
    if not recency.empty:
        fig3 = px.histogram(recency, x="recency_days", nbins=40, title="Histogramme de la récence")
        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(recency.describe().T)
    else:
        st.info("Pas de données de récence client.")

# ---------- Ligne 3 : 2 graphiques temporels ----------
left3, right3 = st.columns(2)

with left3:
    st.subheader("Commandes par mois")
    by_month = load_df(by_month_sql)
    if not by_month.empty:
        fig4 = px.line(by_month, x="month", y="orders", markers=True, title="Volume de commandes (mensuel)")
        st.plotly_chart(fig4, use_container_width=True)
        st.dataframe(by_month)
    else:
        st.info("Pas de séries temporelles disponibles.")

with right3:
    st.subheader("Taux de retard et délai moyen (mensuel)")
    if not by_month.empty:
        # 2 axes séparés : tu peux afficher 2 courbes sur le même graphique
        fig5 = px.line(by_month, x="month", y="late_rate", markers=True, title="Taux de retard (mensuel)")
        st.plotly_chart(fig5, use_container_width=True)
        fig6 = px.line(by_month, x="month", y="avg_days", markers=True, title="Délai moyen (mensuel)")
        st.plotly_chart(fig6, use_container_width=True)
    else:
        st.info("Pas de séries temporelles disponibles.")
