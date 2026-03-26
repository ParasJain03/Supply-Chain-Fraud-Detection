import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import networkx as nx
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.set_page_config(page_title="Fraud Detection Dashboard", layout="wide")
st.title("Supply Chain Fraud Detection Dashboard")

risk_df = session.sql("""
    SELECT * FROM SUPPLY_CHAIN_DB.PUBLIC.SELLER_RISK_REALTIME
    ORDER BY RISK_SCORE DESC
""").to_pandas()

col1, col2, col3, col4 = st.columns(4)
high = len(risk_df[risk_df['RISK_LEVEL'] == 'HIGH'])
med = len(risk_df[risk_df['RISK_LEVEL'] == 'MEDIUM'])
low = len(risk_df[risk_df['RISK_LEVEL'] == 'LOW'])
col1.metric("Total Sellers", len(risk_df))
col2.metric("HIGH Risk", high, delta=None)
col3.metric("MEDIUM Risk", med, delta=None)
col4.metric("LOW Risk", low, delta=None)

st.divider()

alert_df = session.sql("""
    SELECT * FROM SUPPLY_CHAIN_DB.PUBLIC.FRAUD_ALERT_LOG
    ORDER BY ALERT_TIME DESC LIMIT 50
""").to_pandas()

tab1, tab2, tab3, tab4 = st.tabs(["Network Graph", "Risk Table", "Analytics", "Alert Log"])

with tab1:
    st.subheader("Seller-Warehouse Network")

    edges_df = session.sql("""
        SELECT SOURCENODEID, TARGETNODEID,
               SUM(ORDER_VALUE) AS TOTAL_VALUE, COUNT(*) AS CNT
        FROM SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH
        GROUP BY SOURCENODEID, TARGETNODEID
    """).to_pandas()

    wh_df = session.sql("SELECT NODEID FROM SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH").to_pandas()

    G = nx.Graph()
    for _, r in risk_df.iterrows():
        G.add_node(r['NODEID'], node_type='seller', risk=r['RISK_LEVEL'],
                   score=r['RISK_SCORE'], name=r['SELLER_NAME'])
    for _, r in wh_df.iterrows():
        G.add_node(r['NODEID'], node_type='warehouse')
    for _, r in edges_df.iterrows():
        G.add_edge(r['SOURCENODEID'], r['TARGETNODEID'])

    pos = nx.spring_layout(G, seed=42, k=0.5, iterations=30)

    edge_x, edge_y = [], []
    for u, v in G.edges():
        x0, y0 = pos[u]; x1, y1 = pos[v]
        edge_x += [x0, x1, None]; edge_y += [y0, y1, None]

    seller_nodes = [n for n, d in G.nodes(data=True) if d.get('node_type') == 'seller']
    wh_nodes = [n for n, d in G.nodes(data=True) if d.get('node_type') == 'warehouse']
    cmap = {'HIGH': '#FF4444', 'MEDIUM': '#FFA500', 'LOW': '#44BB44'}

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=edge_x, y=edge_y, mode='lines',
        line=dict(width=0.3, color='#ddd'), hoverinfo='none'))
    fig.add_trace(go.Scatter(
        x=[pos[n][0] for n in seller_nodes], y=[pos[n][1] for n in seller_nodes],
        mode='markers', name='Sellers',
        marker=dict(size=8, color=[cmap.get(G.nodes[n].get('risk','LOW'),'#44BB44') for n in seller_nodes]),
        text=[f"{G.nodes[n].get('name','')}<br>Risk: {G.nodes[n].get('risk','')}" for n in seller_nodes],
        hoverinfo='text'))
    fig.add_trace(go.Scatter(
        x=[pos[n][0] for n in wh_nodes], y=[pos[n][1] for n in wh_nodes],
        mode='markers', name='Warehouses',
        marker=dict(size=12, color='#4488FF', symbol='diamond'),
        text=[f"Warehouse {n}" for n in wh_nodes], hoverinfo='text'))
    fig.update_layout(showlegend=True, hovermode='closest', height=600,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='white', margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Seller Risk Table")
    risk_filter = st.multiselect("Filter by Risk Level", ['HIGH', 'MEDIUM', 'LOW'], default=['HIGH', 'MEDIUM'])
    filtered = risk_df[risk_df['RISK_LEVEL'].isin(risk_filter)] if risk_filter else risk_df
    st.dataframe(filtered, use_container_width=True, height=500)

with tab3:
    st.subheader("Risk Analytics")
    c1, c2 = st.columns(2)
    with c1:
        fig_scatter = px.scatter(risk_df, x='PAGERANK_SCORE', y='RISK_SCORE',
            color='RISK_LEVEL', hover_data=['SELLER_NAME', 'CITY'],
            color_discrete_map=cmap, title='PageRank vs Risk Score')
        st.plotly_chart(fig_scatter, use_container_width=True)
    with c2:
        city_risk = risk_df.groupby(['CITY', 'RISK_LEVEL']).size().reset_index(name='COUNT')
        fig_bar = px.bar(city_risk, x='CITY', y='COUNT', color='RISK_LEVEL',
            color_discrete_map=cmap, title='Risk by City', barmode='stack')
        st.plotly_chart(fig_bar, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        fig_pie = px.pie(risk_df, names='RISK_LEVEL', title='Risk Distribution',
            color='RISK_LEVEL', color_discrete_map=cmap)
        st.plotly_chart(fig_pie, use_container_width=True)
    with c4:
        comm_df = risk_df.groupby('LOUVAIN_COMMUNITY').agg(
            SELLERS=('NODEID', 'count'),
            AVG_RISK=('RISK_SCORE', 'mean'),
            HIGH_RISK=('RISK_LEVEL', lambda x: (x == 'HIGH').sum())
        ).reset_index().sort_values('AVG_RISK', ascending=False)
        st.dataframe(comm_df, use_container_width=True, height=300)

with tab4:
    st.subheader("Fraud Alert Log")
    if len(alert_df) > 0:
        st.dataframe(alert_df, use_container_width=True, height=400)
    else:
        st.info("No alerts yet. Alerts trigger when new HIGH-risk sellers are detected.")

    st.subheader("Pipeline Status")
    tasks_df = session.sql("SHOW TASKS IN SCHEMA SUPPLY_CHAIN_DB.PUBLIC").to_pandas()
    st.dataframe(tasks_df, use_container_width=True)

st.caption(f"Last refreshed: {risk_df['LAST_REFRESHED'].max() if 'LAST_REFRESHED' in risk_df.columns else 'N/A'}")

