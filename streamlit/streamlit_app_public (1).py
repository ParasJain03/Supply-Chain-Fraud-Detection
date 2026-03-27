import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import networkx as nx
import snowflake.connector

st.set_page_config(
    page_title="Supply Chain Fraud Intelligence",
    page_icon="🔍",
    layout="wide"
)

@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account   = st.secrets["snowflake"]["account"],
        user      = st.secrets["snowflake"]["user"],
        password  = st.secrets["snowflake"]["password"],
        warehouse = st.secrets["snowflake"]["warehouse"],
        database  = st.secrets["snowflake"]["database"],
        schema    = st.secrets["snowflake"]["schema"],
        role      = st.secrets["snowflake"]["role"]
    )

@st.cache_data(ttl=300)
def run_query(query):
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(query)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)

st.title("🔍 Supply Chain Fraud Intelligence")
st.caption("Amazon & Flipkart · Neo4j Graph Analytics on Snowflake · Built for Hackathon")

risk_df  = run_query("SELECT * FROM SUPPLY_CHAIN_DB.PUBLIC.SELLER_RISK_MASTER ORDER BY RISK_SCORE DESC")
edges_df = run_query("SELECT SOURCENODEID, TARGETNODEID, ORDER_VALUE FROM SUPPLY_CHAIN_DB.PUBLIC.ORDERS_GRAPH LIMIT 300")
fraud_df = run_query("SELECT SOURCENODEID, TARGETNODEID FROM SUPPLY_CHAIN_DB.PUBLIC.SHARED_BANK_EDGES_GRAPH")
wh_df    = run_query("SELECT NODEID FROM SUPPLY_CHAIN_DB.PUBLIC.WAREHOUSES_GRAPH")

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Total Sellers",    len(risk_df))
c2.metric("🔴 High Risk",     len(risk_df[risk_df['RISK_LEVEL']=='HIGH']))
c3.metric("🟡 Medium Risk",   len(risk_df[risk_df['RISK_LEVEL']=='MEDIUM']))
c4.metric("🟢 Low Risk",      len(risk_df[risk_df['RISK_LEVEL']=='LOW']))
c5.metric("Confirmed Fraud",  int(risk_df['FRAUD_FLAG'].sum()))
c6.metric("Suspicious Pairs", len(fraud_df))

st.divider()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🕸 Network Graph",
    "🔴 Fraud Rings",
    "📊 Analytics",
    "📈 PageRank",
    "📋 Risk Table"
])

with tab1:
    st.subheader("Supply Chain Network — Seller → Warehouse")
    st.caption("Node size = PageRank score | Color = Risk level | Diamond = Warehouse")

    G = nx.DiGraph()

    sample_risk = risk_df.head(80)
    for _, r in sample_risk.iterrows():
        G.add_node(
            int(r['NODEID']),
            ntype    = 'seller',
            risk     = str(r['RISK_LEVEL']),
            name     = str(r['SELLER_NAME']),
            city     = str(r['CITY']),
            platform = str(r['PLATFORM']),
            score    = float(r['RISK_SCORE']) if r['RISK_SCORE'] else 0,
            pagerank = float(r['PAGERANK_SCORE']) if r['PAGERANK_SCORE'] else 0
        )

    for _, r in wh_df.iterrows():
        G.add_node(int(r['NODEID']), ntype='warehouse')

    for _, e in edges_df.iterrows():
        s, t = int(e['SOURCENODEID']), int(e['TARGETNODEID'])
        if s in G.nodes and t in G.nodes:
            G.add_edge(s, t)

    pos = nx.spring_layout(G, seed=42, k=0.8, iterations=40)

    edge_x, edge_y = [], []
    for u, v in G.edges():
        x0, y0 = pos[u]; x1, y1 = pos[v]
        edge_x += [x0, x1, None]
        edge_y += [y0, y1, None]

    seller_nodes = [n for n,d in G.nodes(data=True) if d.get('ntype')=='seller']
    wh_nodes_g   = [n for n,d in G.nodes(data=True) if d.get('ntype')=='warehouse']
    cmap = {'HIGH':'#FF4444','MEDIUM':'#FFA500','LOW':'#44BB44'}

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=edge_x, y=edge_y, mode='lines',
        line=dict(width=0.3, color='#aaaaaa'),
        hoverinfo='none', name='Orders', showlegend=False
    ))

    fig.add_trace(go.Scatter(
        x=[pos[n][0] for n in seller_nodes],
        y=[pos[n][1] for n in seller_nodes],
        mode='markers', name='Sellers',
        marker=dict(
            size=[max(7, G.nodes[n].get('pagerank',0)*100+7) for n in seller_nodes],
            color=[cmap.get(G.nodes[n].get('risk','LOW'),'#44BB44') for n in seller_nodes],
            line=dict(width=0.5, color='white')
        ),
        text=[
            f"<b>{G.nodes[n].get('name','')}</b><br>"
            f"City: {G.nodes[n].get('city','')}<br>"
            f"Platform: {G.nodes[n].get('platform','')}<br>"
            f"Risk: {G.nodes[n].get('risk','')} | Score: {G.nodes[n].get('score',0):.1f}<br>"
            f"PageRank: {G.nodes[n].get('pagerank',0):.4f}"
            for n in seller_nodes
        ],
        hoverinfo='text'
    ))

    fig.add_trace(go.Scatter(
        x=[pos[n][0] for n in wh_nodes_g],
        y=[pos[n][1] for n in wh_nodes_g],
        mode='markers', name='Warehouses',
        marker=dict(size=14, color='#4488FF',
                    symbol='diamond', line=dict(width=1, color='white')),
        text=[f"<b>Warehouse {n}</b><br>Connections: {G.degree(n)}"
              for n in wh_nodes_g],
        hoverinfo='text'
    ))

    fig.update_layout(
        showlegend=True, hovermode='closest', height=620,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor='#0d1117', paper_bgcolor='#0d1117',
        font=dict(color='white'),
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(bgcolor='#1a1a2e', bordercolor='#444')
    )
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.subheader("Fraud Ring Network — WCC Entity Resolution")
    st.caption("Sellers sharing the same bank account are connected — each cluster = one real entity")

    fraud_sellers = risk_df[risk_df['FRAUD_FLAG']==1]

    col_l, col_r = st.columns([3, 2])

    with col_l:
        G2 = nx.Graph()
        for _, r in fraud_sellers.iterrows():
            G2.add_node(int(r['NODEID']),
                        name=str(r['SELLER_NAME'])[:14],
                        community=str(r['LOUVAIN_COMMUNITY']),
                        platform=str(r['PLATFORM']),
                        city=str(r['CITY']))

        for _, e in fraud_df.iterrows():
            s, t = int(e['SOURCENODEID']), int(e['TARGETNODEID'])
            if s in G2.nodes and t in G2.nodes:
                G2.add_edge(s, t)

        pos2 = nx.spring_layout(G2, k=1.5, seed=99)

        comms = list(set(nx.get_node_attributes(G2,'community').values()))
        colors_list = px.colors.qualitative.Set3
        comm_color = {c: colors_list[i % len(colors_list)] for i,c in enumerate(comms)}

        edge_x2, edge_y2 = [], []
        for u, v in G2.edges():
            x0,y0=pos2[u]; x1,y1=pos2[v]
            edge_x2+=[x0,x1,None]; edge_y2+=[y0,y1,None]

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=edge_x2, y=edge_y2, mode='lines',
            line=dict(width=1.5, color='#FF4444'),
            hoverinfo='none', showlegend=False))

        nodes_list = list(G2.nodes())
        fig2.add_trace(go.Scatter(
            x=[pos2[n][0] for n in nodes_list],
            y=[pos2[n][1] for n in nodes_list],
            mode='markers+text',
            marker=dict(
                size=14,
                color=[comm_color.get(G2.nodes[n].get('community','0'),'#888') for n in nodes_list],
                line=dict(width=1, color='white')
            ),
            text=[G2.nodes[n].get('name','') for n in nodes_list],
            textposition='top center',
            textfont=dict(size=7, color='white'),
            hovertext=[
                f"<b>{G2.nodes[n].get('name','')}</b><br>"
                f"Platform: {G2.nodes[n].get('platform','')}<br>"
                f"City: {G2.nodes[n].get('city','')}<br>"
                f"Community: {G2.nodes[n].get('community','')}"
                for n in nodes_list
            ],
            hoverinfo='text', name='Fraud Sellers'
        ))

        fig2.update_layout(
            height=500, hovermode='closest',
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='#0d1117', paper_bgcolor='#0d1117',
            font=dict(color='white'), showlegend=False,
            margin=dict(l=5, r=5, t=5, b=5)
        )
        st.plotly_chart(fig2, use_container_width=True)

    with col_r:
        st.markdown("**Duplicate Seller Groups**")
        rings = risk_df.groupby('WCC_ENTITY').agg(
            Accounts=('NODEID','count'),
            Fraud_Count=('FRAUD_FLAG','sum'),
            Avg_Return=('RETURN_RATE','mean'),
        ).reset_index()
        names_df = risk_df.groupby('WCC_ENTITY')['SELLER_NAME'].apply(lambda x: ' | '.join(list(x)[:3])).reset_index(name='Names')
        rings = rings.merge(names_df, on='WCC_ENTITY', how='left')
        rings = rings[rings['Accounts'] > 1].sort_values('Fraud_Count', ascending=False)
        st.warning(f"⚠ {len(rings)} duplicate identity groups detected")
        st.dataframe(rings, use_container_width=True, height=440)

with tab3:
    st.subheader("Fraud Analytics")

    c1, c2 = st.columns(2)
    cmap_disc = {'HIGH':'#FF4444','MEDIUM':'#FFA500','LOW':'#44BB44'}

    with c1:
        plat = risk_df.groupby(['PLATFORM','RISK_LEVEL']).size().reset_index(name='COUNT')
        fig3 = px.bar(plat, x='PLATFORM', y='COUNT', color='RISK_LEVEL',
            color_discrete_map=cmap_disc,
            title='Risk Distribution by Platform', barmode='stack')
        fig3.update_layout(plot_bgcolor='white')
        st.plotly_chart(fig3, use_container_width=True)

    with c2:
        city = risk_df.groupby(['CITY','RISK_LEVEL']).size().reset_index(name='COUNT')
        fig4 = px.bar(city, x='CITY', y='COUNT', color='RISK_LEVEL',
            color_discrete_map=cmap_disc,
            title='Risk by City', barmode='stack')
        fig4.update_layout(plot_bgcolor='white')
        st.plotly_chart(fig4, use_container_width=True)

    c3, c4 = st.columns(2)
    with c3:
        fig5 = px.pie(risk_df, names='RISK_LEVEL',
            color='RISK_LEVEL', color_discrete_map=cmap_disc,
            title='Overall Risk Distribution')
        st.plotly_chart(fig5, use_container_width=True)

    with c4:
        comm = risk_df.groupby('LOUVAIN_COMMUNITY').agg(
            Sellers=('NODEID','count'),
            Avg_Risk=('RISK_SCORE','mean'),
        ).reset_index()
        high_by_comm = risk_df[risk_df['RISK_LEVEL']=='HIGH'].groupby('LOUVAIN_COMMUNITY').size().reset_index(name='High_Risk')
        comm = comm.merge(high_by_comm, on='LOUVAIN_COMMUNITY', how='left').fillna(0)
        comm = comm.sort_values('Avg_Risk', ascending=False).head(15)
        fig6 = px.bar(comm, x='LOUVAIN_COMMUNITY', y='Avg_Risk',
            color='High_Risk', title='Top Communities by Avg Risk Score',
            color_continuous_scale='Reds')
        fig6.update_layout(plot_bgcolor='white')
        st.plotly_chart(fig6, use_container_width=True)

with tab4:
    st.subheader("PageRank vs Risk Score")
    st.caption("High PageRank + High Risk = most dangerous influential seller")

    fig7 = px.scatter(
        risk_df.dropna(subset=['PAGERANK_SCORE','RISK_SCORE']),
        x='PAGERANK_SCORE', y='RISK_SCORE',
        color='RISK_LEVEL',
        color_discrete_map=cmap_disc,
        hover_data=['SELLER_NAME','CITY','PLATFORM','LOUVAIN_COMMUNITY'],
        size='RISK_SCORE',
        title='PageRank Centrality vs Fraud Risk Score',
        labels={'PAGERANK_SCORE':'PageRank Score','RISK_SCORE':'Risk Score'}
    )
    fig7.update_layout(height=550, plot_bgcolor='white')
    st.plotly_chart(fig7, use_container_width=True)

    st.markdown("**Top 10 Sellers by PageRank**")
    top_pr = risk_df.nlargest(10,'PAGERANK_SCORE')[
        ['SELLER_NAME','PLATFORM','CITY','PAGERANK_SCORE','RISK_LEVEL','RISK_SCORE']
    ]
    st.dataframe(top_pr, use_container_width=True)

with tab5:
    st.subheader("Complete Seller Risk Table")

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        pf = st.selectbox("Platform", ["All","Amazon","Flipkart"])
    with col_f2:
        rl = st.selectbox("Risk Level", ["All","HIGH","MEDIUM","LOW"])

    filtered = risk_df.copy()
    if pf != "All": filtered = filtered[filtered['PLATFORM']==pf]
    if rl != "All": filtered = filtered[filtered['RISK_LEVEL']==rl]

    st.markdown(f"Showing **{len(filtered)}** sellers")
    st.dataframe(
        filtered[[
            'SELLER_NAME','PLATFORM','CITY',
            'RISK_LEVEL','RISK_SCORE','PAGERANK_SCORE',
            'LOUVAIN_COMMUNITY','WCC_ENTITY',
            'RETURN_RATE','FRAUD_FLAG'
        ]],
        use_container_width=True, height=500
    )

st.divider()
st.caption("Supply Chain Fraud Detection · Neo4j Graph Analytics · Snowflake · ParasJain03")
