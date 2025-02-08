import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from analytics.analytics_utilities import AnalyticsUtilities

# Page config
st.set_page_config(
    page_title="Stack Exchange Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        text-align: center;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #0068c9;
    }
    .metric-label {
        font-size: 16px;
        color: #555;
    }
    .intro-text {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        margin-bottom: 20px;
        border-left: 5px solid #0068c9;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize analytics
analytics = AnalyticsUtilities()

# Header and Introduction
st.title("📊 Stack Exchange Analytics Dashboard")

# Add last update timestamp
current_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
st.markdown(f"*Last updated: {current_time}*")

# Introduction section with proper markdown
st.markdown("""
## About This Dashboard
Stack Exchange is a network of Q&A websites where millions of community members share knowledge across diverse topics. 
This dashboard provides insights into user engagement, content quality, and community health metrics from our Data Vault implementation.

### Dashboard Features:
* 📊 Key Performance Indicators showing community engagement metrics
* 📈 Comprehensive user activity analysis and classification
* 🏷️ Popular tags analysis showing topic trends
* ⚡ User response time and interaction patterns
* 💬 Comment quality and engagement analysis

*This dashboard analyzes historical Stack Exchange data stored in our Data Vault architecture. The metrics provide insights into community behavior and content patterns.*
""")

st.markdown("---")

# KPI Metrics Section
st.header("📈 Key Performance Indicators")
col1, col2, col3 = st.columns(3)

# Comments to Upvotes Ratio
comments_upvotes = analytics.ration_comments_upvotes()
with col1:
    ratio_value = comments_upvotes['ratio'].iloc[0] if not comments_upvotes.empty else 0
    st.markdown("""
        <div class="metric-card">
            <div class="metric-value">{:.2f}</div>
            <div class="metric-label">Comments per Upvote</div>
        </div>
    """.format(ratio_value), unsafe_allow_html=True)

# Upvotes to Edits Ratio
upvotes_edits = analytics.ration_upvotes_edits()
with col2:
    ratio_value = upvotes_edits['ratio'].iloc[0] if not upvotes_edits.empty else 0
    st.markdown("""
        <div class="metric-card">
            <div class="metric-value">{:.2f}</div>
            <div class="metric-label">Upvotes per Edit</div>
        </div>
    """.format(ratio_value), unsafe_allow_html=True)

# Views per Question
views_ratio = analytics.views_per_question_ratio()
with col3:
    ratio_value = views_ratio['average_views_per_question'].iloc[0] if not views_ratio.empty else 0
    st.markdown("""
        <div class="metric-card">
            <div class="metric-value">{:.0f}</div>
            <div class="metric-label">Avg Views per Question</div>
        </div>
    """.format(ratio_value), unsafe_allow_html=True)

st.markdown("---")

# Popular Tags Visualization
st.header("🏷️ Most Popular Tags")
popular_tags = analytics.most_popular_tags(limit=10)
fig_tags = px.bar(
    popular_tags,
    x='tag_name',
    y='usage_count',
    title='Top 10 Most Used Tags',
    labels={'tag_name': 'Tag', 'usage_count': 'Number of Uses'},
    color='usage_count',
    color_continuous_scale='Viridis'
)
fig_tags.update_layout(showlegend=False)
st.plotly_chart(fig_tags, use_container_width=True)

# User Activity Analysis
st.header("👥 User Activity Analysis")
col1, col2 = st.columns(2)

with col1:
    active_users = analytics.active_vs_non_active_users()
    active_counts = active_users['activity_status'].value_counts()
    fig_activity = px.pie(
        values=active_counts.values,
        names=active_counts.index,
        title='User Activity Distribution',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    st.plotly_chart(fig_activity, use_container_width=True)

with col2:
    # Create a box plot for post counts
    fig_posts = px.box(
        active_users,
        y='post_count',
        color='activity_status',
        title='Post Count Distribution by Activity Status',
        points="all"
    )
    st.plotly_chart(fig_posts, use_container_width=True)

# Before the User Comments Analysis section
# Add Debug Information (can be toggled)
with st.expander("🔍 Debug Information"):
    st.subheader("Data Relationship Statistics")
    relationships = analytics.check_data_relationships()
    if not relationships.empty:
        for col in relationships.columns:
            st.metric(col, f"{relationships[col].iloc[0]:,}")
    
    st.markdown("---")
    st.markdown("**Note:** This information helps understand the data relationships in our database.")

# User Comments Analysis
st.header("💬 User Comments Analysis")
longest_comments = analytics.users_with_longest_comments(limit=10)
if not longest_comments.empty:
    fig_comments = px.bar(
        longest_comments,
        x='displayname',
        y='avg_comment_length',
        title='Users with Longest Comments',
        labels={'displayname': 'User', 'avg_comment_length': 'Average Comment Length'},
        color='total_comments',
        color_continuous_scale='Viridis'
    )
    st.plotly_chart(fig_comments, use_container_width=True)
else:
    st.info("No comment length data available.")

# Response Time Analysis
st.header("⚡ User Response Time Analysis")
fastest_commenters = analytics.fastest_commenters(limit=10)
if not fastest_commenters.empty:
    fastest_commenters['avg_response_time_minutes'] = fastest_commenters['avg_response_time_seconds'] / 60
    fig_response = px.bar(
        fastest_commenters,
        x='displayname',
        y='avg_response_time_minutes',
        title='Fastest Responding Users',
        labels={'displayname': 'User', 'avg_response_time_minutes': 'Average Response Time (minutes)'},
        color='total_comments',
        color_continuous_scale='Viridis'
    )
    st.plotly_chart(fig_response, use_container_width=True)
else:
    st.info("No response time data available.")

# Tag Engagement Analysis
st.header("🎯 Tag Engagement Analysis")
st.markdown("""
This analysis shows which topics (tags) generate the most engagement in terms of:
- Number of posts
- Number of comments
- Average comments per post
- Average response time
""")

tag_engagement = analytics.tag_engagement_analysis(limit=10)
if not tag_engagement.empty:
    # Create two columns for the visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        # Comments per Post by Tag
        fig_comments_per_post = px.bar(
            tag_engagement,
            x='tag_name',
            y='comments_per_post',
            title='Average Comments per Post by Tag',
            labels={'tag_name': 'Tag', 'comments_per_post': 'Comments per Post'},
            color='post_count',
            color_continuous_scale='Viridis'
        )
        fig_comments_per_post.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_comments_per_post, use_container_width=True)
    
    with col2:
        # Response Time by Tag
        fig_response_time = px.bar(
            tag_engagement,
            x='tag_name',
            y='avg_response_time_minutes',
            title='Average Response Time by Tag (minutes)',
            labels={'tag_name': 'Tag', 'avg_response_time_minutes': 'Response Time (min)'},
            color='comment_count',
            color_continuous_scale='Viridis'
        )
        fig_response_time.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_response_time, use_container_width=True)
    
    # Display detailed statistics in an expander
    with st.expander("📊 Detailed Tag Statistics"):
        st.dataframe(
            tag_engagement.style.format({
                'comments_per_post': '{:.2f}',
                'avg_response_time_minutes': '{:.1f}'
            })
        )
else:
    st.info("No tag engagement data available.")

# Footer
st.markdown("---")
st.markdown("*Dashboard created with Streamlit and Plotly*") 