import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="centered"
)

# Custom CSS for styling (consistent style)
st.markdown("""
    <style>
        .css-18e3th9 {background-color: #F0F2F6;} /* Light grey background */
        .css-1kyxreq {border-radius: 12px; padding: 20px;}
        .css-1b36jdy {text-align: center;}
        .stButton>button {background-color: #1E88E5; color: white; border-radius: 5px; height: 40px;}
        .stSelectbox>label {font-size: 16px;}
        .stDataFrame {border: 1px solid #dddddd; border-radius: 8px;}
        .module-box {
            background-color: #D3E8FF; /* Ice-blue box for module */
            padding: 15px;
            border-radius: 8px;
            font-size: 20px;
            font-weight: bold;
            text-align: center;
        }
        /* Tooltip CSS for styling */
        .tooltip {
            position: relative;
            display: inline-block;
            cursor: pointer;
        }

        .tooltip .tooltiptext {
            visibility: hidden;
            width: 250px;
            background-color: #6c757d;
            color: #fff;
            text-align: center;
            border-radius: 5px;
            padding: 5px;
            position: absolute;
            z-index: 1;
            bottom: 125%; /* Position above the button */
            left: 50%;
            margin-left: -125px; /* Centers the tooltip */
            opacity: 0;
            transition: opacity 0.3s;
        }

        .tooltip:hover .tooltiptext {
            visibility: visible;
            opacity: 1;
        }
    </style>
""", unsafe_allow_html=True)

# Title with custom styling
st.markdown("<h1 style='text-align: center; color: #1E88E5;'>Override Dashboard</h1>", unsafe_allow_html=True)

# Retrieve Snowflake credentials from Streamlit secrets
def connect_to_snowflake():
    try:
        connection_parameters = {
            "account": st.secrets["SNOWFLAKE_ACCOUNT"],
            "user": st.secrets["SNOWFLAKE_USER"],
            "password": st.secrets["SNOWFLAKE_PASSWORD"],
            "warehouse": st.secrets["SNOWFLAKE_WAREHOUSE"],
            "database": st.secrets["SNOWFLAKE_DATABASE"],
            "schema": st.secrets["SNOWFLAKE_SCHEMA"],
        }
        session = Session.builder.configs(connection_parameters).create()
        st.success("✅ Successfully Connected to Snowflake")
        return session
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")
        st.stop()

session = connect_to_snowflake()

# Retrieve Configuration Data from Override_Ref
def fetch_override_ref_data(module_number):
    try:
        df = session.sql(f"SELECT * FROM override_ref WHERE module = {module_number}").to_pandas()
        return df
    except Exception as e:
        st.error(f"Error fetching Override_Ref data: {e}")
        return pd.DataFrame()

# Example - Assuming module number is passed via query parameters
query_params = st.query_params
module_number = query_params.get("module", 1)
override_ref_df = fetch_override_ref_data(module_number)

if override_ref_df.empty:
    st.warning("No configuration data found in Override_Ref.")
    st.stop()

# Extract the module name dynamically from the override_ref DataFrame
module_name = override_ref_df['MODULE_NAME'].iloc[0]

# Display the module name in ice-blue color
st.markdown(f"<div class='module-box'>{module_name}</div>", unsafe_allow_html=True)

# Function to fetch data from a given table
def fetch_data(table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        df = session.sql(query).to_pandas()
        df.columns = [col.strip().upper() for col in df.columns]  # Normalize columns
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Extract source and target table names, editable column, and join keys
source_table = override_ref_df['SOURCE_TABLE'].iloc[0]
target_table = override_ref_df['TARGET_TABLE'].iloc[0]
editable_column = override_ref_df['EDITABLE_COLUMN'].iloc[0].strip().upper()
join_keys = override_ref_df['JOINING_KEYS'].iloc[0].strip().upper().split(',')

# Display a dropdown to select a table under the current module
selected_table = st.selectbox(
    'Select a Table to Edit', 
    options=[source_table, target_table],
    label_visibility="collapsed"  # Hide the label for a cleaner UI
)

# Tabular Display
tab1, tab2 = st.tabs([f"Source Data ({selected_table})", "Overridden Values"])

# Tab 1: Source Data
with tab1:
    st.header(f"Source Data from {selected_table}")
    
    # Add search bar functionality to filter the source data table
    filter_term = st.text_input("Filter Source Data", placeholder="Search for records...", key="search_source_data")
    
    with st.spinner(f'Loading data from {selected_table}...'):
        source_df = fetch_data(selected_table)
        
    if source_df.empty:
        st.warning(f"No data found in the {selected_table} table.")
        st.stop()

    st.markdown(f"Editable Column: {editable_column}")

    # Filter the data based on the search term
    if filter_term:
        source_df = source_df[source_df.astype(str).apply(lambda x: x.str.contains(filter_term, case=False)).any(axis=1)]

    # Create a copy of the source data for editing
    editable_df = source_df.copy()

    # Ensure editable column exists in the source data
    if editable_column not in source_df.columns:
        st.error(f"Editable column '{editable_column}' not found in {selected_table} table.")
        st.stop()

    # Highlight the editable column and make it editable
    edited_data = st.data_editor(
        editable_df,
        column_config={
            editable_column: st.column_config.NumberColumn(f"{editable_column} (Editable)✏️")
        },
        disabled=[col for col in editable_df.columns if col != editable_column],
        use_container_width=True,
        hide_index=True  # Remove the index column
    )

    # Submit Updates Button with Tooltip
    st.markdown('<div class="tooltip">Hover to see description<span class="tooltiptext">This action will update the data.</span></div>', unsafe_allow_html=True)

    # Ask for confirmation before submitting the updates
    if st.button("Submit Updates"):
        if st.confirm("Are you sure you want to submit these changes?"):
            # Function to identify changes and insert into target table dynamically
            insert_into_target_table(session, source_df, edited_data, target_table, editable_column, join_keys)
            insert_into_source_table(session, target_table, source_table, editable_column, join_keys)
            update_old_record(session, target_table, source_table, editable_column, join_keys)
            st.success("✅ Data updated successfully!")
        else:
            st.warning("Changes not submitted!")

# Tab 2: Overridden Values
with tab2:
    st.header(f"Overridden Values in {target_table}")
    
    # Add search bar functionality to filter the target table
    filter_term = st.text_input("Filter Overridden Values", placeholder="Search for overridden records...", key="search_overridden_data")
    
    with st.spinner(f'Loading overridden values from {target_table}...'):
        overridden_data = fetch_data(target_table)
        
    if overridden_data.empty:
        st.warning(f"No overridden data found in the {target_table} table.")
        st.stop()

    # Filter the data based on the search term
    if filter_term:
        overridden_data = overridden_data[overridden_data.astype(str).apply(lambda x: x.str.contains(filter_term, case=False)).any(axis=1)]

    st.dataframe(overridden_data, use_container_width=True)

# Footer
if 'last_update_time' in st.session_state:
    last_update_time = st.session_state.last_update_time
    st.markdown("---")
    st.caption(f"Portfolio Performance Override System • Last updated: {last_update_time}")
else:
    st.markdown("---")
    st.caption("Portfolio Performance Override System • Last updated: N/A")
