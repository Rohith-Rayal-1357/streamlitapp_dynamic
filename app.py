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
            background-color: #D3E8FF;
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
# Connect to Snowflake
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

# Function to fetch all available tables (assuming you can query the schema for table names)
def fetch_available_tables():
    try:
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'public'"
        tables_df = session.sql(query).to_pandas()
        return tables_df['TABLE_NAME'].tolist()
    except Exception as e:
        st.error(f"Error fetching available tables: {e}")
        return []

# Fetch available tables for source_table selection
available_tables = fetch_available_tables()

if not available_tables:
    st.warning("No tables available to select.")
    st.stop()

# Select source table dynamically
source_table = st.selectbox("Select Source Table", available_tables)

# Function to fetch data from a given table
def fetch_data(table_name):
    try:
        query = f"SELECT * FROM {table_name}"
        df = session.sql(query).to_pandas()
        # Convert column names to uppercase for consistency
        df.columns = [col.strip().upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Retrieve configuration details (editable column, join keys) based on the selected source_table
source_table_ref = override_ref_df[override_ref_df['SOURCE_TABLE'] == source_table]

if source_table_ref.empty:
    st.warning(f"No configuration data found for {source_table}.")
    st.stop()

editable_column = source_table_ref['EDITABLE_COLUMN'].iloc[0].strip().upper()
join_keys = source_table_ref['JOINING_KEYS'].iloc[0].strip().upper().split(',')

# Tabular Display
tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

# Tab 1: Source Data
with tab1:
    st.header(f"Source Data from {source_table}")

    # Fetch source data for the selected table
    source_df = fetch_data(source_table)
    if source_df.empty:
        st.warning(f"No data found in {source_table}.")
        st.stop()

    # Display Editable Column
    st.markdown(f"Editable Column: {editable_column}")

    # Create a copy of the source data for editing
    editable_df = source_df.copy()

    # Ensure editable column exists in the source data
    if editable_column not in source_df.columns:
        st.error(f"Editable column '{editable_column}' not found in source table.")
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

    # Submit Updates Button
    st.markdown('<div class="tooltip">Hover to see description<span class="tooltiptext">This action will update the data.</span></div>', unsafe_allow_html=True)

    if st.button("Submit Updates"):
        # Function to identify changes and insert into target table dynamically
        def insert_into_target_table(session, source_df, edited_data, target_table, editable_column, join_keys):
            try:
                changes_df = edited_data[edited_data[editable_column] != source_df[editable_column]]

                if changes_df.empty:
                    st.info("No changes detected. No records to insert.")
                    return

                target_columns_query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{target_table.upper()}'"
                target_columns = [row['COLUMN_NAME'].upper() for row in session.sql(target_columns_query).to_pandas().to_dict('records')]

                common_columns = [col for col in source_df.columns if col in target_columns and col not in [editable_column, 'AS_AT_DATE', 'RECORD_FLAG', 'AS_OF_DATE']]

                for _, row in changes_df.iterrows():
                    old_value = source_df.loc[source_df.index == row.name, editable_column].values[0]
                    new_value = row[editable_column]
                    as_at_date = row['AS_AT_DATE']
                    as_of_date = row['AS_OF_DATE']

                    columns_to_insert = ', '.join(common_columns + ['AS_OF_DATE', 'SRC_INS_TS', f'{editable_column}_OLD', f'{editable_column}_NEW', 'RECORD_FLAG', 'AS_AT_DATE'])
                    values_to_insert = ', '.join([f"'{row[col]}'" if isinstance(row[col], str) else str(row[col]) for col in common_columns])

                    insert_sql = f"""
                        INSERT INTO {target_table} ({columns_to_insert})
                        VALUES (
                            {values_to_insert},'{as_of_date}', '{as_at_date}', {old_value}, {new_value}, 'A', CURRENT_TIMESTAMP()
                        )
                    """
                    session.sql(insert_sql).collect()

            except Exception as e:
                st.error(f"❌ Error inserting into {target_table}: {e}")

        # Insert changes into target and source tables
        insert_into_target_table(session, source_df, edited_data, "target_table_name", editable_column, join_keys)
        st.success("✅ Data updated successfully!")

# Tab 2: Overridden Values
with tab2:
    st.header(f"Overridden Values in target_table_name")
    overridden_data = fetch_data("target_table_name")
    if overridden_data.empty:
        st.warning("No overridden data found in the target table.")
    else:
        st.dataframe(overridden_data, use_container_width=True)

# Footer
if 'last_update_time' in st.session_state:
    last_update_time = st.session_state.last_update_time
    st.markdown("---")
    st.caption(f"Portfolio Performance Override System • Last updated: {last_update_time}")
else:
    st.markdown("---")
    st.caption("Portfolio Performance Override System • Last updated: N/A")
