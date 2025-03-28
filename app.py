import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Editable Data Override App",
    page_icon="📊",
    layout="centered"  # Match the layout you want
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

# Initialize session state for last update time
if 'last_update_time' not in st.session_state:
    st.session_state.last_update_time = "N/A"

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

# Validate if module_number is a digit
if isinstance(module_number, list):
    module_number = module_number[0]  # Take the first element if it's a list

if not module_number.isdigit():
    st.error("Invalid module number. Please provide a numeric value.")
    st.stop()

module_number = int(module_number)

override_ref_df = fetch_override_ref_data(module_number)

if override_ref_df.empty:
    st.warning("No configuration data found in Override_Ref.")
    st.stop()

# Display module name based on module_number
module_name = override_ref_df['MODULE_NAME'].iloc[0] if 'MODULE_NAME' in override_ref_df.columns else f"Module {module_number}"
st.markdown(f"<div class='module-box'>{module_name}</div>", unsafe_allow_html=True)

# Dropdown for table selection based on module-level configuration
table_options = override_ref_df['SOURCE_TABLE'].unique()
selected_table = st.selectbox("Select Table", options=table_options)

# Fetch table description from override_ref
selected_table_description = override_ref_df[override_ref_df['SOURCE_TABLE'] == selected_table]['DESCRIPTION'].iloc[0] if not override_ref_df[override_ref_df['SOURCE_TABLE'] == selected_table].empty else "No description available."
st.markdown(f"<p style='text-align:center; font-size:16px;'>{selected_table_description}</p>", unsafe_allow_html=True)
# Function to fetch data from a given table
def fetch_data(table_name):
    try:
        query = f"SELECT * FROM {table_name} WHERE RECORD_FLAG = 'A'"
        df = session.sql(query).to_pandas()
        # Convert column names to uppercase for consistency
        df.columns = [col.strip().upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {table_name}: {e}")
        return pd.DataFrame()

# Function to fetch data from the target table
def fetch_target_data(target_table):
    try:
        query = f"SELECT * FROM {target_table}"
        df = session.sql(query).to_pandas()
        # Convert column names to uppercase for consistency
        df.columns = [col.strip().upper() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Error fetching data from {target_table}: {e}")
        return pd.DataFrame()

# Extract configuration data for the selected table
config = override_ref_df[override_ref_df['SOURCE_TABLE'] == selected_table].iloc[0]
source_table = config['SOURCE_TABLE']
target_table = config['TARGET_TABLE']
editable_column = config['EDITABLE_COLUMN'].strip().upper()
join_keys = config['JOINING_KEYS'].strip().upper().split(',')

# Tabular Display
tab1, tab2 = st.tabs(["Source Data", "Overridden Values"])

# Tab 1: Source Data
with tab1:
    st.header(f"Source Data from {source_table}")
    source_df = fetch_data(source_table)
    if source_df.empty:
        st.warning("No data found in the source table.")
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
                # Identify rows where the editable column has changed
                changes_df = edited_data[edited_data[editable_column] != source_df[editable_column]]

                if changes_df.empty:
                    st.info("No changes detected. No records to insert.")
                    return

                # Fetch the target table columns dynamically
                target_columns_query = f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{target_table.upper()}'"""
                target_columns = [row['COLUMN_NAME'].upper() for row in session.sql(target_columns_query).to_pandas().to_dict('records')]

                # Identify common columns (excluding SRC_INS_TS, editable_column_old, editable_column_new, record_flag, and as_at_date)
                common_columns = [col for col in source_df.columns if col in target_columns and col not in [editable_column, 'AS_AT_DATE', 'RECORD_FLAG','AS_OF_DATE']]

                for _, row in changes_df.iterrows():
                    old_value = source_df.loc[source_df.index == row.name, editable_column].values[0]
                    new_value = row[editable_column]
                    as_at_date = row['AS_AT_DATE']
                    as_of_date = row['AS_OF_DATE']

                    # Forming the dynamic insert query
                    columns_to_insert = ', '.join(common_columns + ['AS_OF_DATE','SRC_INS_TS', f'{editable_column}_OLD', f'{editable_column}_NEW', 'RECORD_FLAG', 'AS_AT_DATE'])

                    # Prepare values, handling potential None or '' values
                    values_to_insert = []
                    for col in common_columns:
                        value = row[col]
                        if pd.isna(value):  # Check for NaN or None
                            values_to_insert.append('NULL')  # Use NULL for Snowflake
                        elif isinstance(value, str):
                            # Escape single quotes within the string by replacing them with double single quotes
                            value = value.replace("'", "''")
                            values_to_insert.append(f"'{value}'")  # Enclose strings in single quotes
                        else:
                            values_to_insert.append(str(value))  # Convert non-string values to strings

                    values_to_insert_str = ', '.join(values_to_insert)

                    insert_sql = f"""
                        INSERT INTO {target_table} ({columns_to_insert})
                        VALUES (
                            {values_to_insert_str},'{as_of_date}', CURRENT_TIMESTAMP(), {old_value}, {new_value}, 'O', '{as_at_date}'
                        )
                    """
                    try:
                        session.sql(insert_sql).collect()
                    except Exception as e:
                        st.error(f"❌ Error inserting record: {e}")
                        st.error(f"SQL Query: {insert_sql}")
                        raise

            except Exception as e:
                st.error(f"❌ Error inserting into {target_table}: {e}")

        # Single 'Submit Changes' button
        def insert_into_source_table(session, target_table, source_table, editable_column, join_keys):
            try:
                # Generate common columns excluding record_flag, as_at_date, and editable_column
                target_columns_query = f"""
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE UPPER(TABLE_NAME) = '{source_table.upper()}'
                      AND COLUMN_NAME NOT IN ('RECORD_FLAG', 'AS_AT_DATE', '{editable_column.upper()}')
                """
                common_columns = [row['COLUMN_NAME'].upper() for row in session.sql(target_columns_query).to_pandas().to_dict('records')]

                if not common_columns:
                    st.error("No matching common columns found between target and source.")
                    return

                # Formulate the insert SQL query
                columns_to_insert = ', '.join(common_columns + [editable_column, 'RECORD_FLAG', 'AS_AT_DATE'])
                insert_sql = f"""
                    INSERT INTO {source_table} ({columns_to_insert})
                    SELECT
                        {', '.join([f"src.{col}" for col in common_columns])},
                        src.{editable_column}_NEW,
                        'A',
                        CURRENT_TIMESTAMP(0)
                    FROM {target_table} src
                    JOIN {source_table} tgt
                     -------#Replaces NULL with empty string for comparison  # ON {" AND ".join([f"tgt.{key} = src.{key}" for key in join_keys])}
                    ON {" AND ".join([f"COALESCE(tgt.{key}, '') = COALESCE(src.{key}, '')"  for key in join_keys])}
                    AND tgt.{editable_column} = src.{editable_column}_OLD
                    WHERE tgt.RECORD_FLAG = 'A';
                """

                # Execute SQL
                session.sql(insert_sql).collect()

            except Exception as e:
                st.error(f"❌ Error inserting into {source_table}: {e}")

        # Function to update the old record in the source table
        def update_old_record(session, target_table, source_table, editable_column, join_keys):
            try:
                # Form the dynamic SQL query to update old records
                join_condition = " AND ".join([
                    f"COALESCE(tgt.{key}, '') = COALESCE(src.{key}, '')"  # Handle NULL/empty values in join keys
                    for key in join_keys
                ])

                update_sql = f"""
                    UPDATE {source_table} tgt
                    SET record_flag = 'D'
                    FROM {target_table} src
                    WHERE {join_condition}
                      AND tgt.{editable_column} = src.{editable_column}_OLD
                      AND tgt.record_flag = 'A';
                """

                # Execute the update SQL
                session.sql(update_sql).collect()

            except Exception as e:
                # Handle and display any errors
                st.error(f"❌ Error updating old records in {source_table}: {e}")

        # Step 1: Insert into target table (fact_portfolio_perf_override)
        insert_into_target_table(session, source_df, edited_data, target_table, editable_column, join_keys)

        # Step 2: Insert into source table (fact_portfolio_perf)
        insert_into_source_table(session, target_table, source_table, editable_column, join_keys)

        # Step 3: Update old records in source table (fact_portfolio_perf)
        update_old_record(session, target_table, source_table, editable_column, join_keys)

        # Update the last update time in session state
        st.session_state.last_update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        st.success("✅ Data updated successfully👍!")
       
# Tab 2: Overridden Values
with tab2:
    st.header(f"Overridden Values in {target_table}")
    overridden_data = fetch_target_data(target_table)  # Use the new function here
    if overridden_data.empty:
        st.warning("No overridden data found in the target table.")
    else:
        st.dataframe(overridden_data, use_container_width=True)


# Footer
st.markdown("---")
st.caption(f"Portfolio Performance Override System • Last updated: {st.session_state.last_update_time}")
