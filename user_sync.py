# ------------------------------------------------------------------
# Append GAD groups (gad_groups.csv) into the "Unnamed" group columns
# between ATTR_GROUPS and ATTR_USER_KEY for mismatch_sync
# ------------------------------------------------------------------
cu.header("Appending GAD Groups for Duplicate Users (mismatch_sync)")

# map: email -> [group1, group2, ...]
email_to_groups = (
    gad_groups_df.dropna(subset=["email_add", "group_name"])
    .groupby("email_add")["group_name"]
    .apply(list)
    .to_dict()
)

# Ensure email key is normalized in mismatch_sync
mismatch_sync["ATTR_EMAIL"] = mismatch_sync["ATTR_EMAIL"].astype(str).str.strip().str.lower()

# Identify the "group columns" slice: from ATTR_GROUPS up to (but excluding) ATTR_USER_KEY
cols = list(mismatch_sync.columns)
try:
    i_groups = cols.index("ATTR_GROUPS")
    i_user_key = cols.index("ATTR_USER_KEY")
except ValueError as e:
    raise ValueError(
        "Could not find required columns in mismatch_sync. "
        "Expected to find 'ATTR_GROUPS' and 'ATTR_USER_KEY'."
    ) from e

def _is_filled(v) -> bool:
    if pd.isna(v):
        return False
    s = str(v).strip()
    return s != ""

# ------------------------------------------------------------------
# PRE-CALCULATE: How many total group columns do we need?
# ------------------------------------------------------------------
max_groups_needed = 0
problematic_rows = []

for row_idx in mismatch_sync.index:
    email = mismatch_sync.at[row_idx, "ATTR_EMAIL"]
    new_groups = email_to_groups.get(email, [])
    
    if not new_groups:
        continue
    
    # Count existing filled columns for this row
    group_cols = cols[i_groups:i_user_key]
    last_filled_pos = -1
    for j, c in enumerate(group_cols):
        if _is_filled(mismatch_sync.at[row_idx, c]):
            last_filled_pos = j
    
    # Start position (never overwrite ATTR_GROUPS itself)
    start_pos = max(last_filled_pos + 1, 1)
    needed = start_pos + len(new_groups)
    
    if needed > max_groups_needed:
        max_groups_needed = needed
        problematic_rows.append({
            'row_idx': row_idx,
            'email': email,
            'last_filled': last_filled_pos,
            'start_pos': start_pos,
            'new_groups_count': len(new_groups),
            'needed': needed
        })

cu.info(f"Max groups needed: {max_groups_needed}")
cu.info(f"Most demanding rows: {problematic_rows[-3:]}")

# ------------------------------------------------------------------
# CREATE ALL NEEDED COLUMNS BY SPLITTING AND RECOMBINING THE DATAFRAME
# ------------------------------------------------------------------
current_group_cols = i_user_key - i_groups

if max_groups_needed > current_group_cols:
    cols_to_add = max_groups_needed - current_group_cols
    
    cu.info(f"Need to add {cols_to_add} new columns")
    
    # Find the current max "Unnamed: N"
    max_n = -1
    for c in mismatch_sync.columns:
        if isinstance(c, str) and c.startswith("Unnamed:"):
            try:
                n = int(c.split(":", 1)[1].strip())
                max_n = max(max_n, n)
            except Exception:
                pass
    
    # Split the DataFrame into three parts:
    cols_before = cols[:i_user_key]  # Everything before ATTR_USER_KEY
    cols_after = cols[i_user_key:]   # ATTR_USER_KEY and everything after
    
    df_before = mismatch_sync[cols_before].copy()
    df_after = mismatch_sync[cols_after].copy()
    
    # Create new columns with NA values
    new_cols_dict = {}
    for i in range(cols_to_add):
        new_col_name = f"Unnamed: {max_n + 1 + i}"
        new_cols_dict[new_col_name] = pd.NA
    
    df_new = pd.DataFrame(new_cols_dict, index=mismatch_sync.index)
    
    # Concatenate: before + new + after
    mismatch_sync = pd.concat([df_before, df_new, df_after], axis=1)
    
    cu.info(f"Added {cols_to_add} new columns before ATTR_USER_KEY")

# ------------------------------------------------------------------
# NOW FILL IN THE GROUPS (no more column insertions)
# ------------------------------------------------------------------
# Refresh column indices after insertions
cols = list(mismatch_sync.columns)
i_groups = cols.index("ATTR_GROUPS")
i_user_key = cols.index("ATTR_USER_KEY")
group_cols = cols[i_groups:i_user_key]

cu.info(f"Total group columns available: {len(group_cols)}")
cu.info(f"ATTR_GROUPS at position: {i_groups}")
cu.info(f"ATTR_USER_KEY at position: {i_user_key}")

# Show first and last few group column names
cu.info(f"First 5 group cols: {group_cols[:5]}")
cu.info(f"Last 5 group cols: {group_cols[-5:]}")

filled_count = 0
for row_idx in mismatch_sync.index:
    email = mismatch_sync.at[row_idx, "ATTR_EMAIL"]
    new_groups = email_to_groups.get(email, [])

    if not new_groups:
        continue

    # Find the last filled group column for THIS ROW
    last_filled_pos = -1
    for j, c in enumerate(group_cols):
        if _is_filled(mismatch_sync.at[row_idx, c]):
            last_filled_pos = j

    # Start writing after the last filled position (min position 1 to skip ATTR_GROUPS)
    start_pos = max(last_filled_pos + 1, 1)

    # Verify we have enough columns
    if start_pos + len(new_groups) > len(group_cols):
        cu.error(f"Row {row_idx}: Not enough group columns!")
        cu.error(f"  Email: {email}")
        cu.error(f"  Last filled position: {last_filled_pos}")
        cu.error(f"  Start position: {start_pos}")
        cu.error(f"  Groups to add: {len(new_groups)}")
        cu.error(f"  Need: {start_pos + len(new_groups)}, Have: {len(group_cols)}")
        raise ValueError(f"Insufficient group columns for row {row_idx}")

    # Write groups into the next available slots
    for k, g in enumerate(new_groups):
        target_idx = start_pos + k
        target_col = group_cols[target_idx]
        
        # Extra verification: make sure target_col exists in mismatch_sync and is before ATTR_USER_KEY
        if target_col not in mismatch_sync.columns:
            cu.error(f"Target column '{target_col}' doesn't exist!")
            raise ValueError(f"Column {target_col} not found")
        
        col_position = list(mismatch_sync.columns).index(target_col)
        if col_position >= i_user_key:
            cu.error(f"ERROR: Attempting to write to column '{target_col}' at position {col_position}, which is >= ATTR_USER_KEY position {i_user_key}")
            cu.error(f"  Row: {row_idx}, Email: {email}")
            cu.error(f"  Group index: {k}, Group: {g}")
            cu.error(f"  Start pos: {start_pos}, Target idx: {target_idx}")
            raise ValueError(f"Attempting to write past ATTR_USER_KEY")
        
        mismatch_sync.at[row_idx, target_col] = g
        filled_count += 1

cu.success(f"Filled {filled_count} group values into mismatch_sync.")

# Defragment after modifications
mismatch_sync = mismatch_sync.copy()

cu.success("Finished appending GAD groups into mismatch_sync.")

# Final verification
final_cols = list(mismatch_sync.columns)
final_i_user_key = final_cols.index("ATTR_USER_KEY")
cu.info(f"Final verification: ATTR_USER_KEY at position {final_i_user_key}")

# Check if any columns after ATTR_USER_KEY have group data
cols_after_user_key = final_cols[final_i_user_key+1:]
for col in cols_after_user_key:
    non_null = mismatch_sync[col].notna().sum()
    if non_null > 0:
        cu.error(f"WARNING: Column '{col}' after ATTR_USER_KEY has {non_null} non-null values!")




Max groups needed: 509
ℹ️ Most demanding rows: [{'row_idx': 1491, 'email': 'user1@samsung.com', 'last_filled': 4, 'start_pos': 5, 'new_groups_count': 51, 'needed': 56}, {'row_idx': 1861, 'email':
'user2@samsung.com', 'last_filled': 8, 'start_pos': 9, 'new_groups_count': 52, 'needed': 61}, {'row_idx': 1914, 'email': 'john.doe@samsung.com', 'last_filled': 11, 
'start_pos': 12, 'new_groups_count': 497, 'needed': 509}]
ℹ️ Need to add 333 new columns
ℹ️ Added 333 new columns before ATTR_USER_KEY
ℹ️ Total group columns available: 509
ℹ️ ATTR_GROUPS at position: 4
ℹ️ ATTR_USER_KEY at position: 513
ℹ️ First 5 group cols: ['ATTR_GROUPS', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 8']
ℹ️ Last 5 group cols: ['Unnamed: 508', 'Unnamed: 509', 'Unnamed: 510', 'Unnamed: 511', 'Unnamed: 512']
✅ Filled 3193 group values into mismatch_sync.
✅ Finished appending GAD groups into mismatch_sync.
ℹ️ Final verification: ATTR_USER_KEY at position 513
❌ WARNING: Column 'ATTR_PROFILE_PICTURE_FILENAME' after ATTR_USER_KEY has 123 non-null values!
❌ WARNING: Column 'ATTR_IS_SYSADMIN' after ATTR_USER_KEY has 123 non-null values!
❌ WARNING: Column 'autoGroupsAdded' after ATTR_USER_KEY has 14 non-null values!
❌ WARNING: Column 'invalidPasswordAttempts' after ATTR_USER_KEY has 12 non-null values!
❌ WARNING: Column 'ATTR_CONFLUENCE_LAST_AUTHENTICATED' after ATTR_USER_KEY has 25 non-null values!
❌ WARNING: Column 'ATTR_LASTNAME' after ATTR_USER_KEY has 123 non-null values!
❌ WARNING: Column 'ATTR_IS_ADMIN' after ATTR_USER_KEY has 123 non-null values!
❌ WARNING: Column 'ATTR_LAST_KNOWN_ACTIVITY' after ATTR_USER_KEY has 25 non-null values!
❌ WARNING: Column 'ATLAS_USER_TIMESTAMP' after ATTR_USER_KEY has 4 non-null values!
❌ WARNING: Column 'CONFLUENCE.UD.position' after ATTR_USER_KEY has 1 non-null values!
❌ WARNING: Column 'ATTR_FIRSTNAME' after ATTR_USER_KEY has 123 non-null values!
❌ WARNING: Column 'CONFLUENCE.UD.department' after ATTR_USER_KEY has 1 non-null values!
❌ WARNING: Column 'ATTR_IS_IGNORE_ACTIVE_ON_UPDATE' after ATTR_USER_KEY has 4 non-null values!
❌ WARNING: Column 'CONFLUENCE.UD.location' after ATTR_USER_KEY has 1 non-null values!
❌ WARNING: Column 'lastAuthenticated' after ATTR_USER_KEY has 13 non-null values!
❌ WARNING: Column 'ATTR_ID' after ATTR_USER_KEY has 123 non-null values!
❌ WARNING: Column 'ATTR_LAST_AUTH_CONSOLIDATED' after ATTR_USER_KEY has 25 non-null values!
Users in previously filtered rows:: 293
Final Count of Users to be Imported:: 416


