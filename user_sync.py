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

for row_idx in mismatch_sync.index:
    email = mismatch_sync.at[row_idx, "ATTR_EMAIL"]
    new_groups = email_to_groups.get(email, [])
    
    if not new_groups:
        continue
    
    # Count existing filled columns for this row ONLY in the group section
    group_cols = cols[i_groups:i_user_key]
    last_filled_pos = -1
    for j, c in enumerate(group_cols):
        if _is_filled(mismatch_sync.at[row_idx, c]):
            last_filled_pos = j
    
    # Start position (never overwrite ATTR_GROUPS itself)
    start_pos = max(last_filled_pos + 1, 1)
    needed = start_pos + len(new_groups)
    max_groups_needed = max(max_groups_needed, needed)

# ------------------------------------------------------------------
# CREATE ALL NEEDED COLUMNS BY SPLITTING AND RECOMBINING THE DATAFRAME
# ------------------------------------------------------------------
current_group_cols = i_user_key - i_groups

if max_groups_needed > current_group_cols:
    cols_to_add = max_groups_needed - current_group_cols
    
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

# ------------------------------------------------------------------
# NOW FILL IN THE GROUPS (no more column insertions)
# ------------------------------------------------------------------
# Refresh column indices after insertions
cols = list(mismatch_sync.columns)
i_groups = cols.index("ATTR_GROUPS")
i_user_key = cols.index("ATTR_USER_KEY")
group_cols = cols[i_groups:i_user_key]

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

    # Write groups into the next available slots
    for k, g in enumerate(new_groups):
        target_col = group_cols[start_pos + k]
        mismatch_sync.at[row_idx, target_col] = g

# Defragment after modifications
mismatch_sync = mismatch_sync.copy()

cu.success("Finished appending GAD groups into mismatch_sync.")

# ------------------------------------------------------------------
# ALIGN COLUMNS before concatenation
# ------------------------------------------------------------------
# Add missing columns to remaining_logged with NA values, maintaining column order
mismatch_cols = list(mismatch_sync.columns)

for col in mismatch_cols:
    if col not in remaining_logged.columns:
        # Find the position where this column should be inserted
        col_idx = mismatch_cols.index(col)
        
        # Find the position in remaining_logged to insert (before the next column that exists)
        insert_before = None
        for next_col in mismatch_cols[col_idx+1:]:
            if next_col in remaining_logged.columns:
                insert_before = next_col
                break
        
        if insert_before:
            insert_at = list(remaining_logged.columns).index(insert_before)
            remaining_logged.insert(insert_at, col, pd.NA)
        else:
            # Add at the end
            remaining_logged[col] = pd.NA

# Reorder remaining_logged to match mismatch_sync column order
remaining_logged = remaining_logged[mismatch_sync.columns]

# remove possible duplicates from the already‑filtered set
remaining_logged = logged_in_filtered_sync[~logged_in_filtered_sync["ATTR_EMAIL"].isin(mismatch_emails)]
cu.key_value("Users in previously filtered rows:", f"{len(remaining_logged)}")

# final concatenation
final_sync = pd.concat([remaining_logged, mismatch_sync], ignore_index=True).drop_duplicates("ATTR_EMAIL")
cu.key_value("Final Count of Users to be Imported:", f"{len(final_sync)}")
cu.spacer()
cu.panel(f"{len(remaining_logged) + len(mismatch_sync)}", title="Final Count Check (user count should equal this):", box="DOUBLE", border_style="bold red", padding=1)
