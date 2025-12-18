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
    
    # Refresh cols for this row
    current_cols = list(mismatch_sync.columns)
    current_i_groups = current_cols.index("ATTR_GROUPS")
    current_i_user_key = current_cols.index("ATTR_USER_KEY")
    group_cols = current_cols[current_i_groups:current_i_user_key]
    
    # Count existing filled columns for this row
    last_filled_pos = -1
    for j, c in enumerate(group_cols):
        if _is_filled(mismatch_sync.at[row_idx, c]):
            last_filled_pos = j
    
    # Start position (never overwrite ATTR_GROUPS itself)
    start_pos = max(last_filled_pos + 1, 1)
    needed = start_pos + len(new_groups)
    max_groups_needed = max(max_groups_needed, needed)

# ------------------------------------------------------------------
# CREATE ALL NEEDED COLUMNS AT ONCE (before ATTR_USER_KEY)
# ------------------------------------------------------------------
# Refresh column indices
cols = list(mismatch_sync.columns)
i_groups = cols.index("ATTR_GROUPS")
i_user_key = cols.index("ATTR_USER_KEY")
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
    
    # CRITICAL FIX: Insert at the SAME position each time (right before ATTR_USER_KEY)
    # because each insert shifts everything to the right
    for i in range(cols_to_add):
        new_col = f"Unnamed: {max_n + 1 + i}"
        # Always insert at the current position of ATTR_USER_KEY (it moves right with each insert)
        insert_at = list(mismatch_sync.columns).index("ATTR_USER_KEY")
        mismatch_sync.insert(insert_at, new_col, pd.NA)
    
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
        cu.error(f"Row {row_idx}: Not enough group columns! Need {start_pos + len(new_groups)}, have {len(group_cols)}")
        cu.error(f"Email: {email}, Groups to add: {len(new_groups)}, Start position: {start_pos}")
        raise ValueError(f"Insufficient group columns for row {row_idx}")

    # Write groups into the next available slots
    for k, g in enumerate(new_groups):
        target_col = group_cols[start_pos + k]
        mismatch_sync.at[row_idx, target_col] = g

# Defragment after modifications
mismatch_sync = mismatch_sync.copy()

cu.success("Finished appending GAD groups into mismatch_sync.")
