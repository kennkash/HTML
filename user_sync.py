# ------------------------------------------------------------------
# Append GAD groups (gad_groups.csv) into the "Unnamed" group columns
# between ATTR_GROUPS and ATTR_USER_KEY for mismatch_sync
# ------------------------------------------------------------------
cu.header("Appending GAD Groups for Duplicate Users (mismatch_sync)")

gad_groups_df = pd.read_csv(gad_groups, dtype=str, low_memory=False)

# normalize
gad_groups_df["email_add"] = gad_groups_df["email_add"].astype(str).str.strip().str.lower()
gad_groups_df["group_name"] = gad_groups_df["group_name"].astype(str).str.strip()

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
    # FIX: treat pandas NA/NaN as empty
    if pd.isna(v):
        return False
    s = str(v).strip()
    return s != ""

def _next_unnamed_name(existing_cols):
    # Find the max "Unnamed: N" currently present; start after it
    max_n = -1
    for c in existing_cols:
        if isinstance(c, str) and c.startswith("Unnamed:"):
            try:
                n = int(c.split(":", 1)[1].strip())
                max_n = max(max_n, n)
            except Exception:
                pass
    return f"Unnamed: {max_n + 1}"
    
# ------------------------------------------------------------------
# FIX: If the raw sync has Unnamed:* columns AFTER ATTR_USER_KEY,
# move them to immediately BEFORE ATTR_USER_KEY so NO groups live after it.
# ------------------------------------------------------------------
cols = list(mismatch_sync.columns)
i_user_key = cols.index("ATTR_USER_KEY")

unnamed_after = [c for c in cols[i_user_key+1:] if isinstance(c, str) and c.startswith("Unnamed:")]
if unnamed_after:
    cu.info(f"Moving {len(unnamed_after)} Unnamed columns from after ATTR_USER_KEY to before it.")
    cols_before_user_key = cols[:i_user_key]
    cols_after_user_key = [c for c in cols[i_user_key+1:] if c not in unnamed_after]

    new_order = cols_before_user_key + unnamed_after + ["ATTR_USER_KEY"] + cols_after_user_key
    mismatch_sync = mismatch_sync.reindex(columns=new_order)

# Append groups row-by-row; any needed new columns are inserted BEFORE ATTR_USER_KEY
for row_idx in mismatch_sync.index:
    email = mismatch_sync.at[row_idx, "ATTR_EMAIL"]
    new_groups = email_to_groups.get(email, [])

    if not new_groups:
        continue

    # Recompute after any prior inserts
    cols = list(mismatch_sync.columns)
    i_groups = cols.index("ATTR_GROUPS")
    i_user_key = cols.index("ATTR_USER_KEY")
    group_cols = cols[i_groups:i_user_key]  # STRICTLY between ATTR_GROUPS and ATTR_USER_KEY

    # Find the last filled group column for THIS ROW ONLY
    last_filled_pos = -1
    for j, c in enumerate(group_cols):
        if _is_filled(mismatch_sync.at[row_idx, c]):
            last_filled_pos = j

    # FIX: always start writing into the first Unnamed column (never overwrite ATTR_GROUPS itself)
    start_pos = max(last_filled_pos + 1, 1)

    # Ensure enough columns exist BETWEEN ATTR_GROUPS and ATTR_USER_KEY
    needed_len = start_pos + len(new_groups)
    if needed_len > len(group_cols):
        to_add = needed_len - len(group_cols)
        for _ in range(to_add):
            new_col = _next_unnamed_name(mismatch_sync.columns)

            # Insert immediately BEFORE ATTR_USER_KEY (so groups never appear after it)
            insert_at = list(mismatch_sync.columns).index("ATTR_USER_KEY")
            mismatch_sync.insert(insert_at, new_col, pd.NA)

        # refresh slice after inserting
        cols = list(mismatch_sync.columns)
        i_groups = cols.index("ATTR_GROUPS")
        i_user_key = cols.index("ATTR_USER_KEY")
        group_cols = cols[i_groups:i_user_key]

    # Write groups into the next available slots BETWEEN ATTR_GROUPS and ATTR_USER_KEY
    for k, g in enumerate(new_groups):
        target_col = group_cols[start_pos + k]
        mismatch_sync.at[row_idx, target_col] = g

# Defragment after many inserts (avoids the fragmentation warning)
mismatch_sync = mismatch_sync.copy()

cu.success("Finished appending GAD groups into mismatch_sync.")