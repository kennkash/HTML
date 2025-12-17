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
    if v is None:
        return False
    s = str(v).strip()
    return s != "" and s.lower() != "nan"

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

# We'll append groups row-by-row, adding new Unnamed columns if needed
for row_idx in range(len(mismatch_sync)):
    email = mismatch_sync.at[row_idx, "ATTR_EMAIL"]
    new_groups = email_to_groups.get(email, [])

    if not new_groups:
        continue

    # Recompute cols each loop in case we added new columns earlier
    cols = list(mismatch_sync.columns)
    i_groups = cols.index("ATTR_GROUPS")
    i_user_key = cols.index("ATTR_USER_KEY")
    group_cols = cols[i_groups:i_user_key]  # includes ATTR_GROUPS + Unnamed... up to before ATTR_USER_KEY

    # Find last filled group column in this row
    last_filled_pos = -1
    for j, c in enumerate(group_cols):
        if _is_filled(mismatch_sync.at[row_idx, c]):
            last_filled_pos = j

    start_pos = last_filled_pos + 1  # first empty after the last filled

    # Make sure we have enough group columns to place all new groups
    needed_len = start_pos + len(new_groups)
    if needed_len > len(group_cols):
        to_add = needed_len - len(group_cols)
        for _ in range(to_add):
            new_col = _next_unnamed_name(mismatch_sync.columns)
            # Insert right before ATTR_USER_KEY to keep the "between" constraint
            insert_at = list(mismatch_sync.columns).index("ATTR_USER_KEY")
            mismatch_sync.insert(insert_at, new_col, pd.NA)

        # refresh group_cols after inserting
        cols = list(mismatch_sync.columns)
        i_groups = cols.index("ATTR_GROUPS")
        i_user_key = cols.index("ATTR_USER_KEY")
        group_cols = cols[i_groups:i_user_key]

    # Write the new groups into the next available group columns
    for k, g in enumerate(new_groups):
        target_col = group_cols[start_pos + k]
        mismatch_sync.at[row_idx, target_col] = g

cu.success("Finished appending GAD groups into mismatch_sync.")