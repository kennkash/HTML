import pandas as pd
from prettiprint import ConsoleUtils, box
from pathlib import Path

cu = ConsoleUtils(theme="dark", verbosity=2)


def coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    If df has duplicate column names (common with 'Unnamed:*' from CSV),
    coalesce duplicates by taking the first non-null value across the duplicate
    columns, and keep only one column with the original name.
    """
    cols = list(df.columns)
    seen = set()
    out = pd.DataFrame(index=df.index)

    # Precompute which labels are duplicated
    col_index = pd.Index(cols)
    duplicated_labels = {c for c in col_index.unique() if (col_index == c).sum() > 1}

    for c in cols:
        if c in seen:
            continue
        seen.add(c)

        if c in duplicated_labels:
            # df.loc[:, c] returns a DataFrame when c is duplicated
            block = df.loc[:, c]
            # take first non-null from left to right
            out[c] = block.bfill(axis=1).iloc[:, 0]
        else:
            out[c] = df[c]

    return out

# ------------------------------------------------------------------
# CSV Paths
# ------------------------------------------------------------------
gad_path = "/mnt/k.kashmiry/zdrive/conf_gad_users.csv"
ldap_path = "/mnt/k.kashmiry/zdrive/conf_ldap_users.csv"
user_sync = "/mnt/k.kashmiry/zdrive/CONFUSERSYNC.csv"
gad_groups = "/mnt/k.kashmiry/zdrive/gad_groups.csv"

# ------------------------------------------------------------------
# Load the CSVs
# ------------------------------------------------------------------
gad_users = pd.read_csv(gad_path)
ldap_users = pd.read_csv(ldap_path)
sync = pd.read_csv(user_sync, dtype=str, low_memory=False)

# ------------------------------------------------------------------
# Normalise the columns we will compare
# ------------------------------------------------------------------
# ---- e-mail (GAD) -------------------------------------------------
gad_users["email_address"] = (
    gad_users["email_address"]
    .astype(str)  # protect against NaN
    .str.strip()
    .str.lower()
)

# ---- e-mail (LDAP) ------------------------------------------------
ldap_users["EMAIL"] = (
    ldap_users["EMAIL"]
    .astype(str)
    .str.strip()
    .str.lower()
)

# ---- user-name ----------------------------------------------------
# LDAP column is USER_NAME, GAD column is user_name
ldap_users["USER_NAME"] = (
    ldap_users["USER_NAME"]
    .astype(str)
    .str.strip()
    .str.lower()
)

gad_users["user_name"] = (
    gad_users["user_name"]
    .astype(str)
    .str.strip()
    .str.lower()
)

# ---- e-mail (sync) -----------------------------------------------
sync["ATTR_EMAIL"] = sync["ATTR_EMAIL"].astype(str).str.strip().str.lower()

# ------------------------------------------------------------------
# LDAP rows missing from GAD (first requirement)
# ------------------------------------------------------------------
ldap_missing_in_gad = ldap_users[
    ~ldap_users["EMAIL"].isin(gad_users["email_address"])
].copy()

cu.header("Finding Users Not in GAD...")
cu.key_value("LDAP-Only Rows (users not in GAD):", f"{len(ldap_missing_in_gad)}")
print(ldap_missing_in_gad)

cu.table(
    headers=list(ldap_missing_in_gad.columns),
    rows=ldap_missing_in_gad.values.tolist(),
    title="Users not in GAD",
    header_style="bold purple",
    expand=True,
    table_box=box.ROUNDED,
)

# ------------------------------------------------------------------
# Filter the Raw User Sync file to the users missing in GAD
# ------------------------------------------------------------------
cu.header("Filtering the Raw User Sync File to Users Missing in GAD")

missing_set = set(ldap_missing_in_gad["EMAIL"])
filtered_sync = sync[sync["ATTR_EMAIL"].isin(missing_set)].copy()

cu.key_value("Users Missing in GAD:", f"{len(filtered_sync)}")
print(filtered_sync)

# ------------------------------------------------------------------
# Filter the Raw User Sync file to only users who have logged in
# ------------------------------------------------------------------
cu.header("Filtering the Filtered User Sync File to Users Who Have Logged In")

col = "ATTR_CONFLUENCE_LAST_AUTHENTICATED"

cu.key_value("Users Who Have Never Logged In:", f"{filtered_sync[col].isna().sum()}")

logged_in_filtered_sync = filtered_sync[filtered_sync[col].notna()].copy()

cu.key_value("Rows Kept After Removing Null Logins", f"{len(logged_in_filtered_sync)}")
print(logged_in_filtered_sync)

# ------------------------------------------------------------------
# Same e-mail in both, but different usernames (second requirement)
# ------------------------------------------------------------------
cu.header("Finding Users that Exist in Both LDAP & GAD")

merged = pd.merge(
    ldap_users,
    gad_users,
    left_on="EMAIL",
    right_on="email_address",
    how="inner",
    suffixes=("_ldap", "_gad"),  # creates USER_NAME_ldap and user_name_gad
)

cu.panel("Finding Duplicate Users With Different Usernames", expand=False)

# Keep only the rows where the usernames differ
username_mismatch = merged[
    merged["USER_NAME"] != merged["user_name"]
][["EMAIL", "USER_NAME", "user_name"]].reset_index(drop=True)

cu.key_value(
    "Username Mismatches (same email, different usernames):",
    f"{len(username_mismatch)}",
)
print(username_mismatch)

cu.table(
    headers=list(username_mismatch.columns),
    rows=username_mismatch.values.tolist(),
    title="Duplicate users with username mismatch",
    header_style="bold purple",
    expand=True,
    table_box=box.ROUNDED,
)

# ------------------------------------------------------------------
# Filter the Raw User Sync file to duplicate users,
# replace username with username being used in GAD Directory
# ------------------------------------------------------------------
cu.header("Filtering the Raw User Sync File to Users Who Have An Account in Both Directories")

# clean keys
sync["ATTR_EMAIL"] = sync["ATTR_EMAIL"].str.lower().str.strip()
username_mismatch["EMAIL"] = username_mismatch["EMAIL"].str.lower().str.strip()

# keep only rows that belong to the mismatch set
mismatch_emails = set(username_mismatch["EMAIL"])
mismatch_sync = sync[sync["ATTR_EMAIL"].isin(mismatch_emails)].copy()

cu.panel("Raw Sync Data of Duplicate Users", expand=False)
print(mismatch_sync)

# replace the name
lookup = username_mismatch.set_index("EMAIL")["user_name"]
mismatch_sync["ATTR_NAME"] = mismatch_sync["ATTR_EMAIL"].map(lookup)

cu.panel("Raw Sync Data of Duplicate Users - Updated Usernames", expand=False)
print(mismatch_sync)
print(mismatch_sync.columns)

# ------------------------------------------------------------------
# NEW: Append GAD groups into the Unnamed group columns BETWEEN
#      ATTR_GROUPS and ATTR_USER_KEY (never after ATTR_USER_KEY)
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

def _is_filled(v) -> bool:
    # Handle scalar values
    if not isinstance(v, pd.Series):
        if pd.isna(v):
            return False
        return str(v).strip() != ""

    # Handle Series (duplicate columns case)
    return v.notna().any()
    
def _max_unnamed_num(existing_cols):
    max_n = -1
    for c in existing_cols:
        if isinstance(c, str) and c.startswith("Unnamed:"):
            try:
                n = int(c.split(":", 1)[1].strip())
                max_n = max(max_n, n)
            except Exception:
                pass
    return max_n

# PREALLOCATE needed Unnamed columns BEFORE ATTR_USER_KEY (no insert in loop)
cols = list(mismatch_sync.columns)
try:
    i_groups = cols.index("ATTR_GROUPS")
    i_user_key = cols.index("ATTR_USER_KEY")
except ValueError as e:
    raise ValueError(
        "Could not find required columns in mismatch_sync. "
        "Expected to find 'ATTR_GROUPS' and 'ATTR_USER_KEY'."
    ) from e

# group columns are strictly between ATTR_GROUPS and ATTR_USER_KEY
group_cols_initial = cols[i_groups:i_user_key]

max_unn = _max_unnamed_num(cols)

# Compute the maximum number of group columns we will need (between ATTR_GROUPS and ATTR_USER_KEY)
required_group_len = len(group_cols_initial)

for idx in mismatch_sync.index:
    email = mismatch_sync.at[idx, "ATTR_EMAIL"]
    new_groups = email_to_groups.get(email, [])
    if not new_groups:
        continue

    # find last filled position for THIS row in the current group slice
    last_filled_pos = -1
    for j, c in enumerate(group_cols_initial):
        if _is_filled(mismatch_sync.at[idx, c]):
            last_filled_pos = j

    # always start writing into the first Unnamed column (pos 1), never overwrite ATTR_GROUPS
    start_pos = max(last_filled_pos + 1, 1)

    needed_len = start_pos + len(new_groups)
    required_group_len = max(required_group_len, needed_len)

# If we need more columns, create them and force them to appear BEFORE ATTR_USER_KEY
to_add = required_group_len - len(group_cols_initial)
if to_add > 0:
    new_cols = []
    for _ in range(to_add):
        max_unn += 1
        new_cols.append(f"Unnamed: {max_unn}")

    # Create a small DF of empty columns, then concat (fast, no fragmentation)
    extra = pd.DataFrame({c: pd.NA for c in new_cols}, index=mismatch_sync.index)
    mismatch_sync = pd.concat([mismatch_sync, extra], axis=1)

    # Now rebuild the column order so the new Unnamed columns are inserted BEFORE ATTR_USER_KEY
    cols_now = list(mismatch_sync.columns)
    i_user_key_now = cols_now.index("ATTR_USER_KEY")

    before_user_key = cols_now[:i_user_key_now]
    after_user_key = cols_now[i_user_key_now:]  # includes ATTR_USER_KEY and anything after

    # remove the new cols from wherever concat put them (end), and place before ATTR_USER_KEY
    before_user_key_wo_new = [c for c in before_user_key if c not in new_cols]
    new_order = before_user_key_wo_new + new_cols + after_user_key

    mismatch_sync = mismatch_sync.reindex(columns=new_order)

# refresh slice after preallocation/reorder
cols = list(mismatch_sync.columns)
i_groups = cols.index("ATTR_GROUPS")
i_user_key = cols.index("ATTR_USER_KEY")
group_cols = cols[i_groups:i_user_key]  # guaranteed long enough and strictly before ATTR_USER_KEY

# Fill groups row-by-row (no column inserts here)
for row_idx in mismatch_sync.index:
    email = mismatch_sync.at[row_idx, "ATTR_EMAIL"]
    new_groups = email_to_groups.get(email, [])
    if not new_groups:
        continue

    # find last filled in the FINAL group_cols slice
    last_filled_pos = -1
    for j, c in enumerate(group_cols):
        if _is_filled(mismatch_sync.at[row_idx, c]):
            last_filled_pos = j

    start_pos = max(last_filled_pos + 1, 1)  # start at first Unnamed column

    for k, g in enumerate(new_groups):
        mismatch_sync.at[row_idx, group_cols[start_pos + k]] = g

# sanity check: no Unnamed columns after ATTR_USER_KEY (warn if present)
cols = list(mismatch_sync.columns)
i_user_key = cols.index("ATTR_USER_KEY")
bad = [c for c in cols[i_user_key + 1:] if isinstance(c, str) and c.startswith("Unnamed:")]
if bad:
    cu.warning(f"Unnamed columns exist after ATTR_USER_KEY (these will NOT be used for groups): {bad}")

cu.success("Finished appending GAD groups into mismatch_sync.")

# ------------------------------------------------------------------
# remove possible duplicates from the already-filtered set
# ------------------------------------------------------------------
remaining_logged = logged_in_filtered_sync[~logged_in_filtered_sync["ATTR_EMAIL"].isin(mismatch_emails)]
cu.key_value("Users in previously filtered rows:", f"{len(remaining_logged)}")

# ------------------------------------------------------------------
# final concatenation
# ------------------------------------------------------------------
# Fix duplicate column-name issue before concat (prevents "reindexing only valid..." error)
remaining_logged = coalesce_duplicate_columns(remaining_logged)
mismatch_sync = coalesce_duplicate_columns(mismatch_sync)

final_sync = (
    pd.concat([remaining_logged, mismatch_sync], ignore_index=True)
    .drop_duplicates("ATTR_EMAIL")
)

cu.key_value("Final Count of Users to be Imported:", f"{len(final_sync)}")
cu.spacer()
cu.panel(
    f"{len(remaining_logged) + len(mismatch_sync)}",
    title="Final Count Check (user count should equal this):",
    box="DOUBLE",
    border_style="bold red",
    padding=1,
)

# ------------------------------------------------------------------
# Save the results
# ------------------------------------------------------------------
ldap_missing_in_gad.to_csv("/mnt/k.kashmiry/zdrive/ldap_missing_in_gad.csv", index=False)
username_mismatch.to_csv("/mnt/k.kashmiry/zdrive/email_username_mismatches.csv", index=False)

# --------------------------------------------------------------
# Path where the workbook should be saved
# --------------------------------------------------------------
out_folder = Path("/mnt/k.kashmiry/zdrive")
out_file = out_folder / "final_sync.xlsx"

# --------------------------------------------------------------
# Make sure the directory exists (mkdir with parents=True is safe)
# --------------------------------------------------------------
out_folder.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------
# Save the DataFrame as an Excel workbook
# --------------------------------------------------------------
final_sync.to_excel(
    out_file,
    index=False,       # do not write the pandas index as a column
    engine="openpyxl", # optional – explicit engine declaration
)

cu.success(f"Excel file written to: {out_file}")
cu.info(f"File size: {out_file.stat().st_size / 1_024:.1f} KiB")
cu.header("Sample of the written file:")
print(final_sync.head())