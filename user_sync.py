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
    
    # Count existing filled columns for this row
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
# CREATE ALL NEEDED COLUMNS AT ONCE (before ATTR_USER_KEY)
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
    
    # Insert all needed columns at once, right before ATTR_USER_KEY
    insert_at = list(mismatch_sync.columns).index("ATTR_USER_KEY")
    for i in range(cols_to_add):
        new_col = f"Unnamed: {max_n + 1 + i}"
        mismatch_sync.insert(insert_at + i, new_col, pd.NA)

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



import pandas as pd
from prettiprint import ConsoleUtils
from pathlib import Path
cu = ConsoleUtils(theme="dark", verbosity=2)
# pd.set_option('display.max_columns', 12) 
pd.reset_option('display.max_columns')
# ------------------------------------------------------------------
# CSV Paths
# ------------------------------------------------------------------

gad_path  = "/mnt/k.kashmiry/zdrive/conf_gad_users.csv"
ldap_path = "/mnt/k.kashmiry/zdrive/conf_ldap_users.csv"
user_sync = "/mnt/k.kashmiry/zdrive/CONFUSERSYNC.csv"
gad_groups = "/mnt/k.kashmiry/zdrive/gad_groups.csv"

# ------------------------------------------------------------------
# Load the CSVs
# ------------------------------------------------------------------

gad_users  = pd.read_csv(gad_path)
ldap_users = pd.read_csv(ldap_path)
sync = pd.read_csv(user_sync, dtype=str, low_memory=False)
gad_groups_df = pd.read_csv(gad_groups, dtype=str, low_memory=False)

# ------------------------------------------------------------------
# Normalise the columns we will compare
# ------------------------------------------------------------------
# ---- e‑mail -------------------------------------------------------

gad_users["email_address"] = (
    gad_users["email_address"]
    .astype(str)          # protect against NaN
    .str.strip()
    .str.lower()
)

ldap_users["EMAIL"] = (
    ldap_users["EMAIL"]
    .astype(str)
    .str.strip()
    .str.lower()
)

# ---- user‑name ----------------------------------------------------
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

# ---- e‑mail -------------------------------------------------------
sync["ATTR_EMAIL"] = sync["ATTR_EMAIL"].astype(str).str.strip().str.lower()

# ---- e‑mail/group_name -------------------------------------------------------
gad_groups_df["email_add"] = gad_groups_df["email_add"].astype(str).str.strip().str.lower()
gad_groups_df["group_name"] = gad_groups_df["group_name"].astype(str).str.strip()

# ------------------------------------------------------------------
# LDAP rows **missing** from GAD (first requirement)
# ------------------------------------------------------------------

ldap_missing_in_gad = ldap_users[
    ~ldap_users["EMAIL"].isin(gad_users["email_address"])
].copy()


cu.header("Finding Users Not in GAD...")
cu.key_value("LDAP‑Only Rows (users not in GAD):", f"{len(ldap_missing_in_gad)}")
print(ldap_missing_in_gad)

# cu.table(
#     headers=list(ldap_missing_in_gad.columns),
#     rows=ldap_missing_in_gad.values.tolist(),
#     title="Users not in GAD",
#     header_style="bold purple",
#     expand=True,
#     table_box=box.ROUNDED,
# )

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
# Same e‑mail in both, but different usernames (second requirement)
# ------------------------------------------------------------------
# Inner merge on the e‑mail column, keeping both username columns side‑by‑side

cu.header("Finding Users that Exist in Both LDAP & GAD")

merged = pd.merge(
    ldap_users,
    gad_users,
    left_on="EMAIL",
    right_on="email_address",
    how="inner",
    suffixes=("_ldap", "_gad")   # creates USER_NAME_ldap and user_name_gad
)


cu.panel("Finding Duplicate Users With Different Usernames", expand=False)

# Keep only the rows where the usernames differ
username_mismatch = merged[
    merged["USER_NAME"] != merged["user_name"]
][["EMAIL", "USER_NAME", "user_name"]].reset_index(drop=True)


cu.key_value("Username Mismatches (same email, different usernames):", f"{len(username_mismatch)}")
print(username_mismatch)

# cu.table(
#     headers=list(username_mismatch.columns),
#     rows=username_mismatch.values.tolist(),
#     title="Duplicate users with username mismatch",
#     header_style="bold purple",
#     expand=True,
#     table_box=box.ROUNDED,
# )

# ------------------------------------------------------------------
# Filter the Raw User Sync file to duplicate users, 
# replace username with username being used in GAD Directory
# ------------------------------------------------------------------
cu.header("Filtering the Raw User Sync File to Users Who Have An Account in Both Directories")

# clean keys
sync["ATTR_EMAIL"]   = sync["ATTR_EMAIL"].str.lower().str.strip()
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



# remove possible duplicates from the already‑filtered set
remaining_logged = logged_in_filtered_sync[~logged_in_filtered_sync["ATTR_EMAIL"].isin(mismatch_emails)]
cu.key_value("Users in previously filtered rows:", f"{len(remaining_logged)}")

# final concatenation
final_sync = pd.concat([remaining_logged, mismatch_sync], ignore_index=True).drop_duplicates("ATTR_EMAIL")
cu.key_value("Final Count of Users to be Imported:", f"{len(final_sync)}")
cu.spacer()
cu.panel(f"{len(remaining_logged + mismatch_sync)}", title="Final Count Check (user count should equal this):", box="DOUBLE", border_style="bold red", padding=1)
# ------------------------------------------------------------------
# Save the results
# ------------------------------------------------------------------
# ldap_missing_in_gad.to_csv("/mnt/k.kashmiry/zdrive/ldap_missing_in_gad.csv", index=False)
# username_mismatch.to_csv("/mnt/k.kashmiry/zdrive/email_username_mismatches.csv", index=False)

# --------------------------------------------------------------
# Path where the workbook should be saved
# --------------------------------------------------------------
out_folder = Path("/mnt/k.kashmiry/zdrive")
out_file   = out_folder / "final_sync_test.xlsx"     

# --------------------------------------------------------------
# Make sure the directory exists (mkdir with parents=True is safe)
# --------------------------------------------------------------
out_folder.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------------------
# Save the DataFrame as an Excel workbook
# --------------------------------------------------------------
final_sync.to_excel(
    out_file,
    index=False,          # do not write the pandas index as a column
    engine="openpyxl"     # optional – explicit engine declaration
)

cu.success(f"Excel file written to: {out_file}")
cu.info(f"File size: {out_file.stat().st_size / 1_024:.1f} KiB")
cu.header("Sample of the written file:")
print(final_sync.head())

