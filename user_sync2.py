# remove possible duplicates from the already‑filtered set
remaining_logged = logged_in_filtered_sync[~logged_in_filtered_sync["ATTR_EMAIL"].isin(mismatch_emails)]
cu.key_value("Users in previously filtered rows:", f"{len(remaining_logged)}")

# ------------------------------------------------------------------
# CHECK remaining_logged structure before concatenation
# ------------------------------------------------------------------
cu.header("Checking remaining_logged DataFrame structure")
remaining_cols = list(remaining_logged.columns)
try:
    rl_i_user_key = remaining_cols.index("ATTR_USER_KEY")
    rl_i_last_auth = remaining_cols.index("ATTR_LAST_AUTH_CONSOLIDATED")
    cu.info(f"remaining_logged: ATTR_USER_KEY at position {rl_i_user_key}")
    cu.info(f"remaining_logged: ATTR_LAST_AUTH_CONSOLIDATED at position {rl_i_last_auth}")
    cu.info(f"remaining_logged: Total columns: {len(remaining_cols)}")
    
    # Check for unnamed columns after ATTR_LAST_AUTH_CONSOLIDATED
    rl_cols_after = remaining_cols[rl_i_last_auth+1:]
    rl_unnamed_after = [c for c in rl_cols_after if isinstance(c, str) and c.startswith("Unnamed:")]
    if rl_unnamed_after:
        cu.error(f"❌ remaining_logged has {len(rl_unnamed_after)} unnamed columns after ATTR_LAST_AUTH_CONSOLIDATED")
        cu.error(f"  First 10: {rl_unnamed_after[:10]}")
    else:
        cu.success("✅ remaining_logged structure is correct")
except ValueError as e:
    cu.error(f"Could not find required columns in remaining_logged: {e}")

# ------------------------------------------------------------------
# ALIGN COLUMNS before concatenation
# ------------------------------------------------------------------
cu.header("Aligning columns between mismatch_sync and remaining_logged")

# Get the column lists
mismatch_cols = list(mismatch_sync.columns)
remaining_cols = list(remaining_logged.columns)

cu.info(f"mismatch_sync has {len(mismatch_cols)} columns")
cu.info(f"remaining_logged has {len(remaining_cols)} columns")

# Find columns that exist in one but not the other
mismatch_only = set(mismatch_cols) - set(remaining_cols)
remaining_only = set(remaining_cols) - set(mismatch_cols)

if mismatch_only:
    cu.warning(f"Columns only in mismatch_sync ({len(mismatch_only)}): {list(mismatch_only)[:10]}")
if remaining_only:
    cu.warning(f"Columns only in remaining_logged ({len(remaining_only)}): {list(remaining_only)[:10]}")

# Add missing columns to remaining_logged with NA values, maintaining column order
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

cu.info(f"After alignment, remaining_logged has {len(remaining_logged.columns)} columns")

# Verify column order matches
if list(remaining_logged.columns) != list(mismatch_sync.columns):
    cu.error("❌ Column order still doesn't match after alignment!")
    
    # Reorder remaining_logged to match mismatch_sync
    cu.info("Reordering remaining_logged columns to match mismatch_sync...")
    remaining_logged = remaining_logged[mismatch_sync.columns]
    cu.success("✅ Columns reordered")

# final concatenation
final_sync = pd.concat([remaining_logged, mismatch_sync], ignore_index=True).drop_duplicates("ATTR_EMAIL")
cu.key_value("Final Count of Users to be Imported:", f"{len(final_sync)}")

# ------------------------------------------------------------------
# FINAL CHECK on final_sync
# ------------------------------------------------------------------
cu.header("Final check on concatenated DataFrame")
final_cols = list(final_sync.columns)
final_i_user_key = final_cols.index("ATTR_USER_KEY")
final_i_last_auth = final_cols.index("ATTR_LAST_AUTH_CONSOLIDATED")

cu.info(f"final_sync: ATTR_USER_KEY at position {final_i_user_key}")
cu.info(f"final_sync: ATTR_LAST_AUTH_CONSOLIDATED at position {final_i_last_auth}")
cu.info(f"final_sync: Total columns: {len(final_cols)}")

# Check for unnamed columns after ATTR_LAST_AUTH_CONSOLIDATED
final_cols_after = final_cols[final_i_last_auth+1:]
final_unnamed_after = [c for c in final_cols_after if isinstance(c, str) and c.startswith("Unnamed:")]
if final_unnamed_after:
    cu.error(f"❌ ERROR: final_sync has {len(final_unnamed_after)} unnamed columns after ATTR_LAST_AUTH_CONSOLIDATED!")
    cu.error(f"  First 10: {final_unnamed_after[:10]}")
    
    # Check which rows have data in these columns
    for col in final_unnamed_after[:3]:
        non_null = final_sync[col].notna().sum()
        if non_null > 0:
            cu.error(f"  Column '{col}' has {non_null} non-null values")
            sample_emails = final_sync[final_sync[col].notna()]["ATTR_EMAIL"].head(3).tolist()
            cu.error(f"    From users: {sample_emails}")
else:
    cu.success("✅ final_sync structure is correct - no unnamed columns after ATTR_LAST_AUTH_CONSOLIDATED!")
