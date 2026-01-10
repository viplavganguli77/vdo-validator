import os
import io
import sqlite3
from typing import List, Dict, Tuple, Optional

import pandas as pd
import requests
import streamlit as st

# ==========================
# BASIC CONFIG
# ==========================
APP_TITLE = "V-Ads.txt-validator"

# Ensure database folder exists
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "database")
os.makedirs(DB_DIR, exist_ok=True)

DB_FILE = os.path.join(DB_DIR, "adsdata.db")
EXCEL_FILE = os.path.join(DB_DIR, "App-Demand Ops Ads.txt coverage (2).xlsx")

st.set_page_config(page_title=APP_TITLE, layout="wide")


# ==========================
# DB HELPERS
# ==========================
def get_conn():
    return sqlite3.connect(DB_FILE, check_same_thread=False)


def init_db_from_excel():
    """
    If adsdata.db does not exist or is empty, create it and populate from the Excel file.
    Excel structure:
      - First sheet = MASTER
          Col 0: Domain
          Col 3: Account Manager
          Col 6: Master ads.txt lines (all partners)
      - Last sheet = Account Manager List -> ignored
      - All sheets in between = Demand Partners
          Col 4: Integration Type (Prebid / VAST / Other)
          Col 6: Partner-specific ads.txt lines
    """
    if not os.path.exists(EXCEL_FILE):
        st.error(
            f"Excel file '{os.path.basename(EXCEL_FILE)}' not found in 'database/' folder.\n"
            f"Please place the file at:\n{EXCEL_FILE}"
        )
        st.stop()

    conn = get_conn()
    cur = conn.cursor()

    # Create tables
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS domains (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT UNIQUE,
            account_manager TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            integration_type TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS partner_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            partner_id INTEGER,
            line TEXT,
            FOREIGN KEY(partner_id) REFERENCES partners(id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS master_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            line TEXT UNIQUE
        )
        """
    )

    conn.commit()

    # If already populated, do nothing
    cur.execute("SELECT COUNT(*) FROM domains")
    if cur.fetchone()[0] > 0:
        conn.close()
        return

    # Read Excel
    xls = pd.ExcelFile(EXCEL_FILE)
    sheet_names = xls.sheet_names

    if len(sheet_names) < 2:
        st.error("Excel file must have at least MASTER + one partner sheet.")
        st.stop()

    master_sheet = sheet_names[0]
    last_sheet = sheet_names[-1]  # Account Manager List - ignore as partner

    master_df = xls.parse(master_sheet)

    # Safety: check columns exist
    if master_df.shape[1] < 7:
        st.error("MASTER sheet does not have enough columns (need at least 7).")
        st.stop()

    # MASTER: columns by position
    domains_col = master_df.iloc[:, 0]  # Column A
    am_col = master_df.iloc[:, 3]       # Column D
    master_lines_col = master_df.iloc[:, 6]  # Column G

    # Populate domains table
    for dom, am in zip(domains_col, am_col):
        if pd.isna(dom):
            continue
        domain = str(dom).strip().lower()
        account_manager = "" if pd.isna(am) else str(am).strip()
        if domain:
            cur.execute(
                "INSERT OR IGNORE INTO domains(domain, account_manager) VALUES (?, ?)",
                (domain, account_manager),
            )

    # Populate master_lines table
    for val in master_lines_col.dropna():
        text = str(val)
        for line in text.split("\n"):
            ln = line.strip().lower()
            if ln:
                cur.execute(
                    "INSERT OR IGNORE INTO master_lines(line) VALUES (?)", (ln,)
                )

    # Partner sheets: all between MASTER and last sheet
    for sheet in sheet_names[1:-1]:
        df = xls.parse(sheet)
        if df.shape[1] < 7:
            # Not enough columns, skip
            continue

        # Integration type: column E (index 4)
        integration_col = df.iloc[:, 4] if df.shape[1] > 4 else None
        integration_type = ""
        if integration_col is not None:
            non_null = integration_col.dropna()
            if not non_null.empty:
                integration_type = str(non_null.iloc[0]).strip()

        # Partner lines: column G (index 6)
        lines_series = df.iloc[:, 6].dropna()

        partner_name = sheet.strip()
        cur.execute(
            "INSERT OR IGNORE INTO partners(name, integration_type) VALUES (?, ?)",
            (partner_name, integration_type),
        )
        cur.execute("SELECT id FROM partners WHERE name = ?", (partner_name,))
        row = cur.fetchone()
        if not row:
            continue
        pid = row[0]

        for val in lines_series:
            text = str(val)
            for line in text.split("\n"):
                ln = line.strip().lower()
                if ln:
                    cur.execute(
                        "INSERT INTO partner_lines(partner_id, line) VALUES (?, ?)",
                        (pid, ln),
                    )
                    cur.execute(
                        "INSERT OR IGNORE INTO master_lines(line) VALUES (?)", (ln,)
                    )

    conn.commit()
    conn.close()


def get_all_domains_and_ams() -> Tuple[List[str], Dict[str, str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT domain, account_manager FROM domains ORDER BY domain")
    rows = cur.fetchall()
    conn.close()
    domains = [r[0] for r in rows]
    am_map = {r[0]: r[1] for r in rows}
    return domains, am_map


def get_all_partners() -> List[Tuple[int, str, str]]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id, name, integration_type FROM partners ORDER BY name")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_partner_lines(partner_id: int) -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT line FROM partner_lines WHERE partner_id = ?", (partner_id,))
    lines = [r[0] for r in cur.fetchall()]
    conn.close()
    # preserve insertion order, remove dups
    return list(dict.fromkeys(lines))


def get_partner_first_line(partner_id: int) -> Optional[str]:
    """Return the first line for a partner based on insertion order (Excel row order)."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT line FROM partner_lines WHERE partner_id = ? ORDER BY id ASC LIMIT 1",
        (partner_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_master_lines() -> List[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT line FROM master_lines ORDER BY id")
    lines = [r[0] for r in cur.fetchall()]
    conn.close()
    return list(dict.fromkeys(lines))


def get_master_first_line() -> Optional[str]:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT line FROM master_lines ORDER BY id ASC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def add_domain(domain: str, account_manager: str):
    conn = get_conn()
    cur = conn.cursor()
    d = domain.lower().strip()
    am = account_manager.strip()
    if not d:
        return
    cur.execute(
        "INSERT OR IGNORE INTO domains(domain, account_manager) VALUES (?, ?)", (d, am)
    )
    cur.execute("UPDATE domains SET account_manager = ? WHERE domain = ?", (am, d))
    conn.commit()
    conn.close()


def add_partner(name: str, integration_type: str, lines: List[str]):
    name = name.strip()
    integration_type = integration_type.strip()
    clean_lines = [ln.strip().lower() for ln in lines if ln.strip()]
    if not name or not clean_lines:
        return

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO partners(name, integration_type) VALUES (?, ?)",
        (name, integration_type),
    )
    cur.execute("SELECT id FROM partners WHERE name = ?", (name,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    pid = row[0]

    for ln in clean_lines:
        cur.execute(
            "INSERT INTO partner_lines(partner_id, line) VALUES (?, ?)", (pid, ln)
        )
        cur.execute("INSERT OR IGNORE INTO master_lines(line) VALUES (?)", (ln,))

    conn.commit()
    conn.close()


def delete_partners(names: List[str]):
    if not names:
        return
    conn = get_conn()
    cur = conn.cursor()
    for name in names:
        cur.execute("SELECT id FROM partners WHERE name = ?", (name,))
        row = cur.fetchone()
        if not row:
            continue
        pid = row[0]
        cur.execute("DELETE FROM partner_lines WHERE partner_id = ?", (pid,))
        cur.execute("DELETE FROM partners WHERE id = ?", (pid,))
    conn.commit()
    conn.close()


def replace_master_lines(new_lines: List[str]):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM master_lines")
    for ln in new_lines:
        cur.execute("INSERT OR IGNORE INTO master_lines(line) VALUES (?)", (ln,))
    conn.commit()
    conn.close()


# ====== NEW: PARTNER EDIT HELPERS ======
def rename_partner(old_name: str, new_name: str) -> bool:
    """Rename a partner. Returns False if new_name already exists."""
    conn = get_conn()
    cur = conn.cursor()
    new_name_clean = new_name.strip()
    if not new_name_clean:
        conn.close()
        return False

    # Check collision
    cur.execute("SELECT id FROM partners WHERE name = ?", (new_name_clean,))
    if cur.fetchone():
        conn.close()
        return False

    cur.execute("UPDATE partners SET name = ? WHERE name = ?", (new_name_clean, old_name))
    conn.commit()
    conn.close()
    return True


def update_partner_integration(name: str, integration_type: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE partners SET integration_type = ? WHERE name = ?",
        (integration_type.strip(), name),
    )
    conn.commit()
    conn.close()


def append_partner_lines(name: str, lines: List[str]):
    clean_lines = [ln.strip().lower() for ln in lines if ln.strip()]
    if not clean_lines:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM partners WHERE name = ?", (name,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    pid = row[0]
    for ln in clean_lines:
        cur.execute(
            "INSERT INTO partner_lines(partner_id, line) VALUES (?, ?)", (pid, ln)
        )
        cur.execute("INSERT OR IGNORE INTO master_lines(line) VALUES (?)", (ln,))
    conn.commit()
    conn.close()


def delete_partner_lines_by_values(name: str, lines: List[str]):
    if not lines:
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM partners WHERE name = ?", (name,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return
    pid = row[0]
    for ln in lines:
        cur.execute(
            "DELETE FROM partner_lines WHERE partner_id = ? AND line = ?",
            (pid, ln),
        )
    conn.commit()
    conn.close()


# ==========================
# CRAWLER / UTILS
# ==========================
def fetch_ads_txt(domain: str) -> List[str]:
    url = f"https://{domain}/ads.txt"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=7)
        if r.status_code != 200:
            return []
        lines = []
        for raw in r.text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            lines.append(line.lower())
        return lines
    except Exception:
        return []


def normalize_ads_line(line: str) -> str:
    """Normalize an ads.txt line for comparison (case + whitespace insensitive)."""
    return "".join(line.split()).lower()


def summarize_first_line(first_line: Optional[str], live_lines: List[str]) -> str:
    """Return ✅ if partner's first line appears in live ads.txt (normalized), else ❌."""
    if not first_line:
        return "N/A"
    first_norm = normalize_ads_line(first_line)
    live_norms = {normalize_ads_line(ln) for ln in live_lines}
    return "✅" if first_norm in live_norms else "❌"


# ==========================
# INIT DB
# ==========================
if not os.path.exists(DB_FILE):
    init_db_from_excel()

# ==========================
# LOAD DATA
# ==========================
all_domains, domain_am_map = get_all_domains_and_ams()
partner_rows = get_all_partners()
partner_name_map = {row[1]: (row[0], row[2]) for row in partner_rows}  # name -> (id, integration)
all_partner_names = sorted(partner_name_map.keys())
all_account_managers = sorted({am for am in domain_am_map.values() if am})

master_lines_list = get_master_lines()
prebid_partners = [name for (_id, name, it) in partner_rows if (it or "").lower() == "prebid"]
vast_partners = [name for (_id, name, it) in partner_rows if (it or "").lower() == "vast"]
prebid_count = len(prebid_partners)
vast_count = len(vast_partners)

# ==========================
# LIGHT / SIMPLE STYLING
# ==========================
st.markdown(
    """
<style>
.big-title {
    font-size:26px !important;
    font-weight:700 !important;
    color:#111111;
}
.info-box {
    padding:10px;
    background:#f5f5f5;
    border-radius:8px;
    border:1px solid #dddddd;
    margin-bottom:10px;
}
.metric-label {
    font-size:12px;
    color:#666666;
    font-weight:500;
    margin-bottom:2px;
}
.metric-value {
    font-size:16px;
    font-weight:700;
    color:#222222;
    margin-bottom:6px;
}
</style>
""",
    unsafe_allow_html=True,
)

# Title
st.markdown(f"<div class='big-title'>{APP_TITLE}</div>", unsafe_allow_html=True)
st.markdown("")

# ==========================
# LAYOUT: LEFT (filters) + RIGHT (IC)
# ==========================
col_left, col_info = st.columns([0.7, 0.3])

# ---------- RIGHT: INFORMATION CENTER ----------
with col_info:
    st.markdown("### IC")
    st.markdown("<div class='info-box'>", unsafe_allow_html=True)
    st.markdown("<div class='metric-label'>Total Domains</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='metric-value'>{len(all_domains)}</div>", unsafe_allow_html=True
    )

    st.markdown("<div class='metric-label'>Demand Partners</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='metric-value'>{len(all_partner_names)}</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<div class='metric-label'>Master Lines</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='metric-value'>{len(master_lines_list)}</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<div class='metric-label'>Prebid Partners</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='metric-value'>{prebid_count}</div>", unsafe_allow_html=True
    )

    st.markdown("<div class='metric-label'>VAST Partners</div>", unsafe_allow_html=True)
    st.markdown(
        f"<div class='metric-value'>{vast_count}</div>", unsafe_allow_html=True
    )
    st.markdown("</div>", unsafe_allow_html=True)

    # --- IC Details: Prebid / VAST lists, Master line editor, Partner manager ---
    with st.expander("Prebid Partners", expanded=False):
        if prebid_partners:
            st.write(", ".join(prebid_partners))
        else:
            st.caption("No Prebid partners found.")

    with st.expander("VAST Partners", expanded=False):
        if vast_partners:
            st.write(", ".join(vast_partners))
        else:
            st.caption("No VAST partners found.")

    with st.expander("Master Lines Editor", expanded=False):
        current_master = get_master_lines()
        master_text = "\n".join(current_master)
        edited_master = st.text_area(
            "Master Lines (one per line)",
            value=master_text,
            height=200,
            key="master_lines_editor",
        )

        col_ml1, col_ml2 = st.columns(2)
        with col_ml1:
            if st.button("Save Master Lines", key="save_master_lines"):
                new_lines = [
                    ln.strip().lower()
                    for ln in edited_master.splitlines()
                    if ln.strip()
                ]
                # remove duplicates, preserve order
                new_lines = list(dict.fromkeys(new_lines))
                replace_master_lines(new_lines)
                st.success("Master lines updated. (App will use new lines on next run.)")

        with col_ml2:
            dl_bytes = io.BytesIO(master_text.encode("utf-8"))
            st.download_button(
                label="Download Master Lines",
                data=dl_bytes,
                file_name="master_lines.txt",
                mime="text/plain",
                key="download_master_lines",
            )

    with st.expander("Demand Partners Manager", expanded=False):
        # Delete partners
        del_select = st.multiselect(
            "Select partners to delete from DB",
            options=all_partner_names,
            key="delete_partners_select",
        )
        if st.button("Delete Selected Partners", key="delete_partners_button"):
            if not del_select:
                st.warning("No partners selected.")
            else:
                delete_partners(del_select)
                st.success(
                    f"Deleted {len(del_select)} partner(s) from database. "
                    "They will disappear from lists on next run."
                )

        st.markdown("---")

        # Edit a single partner
        edit_partner = st.selectbox(
            "Select partner to edit",
            options=["(Select)"] + all_partner_names,
            key="edit_partner_select",
        )

        if edit_partner != "(Select)":
            pid, cur_integration = partner_name_map.get(edit_partner, (None, ""))
            if pid is not None:
                st.write(f"**Current Integration Type:** {cur_integration or 'Not set'}")

                # Update integration type
                new_it = st.selectbox(
                    "New Integration Type",
                    ["Prebid", "VAST", "Other"],
                    index=["Prebid", "VAST", "Other"].index(
                        (cur_integration or "Other")
                        if (cur_integration or "Other") in ["Prebid", "VAST", "Other"]
                        else "Other"
                    ),
                    key="edit_partner_integration_type",
                )
                if st.button("Update Integration Type", key="btn_update_integration"):
                    update_partner_integration(edit_partner, new_it)
                    st.success(
                        f"Integration type updated to '{new_it}' "
                        f"for partner '{edit_partner}'. (Refresh to see in IC.)"
                    )

                st.markdown("---")

                # Existing lines
                existing_lines = get_partner_lines(pid)
                if existing_lines:
                    st.caption("Existing ads.txt lines:")
                    st.text_area(
                        "Lines (read-only)",
                        value="\n".join(existing_lines),
                        height=150,
                        key="existing_partner_lines_view",
                    )
                else:
                    st.caption("This partner currently has no ads.txt lines in DB.")

                # Add new lines
                new_lines_text = st.text_area(
                    "Add new ads.txt lines (will be appended)",
                    key="partner_add_lines",
                )
                if st.button("Append New Lines", key="btn_append_lines"):
                    new_lines_list = [
                        ln for ln in new_lines_text.splitlines() if ln.strip()
                    ]
                    if not new_lines_list:
                        st.warning("No lines entered.")
                    else:
                        append_partner_lines(edit_partner, new_lines_list)
                        st.success(
                            f"Appended {len(new_lines_list)} line(s) to partner '{edit_partner}'."
                        )

                st.markdown("---")

                # Delete specific lines
                if existing_lines:
                    lines_to_delete = st.multiselect(
                        "Select ads.txt lines to delete",
                        options=existing_lines,
                        key="partner_delete_lines",
                    )
                    if st.button("Delete Selected Lines", key="btn_delete_lines"):
                        if not lines_to_delete:
                            st.warning("No lines selected for deletion.")
                        else:
                            delete_partner_lines_by_values(edit_partner, lines_to_delete)
                            st.success(
                                f"Deleted {len(lines_to_delete)} line(s) "
                                f"from partner '{edit_partner}'."
                            )

                st.markdown("---")

                # Rename partner
                new_partner_name = st.text_input(
                    "Rename partner (new name)",
                    key="rename_partner_input",
                ).strip()
                if st.button("Rename Partner", key="btn_rename_partner"):
                    if not new_partner_name:
                        st.warning("New partner name cannot be empty.")
                    elif new_partner_name in all_partner_names:
                        st.error(
                            f"A partner with name '{new_partner_name}' already exists."
                        )
                    else:
                        ok = rename_partner(edit_partner, new_partner_name)
                        if ok:
                            st.success(
                                f"Partner '{edit_partner}' renamed to '{new_partner_name}'. "
                                "Changes will appear in dropdowns on next refresh."
                            )
                        else:
                            st.error(
                                "Rename failed. It may be due to a name collision or DB issue."
                            )

# ---------- SIDEBAR: Manage Data ----------
st.sidebar.header("Manage Data")

# Add Domain
with st.sidebar.expander("➕ Add / Update Domain", expanded=False):
    new_dom = st.text_input("Domain (example.com)", key="new_domain_input").strip().lower()

    existing_am = st.selectbox(
        "Select existing Account Manager",
        ["(None)"] + all_account_managers,
        key="existing_am_select",
    )

    new_am_custom = st.text_input(
        "Or add new Account Manager", key="new_am_input"
    ).strip()

    if st.button("Save Domain", key="save_domain_button"):
        if not new_dom:
            st.sidebar.error("Domain cannot be empty.")
        else:
            if new_am_custom:
                am_to_use = new_am_custom
            elif existing_am != "(None)":
                am_to_use = existing_am
            else:
                am_to_use = ""

            add_domain(new_dom, am_to_use)
            st.sidebar.success(f"Domain '{new_dom}' saved successfully.")

# Edit existing domain's Account Manager
with st.sidebar.expander("✏️ Edit Domain", expanded=False):
    edit_dom = st.selectbox(
        "Select Domain",
        options=["(Select)"] + all_domains,
        key="edit_domain_select",
    )
    if edit_dom != "(Select)":
        current_am = domain_am_map.get(edit_dom, "") or "(None)"
        st.write(f"Current Account Manager: **{current_am}**")

        existing_am2 = st.selectbox(
            "Select existing Account Manager",
            ["(None)"] + all_account_managers,
            key="edit_existing_am_select",
        )
        new_am2 = st.text_input(
            "Or add new Account Manager",
            key="edit_new_am_input",
        ).strip()

        if st.button("Update Account Manager", key="btn_update_domain_am"):
            if new_am2:
                am_to_use2 = new_am2
            elif existing_am2 != "(None)":
                am_to_use2 = existing_am2
            else:
                am_to_use2 = ""

            add_domain(edit_dom, am_to_use2)
            st.success(
                f"Account Manager for domain '{edit_dom}' updated to '{am_to_use2 or '(None)'}'."
            )

# Add Partner
with st.sidebar.expander("➕ Add Demand Partner", expanded=False):
    new_partner = st.text_input("Partner Name", key="new_partner_input").strip()
    new_integration = st.selectbox(
        "Integration Type",
        ["Prebid", "VAST", "Other"],
        key="new_partner_integration",
    )
    new_lines = st.text_area(
        "Ads.txt Lines (one per line)", key="new_partner_lines"
    ).strip()

    if st.button("Save Partner", key="save_partner_button"):
        if not new_partner:
            st.sidebar.error("Partner name is required.")
        elif not new_lines:
            st.sidebar.error("Enter at least one ads.txt line.")
        else:
            line_list = [ln for ln in new_lines.splitlines() if ln.strip()]
            add_partner(new_partner, new_integration, line_list)
            st.sidebar.success(f"Partner '{new_partner}' added successfully.")

st.sidebar.markdown("---")
st.sidebar.caption("Developed by **Viplav Ganguli**")

# ==========================
# MAIN FILTER SECTION
# ==========================
with col_left:
    c1, c2, c3 = st.columns(3)

    with c1:
        selected_am = st.selectbox(
            "Account Manager",
            ["(All)"] + all_account_managers,
            key="filter_am",
        )

    with c2:
        selected_domains = st.multiselect(
            "Domains",
            all_domains,
            default=[],
            key="filter_domains",
        )

    with c3:
        integration_filter = st.selectbox(
            "Integration Type",
            ["(All)", "Prebid", "VAST", "Other"],
            key="filter_integration",
        )

    pasted_domains_text = st.text_area(
        "Paste Domains (optional)",
        placeholder="example.com, testsite.com\nOR\npaste one per line",
        height=80,
        key="pasted_domains",
    )

    selected_partners = st.multiselect(
        "Demand Partners",
        ["(All)"] + all_partner_names,
        default=["(All)"],
        key="filter_partners",
    )

    st.markdown("---")

    # ==========================
    # VALIDATION BUTTON
    # ==========================
    if st.button("Validate ads.txt", key="validate_button"):
        with st.spinner("Validating ads.txt for selected domains and partners..."):
            # --- Resolve domains selection ---
            pasted_domains = set()
            if pasted_domains_text.strip():
                raw_parts = pasted_domains_text.replace(",", "\n").split("\n")
                for part in raw_parts:
                    d = part.strip().lower()
                    if d:
                        pasted_domains.add(d)

            # base domains = selected from dropdown + pasted ones
            final_domains = set(selected_domains)
            final_domains.update(pasted_domains)

            # NEW: save pasted domains to DB if not already present (AM remains blank)
            for d in pasted_domains:
                if d and d not in all_domains:
                    add_domain(d, "")
                    all_domains.append(d)
                    domain_am_map.setdefault(d, "")

            # If nothing selected/pasted, use all DB domains
            if not final_domains:
                final_domains = set(all_domains)

            # Filter by account manager (only for domains that have AM in DB)
            if selected_am != "(All)":
                final_domains = {
                    d
                    for d in final_domains
                    if domain_am_map.get(d, "") == selected_am
                }

            final_domains = sorted(final_domains)

            if not final_domains:
                st.warning("No domains match the selection/filters.")
            else:
                # --- Resolve partners selection ---
                if "(All)" in selected_partners or not selected_partners:
                    chosen_partner_names = all_partner_names[:]
                else:
                    chosen_partner_names = [
                        p for p in selected_partners if p in all_partner_names
                    ]

                # Apply integration filter
                if integration_filter != "(All)":
                    filtered = []
                    for pname in chosen_partner_names:
                        pid, itype = partner_name_map.get(pname, (None, None))
                        if integration_filter.lower() == (itype or "").lower():
                            filtered.append(pname)
                    chosen_partner_names = filtered

                # Convert to partner ids
                chosen_partner_ids = [
                    partner_name_map[p][0]
                    for p in chosen_partner_names
                    if p in partner_name_map
                ]

                # Fetch live ads.txt for each domain only once
                domain_live_lines: Dict[str, List[str]] = {}
                for dom in final_domains:
                    domain_live_lines[dom] = fetch_ads_txt(dom)

                results = []
                present_by_domain: Dict[str, set] = {d: set() for d in final_domains}
                missing_by_domain: Dict[str, set] = {d: set() for d in final_domains}

                if chosen_partner_ids:
                    # Partner-specific summary: one row per domain × partner
                    for pname in chosen_partner_names:
                        pid, itype = partner_name_map.get(pname, (None, None))
                        if pid is None:
                            continue
                        expected_lines = get_partner_lines(pid)
                        if not expected_lines:
                            continue
                        first_line = get_partner_first_line(pid)

                        for dom in final_domains:
                            live_lines = domain_live_lines[dom]
                            live_norms = {
                                normalize_ads_line(ln) for ln in live_lines
                            }

                            domain_present = [
                                ln
                                for ln in expected_lines
                                if normalize_ads_line(ln) in live_norms
                            ]
                            domain_missing = [
                                ln
                                for ln in expected_lines
                                if normalize_ads_line(ln) not in live_norms
                            ]

                            present_by_domain[dom].update(domain_present)
                            missing_by_domain[dom].update(domain_missing)

                            results.append(
                                {
                                    "Domain": dom,
                                    "Account Manager": domain_am_map.get(dom, ""),
                                    "Demand Partner": pname,
                                    "Integration Type": itype or "",
                                    "Present Lines": len(domain_present),
                                    "Missing Lines": len(domain_missing),
                                    "First Line": summarize_first_line(
                                        first_line, live_lines
                                    ),
                                }
                            )
                else:
                    # No specific partners -> use MASTER lines
                    expected_lines = master_lines_list
                    master_first = get_master_first_line()

                    for dom in final_domains:
                        live_lines = domain_live_lines[dom]
                        live_norms = {
                            normalize_ads_line(ln) for ln in live_lines
                        }

                        domain_present = [
                            ln
                            for ln in expected_lines
                            if normalize_ads_line(ln) in live_norms
                        ]
                        domain_missing = [
                            ln
                            for ln in expected_lines
                            if normalize_ads_line(ln) not in live_norms
                        ]

                        present_by_domain[dom].update(domain_present)
                        missing_by_domain[dom].update(domain_missing)

                        results.append(
                            {
                                "Domain": dom,
                                "Account Manager": domain_am_map.get(dom, ""),
                                "Demand Partner": "MASTER (All)",
                                "Integration Type": "MASTER",
                                "Present Lines": len(domain_present),
                                "Missing Lines": len(domain_missing),
                                "First Line": summarize_first_line(
                                    master_first, live_lines
                                ),
                            }
                        )

                if not results:
                    st.warning("No ads.txt lines found for the current selection.")
                else:
                    # --- Summary: one table, domain → partner sorted ---
                    st.markdown("### Summary (Domain × Demand Partner)")
                    df_summary = pd.DataFrame(results)
                    df_summary = df_summary.sort_values(
                        by=["Domain", "Demand Partner"]
                    ).reset_index(drop=True)
                    st.dataframe(df_summary, use_container_width=True)

                    # --- DOWNLOAD SUMMARY SECTION ---
                    st.subheader("Download Summary")

                    # Excel in-memory
                    output_excel = io.BytesIO()
                    with pd.ExcelWriter(output_excel, engine="xlsxwriter") as writer:
                        df_summary.to_excel(
                            writer, index=False, sheet_name="Summary"
                        )
                    output_excel.seek(0)

                    # CSV in-memory
                    csv_data = df_summary.to_csv(index=False).encode("utf-8")

                    dl1, dl2 = st.columns(2)
                    with dl1:
                        st.download_button(
                            label="📥 Download Excel Summary",
                            data=output_excel,
                            file_name="ads_txt_summary.xlsx",
                            mime=(
                                "application/"
                                "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            ),
                            key="download_excel",
                        )
                    with dl2:
                        st.download_button(
                            label="📄 Download CSV Summary",
                            data=csv_data,
                            file_name="ads_txt_summary.csv",
                            mime="text/csv",
                            key="download_csv",
                        )

                    # --- Domain-specific combined lines ---
                    st.markdown("### Combined Present & Missing Lines (Domain Specific)")
                    for dom in final_domains:
                        st.markdown(f"#### Domain: `{dom}`")
                        colp, colm = st.columns(2)
                        present_text = (
                            "\n".join(sorted(present_by_domain.get(dom, set())))
                            if present_by_domain.get(dom)
                            else "No present lines"
                        )
                        missing_text = (
                            "\n".join(sorted(missing_by_domain.get(dom, set())))
                            if missing_by_domain.get(dom)
                            else "No missing lines"
                        )

                        with colp:
                            st.markdown(
                                "**Present Lines (Combined across selected partners)**"
                            )
                            st.text_area(
                                f"Present Lines - {dom}",
                                value=present_text,
                                height=150,
                                key=f"present_{dom}",
                            )
                        with colm:
                            st.markdown(
                                "**Missing Lines (Combined across selected partners)**"
                            )
                            st.text_area(
                                f"Missing Lines - {dom}",
                                value=missing_text,
                                height=150,
                                key=f"missing_{dom}",
                            )
    else:
        st.info(
            "Select any combination of filters (or paste domains) and click "
            "**Validate ads.txt**."
        )

