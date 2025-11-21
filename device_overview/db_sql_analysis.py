from django.db import connections

DEVICE_ALIAS = "device_db"

# Harte Liste der DACH-Sitecodes (kannst du später in eine Tabelle auslagern)
DACH_SITES = [
    "ARW", "ALS", "ARB", "BYR", "BRL", "BR2", "BEH", "BER", "BLF", "BRB", "BRM",
    "BRN", "DMM", "DAM", "DED", "DLN", "DPH", "DRT", "DRS", "DUS", "EDM", "ETR",
    "ETF", "ESC", "ES2", "ESP", "ERB", "FR2", "FR4", "FRK", "FRD", "FRT", "GEL",
    "GCH", "GC2", "GTH", "GRN", "GDN", "HNV", "HN2", "HN3", "HN4", "HLD", "HCO",
    "IGS", "KRL", "KSM", "KVL", "KOB", "ELS", "KSC", "KRS", "KRZ", "LNG", "LN2",
    "LN3", "LSN", "LBR", "LMF", "LVK", "LAT", "LHR", "MGD", "MNN", "MNH", "MH2",
    "MND", "MGG", "MNC", "MN2", "NKR", "NEU", "NDR", "NRN", "NR2", "NRB", "OBR",
    "PAS", "PEN", "PEI", "PFL", "PFN", "RAD", "RIZ", "RVS", "RGN", "RG2", "SBR",
    "SCB", "SCM", "SCN", "SCF", "SCW", "SC2", "SEL", "SNN", "SMM", "SND", "STY",
    "STT", "THY", "THN", "TRS", "UEB", "UNT", "VNN", "VN2", "VLK", "WGN", "WRD",
    "WTZ", "WTT", "WTN", "WLF", "WUE", "ZEU", "ZUG",
]


def _get_connection():
    return connections[DEVICE_ALIAS]


def fetch_device_rows(filters):
    """
    Holt die Zeilen aus device_flat inkl. Filter:
    - dach_only (bool)
    - ci_status (str | None)
    - tier3 (str | None)
    - search (str | None)
    """

    base_sql = """
        SELECT
            PL_NAME,
            REGION,
            COMPANY,
            SITEGROUP,
            SITE,
            ROOM,
            PHYSICALPOSITION,
            SHORTDESCRIPTION,
            DEPARTMENT,
            OWNED_BY,
            USED_BY,
            SUPPORTED_BY,
            PL_COST_CENTER,
            PL_STATUS,
            RELATION,
            DESTINATION_CLASSID,
            TIER1,
            TIER2,
            TIER3,
            MODEL,
            MANUFACTURERNAME,
            SERIALNUMBER,
            CI_ROOM,
            FLOOR,
            PARTNUMBER,
            SUPPLIERNAME,
            CI_STATUS,
            PURCHASE_DATE,
            RECEIVED_DATE,
            INSTALLATION_DATE,
            AVAILABLE_DATE,
            RETURN_DATE,
            DISPOSAL_DATE,
            MARK_AS_DELETED,
            CREATE_DATE,
            MODIFIED_DATE,
            ROLE,
            CHILDNAME,
            CONFBASICNUMBER,
            BUILDNUMBER,
            TYPE,
            ADDITIONAL_INFORMATION,
            DEPOT,
            SUPPORTED
        FROM device_flat
        WHERE 1=1
    """

    params = []
    conditions = []

    if filters.get("dach_only"):
        placeholders = ", ".join(["%s"] * len(DACH_SITES))
        conditions.append(f"SITE IN ({placeholders})")
        params.extend(DACH_SITES)

    if filters.get("ci_status"):
        conditions.append("CI_STATUS = %s")
        params.append(filters["ci_status"])

    if filters.get("tier3"):
        conditions.append("TIER3 = %s")
        params.append(filters["tier3"])

    if filters.get("search"):
        search = f"%{filters['search']}%"
        conditions.append(
            """
            (
                PL_NAME LIKE %s OR
                SHORTDESCRIPTION LIKE %s OR
                MODEL LIKE %s OR
                SERIALNUMBER LIKE %s
            )
            """
        )
        params.extend([search] * 4)

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    base_sql += " ORDER BY SITE, PL_NAME, SERIALNUMBER"

    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute(base_sql, params)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]

    return columns, rows


def fetch_filter_options():
    """
    Liest die Werte für die Dropdowns aus device_flat:
    - CI_STATUS
    - TIER3
    - DACH-Sites, die in den Daten wirklich vorkommen
    """
    conn = _get_connection()
    with conn.cursor() as cur:
        # CI-Status
        cur.execute(
            """
            SELECT DISTINCT CI_STATUS
            FROM device_flat
            WHERE CI_STATUS IS NOT NULL AND CI_STATUS <> ''
            ORDER BY CI_STATUS
        """
        )
        ci_statuses = [row[0] for row in cur.fetchall()]

        # Tier3
        cur.execute(
            """
            SELECT DISTINCT TIER3
            FROM device_flat
            WHERE TIER3 IS NOT NULL AND TIER3 <> ''
            ORDER BY TIER3
        """
        )
        tier3_values = [row[0] for row in cur.fetchall()]

        # DACH-Sites, die es in den Daten wirklich gibt
        placeholders = ", ".join(["%s"] * len(DACH_SITES))
        cur.execute(
            f"""
            SELECT DISTINCT SITE
            FROM device_flat
            WHERE SITE IN ({placeholders})
            ORDER BY SITE
        """,
            DACH_SITES,
        )
        dach_sites_in_db = [row[0] for row in cur.fetchall()]

    return {
        "ci_statuses": ci_statuses,
        "tier3_values": tier3_values,
        "dach_sites": dach_sites_in_db,
    }


def fetch_counts_by_site(filters):
    """
    Aggregation: Anzahl Geräte pro Site (mit denselben Filtern).
    """
    base_sql = """
        SELECT SITE, COUNT(*) AS device_count
        FROM device_flat
        WHERE 1=1
    """

    params = []
    conditions = []

    if filters.get("dach_only"):
        placeholders = ", ".join(["%s"] * len(DACH_SITES))
        conditions.append(f"SITE IN ({placeholders})")
        params.extend(DACH_SITES)

    if filters.get("ci_status"):
        conditions.append("CI_STATUS = %s")
        params.append(filters["ci_status"])

    if filters.get("tier3"):
        conditions.append("TIER3 = %s")
        params.append(filters["tier3"])

    if filters.get("search"):
        search = f"%{filters['search']}%"
        conditions.append(
            """
            (
                PL_NAME LIKE %s OR
                SHORTDESCRIPTION LIKE %s OR
                MODEL LIKE %s OR
                SERIALNUMBER LIKE %s
            )
            """
        )
        params.extend([search] * 4)

    if conditions:
        base_sql += " AND " + " AND ".join(conditions)

    base_sql += " GROUP BY SITE ORDER BY SITE"

    conn = _get_connection()
    with conn.cursor() as cur:
        cur.execute(base_sql, params)
        rows = cur.fetchall()

    # Fürs Frontend einfaches Dict
    return [{"site": r[0], "count": r[1]} for r in rows]
