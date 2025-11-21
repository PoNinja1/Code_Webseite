# device_overview/db_sql_reports.py

from django.db import connections

DEVICE_ALIAS = "device_db"

# Feste DACH-Sitecodes aus deiner Liste
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

# Tier3, die du im Report haben willst
TIER3_FILTER = [
    "Computer",
    "Notebook",
    "Notebook-Special",
    "ThinClient",
    "Workstation",
    "Workstation-Mobile",
]


def _conn():
    return connections[DEVICE_ALIAS]


def fetch_dach_deployed_t3_devices():
    """
    Report 1:
    - Nur DACH-Sites
    - CI_STATUS = 'Deployed'
    - TIER3 in definierter Liste
    -> komplette device_flat-Zeilen (Fake CSV)
    """
    conn = _conn()
    with conn.cursor() as cur:
        site_placeholders = ", ".join(["%s"] * len(DACH_SITES))
        tier_placeholders = ", ".join(["%s"] * len(TIER3_FILTER))

        sql = f"""
            SELECT *
            FROM device_flat
            WHERE
                SITE IN ({site_placeholders})
                AND CI_STATUS = %s
                AND TIER3 IN ({tier_placeholders})
            ORDER BY SITE, PL_NAME, SERIALNUMBER
        """

        params = list(DACH_SITES) + ["Deployed"] + list(TIER3_FILTER)
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]

    return columns, rows


def fetch_dach_deployed_t3_counts_by_site():
    """
    Report 2:
    Gleiche Filter wie oben, aber
    -> Aggregation nach SITE.
    """
    conn = _conn()
    with conn.cursor() as cur:
        site_placeholders = ", ".join(["%s"] * len(DACH_SITES))
        tier_placeholders = ", ".join(["%s"] * len(TIER3_FILTER))

        sql = f"""
            SELECT
                SITE,
                COUNT(*) AS device_count
            FROM device_flat
            WHERE
                SITE IN ({site_placeholders})
                AND CI_STATUS = %s
                AND TIER3 IN ({tier_placeholders})
            GROUP BY SITE
            ORDER BY SITE
        """

        params = list(DACH_SITES) + ["Deployed"] + list(TIER3_FILTER)
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [col[0] for col in cur.description]

    return columns, rows
