# device_overview/db_sql.py

import csv
import io

from django.db import connections, transaction

# Alias aus settings.DATABASES (zweite DB = MariaDB)
DEVICE_ALIAS = "device_db"

# Spalten der CSV und der Staging-Tabelle WKPBE_Test_kurz
CSV_COLUMNS = [
    "PL_NAME",
    "REGION",
    "COMPANY",
    "SITEGROUP",
    "SITE",
    "ROOM",
    "PHYSICALPOSITION",
    "SHORTDESCRIPTION",
    "DEPARTMENT",
    "OWNED_BY",
    "USED_BY",
    "SUPPORTED_BY",
    "PL_COST_CENTER",
    "PL_STATUS",
    "RELATION",
    "DESTINATION_CLASSID",
    "TIER1",
    "TIER2",
    "TIER3",
    "MODEL",
    "MANUFACTURERNAME",
    "CI_NAME",
    "SERIALNUMBER",
    "CI_ID",
    "BUDGETCODE",
    "CI_ROOM",
    "FLOOR",
    "PARTNUMBER",
    "SUPPLIERNAME",
    "CI_STATUS",
    "PURCHASE_DATE",
    "RECEIVED_DATE",
    "INSTALLATION_DATE",
    "AVAILABLE_DATE",
    "RETURN_DATE",
    "DISPOSAL_DATE",
    "MARK_AS_DELETED",
    "CREATE_DATE",
    "MODIFIED_DATE",
    "ROLE",
    "CHILDNAME",
    "CONFBASICNUMBER",
    "BUILDNUMBER",
    "TYPE",
    "ADDITIONAL_INFORMATION",
    "DEPOT",
    "SUPPORTED",
]


def _conn():
    return connections[DEVICE_ALIAS]


def clear_all_tables():
    """
    Leert alle fachlichen Tabellen + Staging, löscht aber nichts.
    (1:1 Model–Partnumber, keine Zwischentabelle mehr.)
    """
    sql = """
    SET FOREIGN_KEY_CHECKS = 0;
    TRUNCATE TABLE devices;
    TRUNCATE TABLE partnumbers;
    TRUNCATE TABLE models;
    TRUNCATE TABLE tbltier3;
    TRUNCATE TABLE tbltier2;
    TRUNCATE TABLE tbltier1;
    TRUNCATE TABLE manufacturers;
    TRUNCATE TABLE rooms;
    TRUNCATE TABLE sites;
    TRUNCATE TABLE regions;
    TRUNCATE TABLE cost_centers;
    TRUNCATE TABLE suppliers;
    TRUNCATE TABLE departments;
    TRUNCATE TABLE pl_names;
    TRUNCATE TABLE owned_bys;
    TRUNCATE TABLE used_bys;
    TRUNCATE TABLE supported_bys;
    TRUNCATE TABLE relations;
    TRUNCATE TABLE types;
    TRUNCATE TABLE depots;
    TRUNCATE TABLE tblpl_status;
    TRUNCATE TABLE tblci_status;
    TRUNCATE TABLE WKPBE_Test_kurz;
    SET FOREIGN_KEY_CHECKS = 1;
    """
    with _conn().cursor() as cur:
        for stmt in sql.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt + ";")


def import_csv_to_staging(csv_file):
    """
    CSV (mit ';') in die Staging-Tabelle WKPBE_Test_kurz laden.

    Voraussetzung:
    - WKPBE_Test_kurz hat GENAU die Spalten aus CSV_COLUMNS.
    """
    data = csv_file.read().decode("utf-8-sig")
    f = io.StringIO(data)

    reader = csv.DictReader(f, delimiter=";")

    insert_sql = """
        INSERT INTO WKPBE_Test_kurz (
            {cols}
        ) VALUES (
            {placeholders}
        )
    """.format(
        cols=", ".join(CSV_COLUMNS),
        placeholders=", ".join(["%s"] * len(CSV_COLUMNS)),
    )

    rows = []
    for row in reader:
        # komplett leere Zeilen ignorieren
        if not any(row.values()):
            continue

        values = [
            (row.get(col, "") or "").strip()
            for col in CSV_COLUMNS
        ]
        rows.append(values)

    with _conn().cursor() as cur:
        cur.execute("TRUNCATE TABLE WKPBE_Test_kurz;")
        if rows:
            cur.executemany(insert_sql, rows)


def populate_normalized_from_staging():
    """
    Füllt die normalisierte Struktur aus WKPBE_Test_kurz.
    Variante mit 1:1 Model–Partnumber (models.partnumber_id als FK).
    """
    conn = _conn()

    with transaction.atomic(using=DEVICE_ALIAS):
        with conn.cursor() as cur:
            # Regions
            cur.execute("""
                INSERT INTO regions (region)
                SELECT DISTINCT REGION
                FROM WKPBE_Test_kurz
                WHERE REGION IS NOT NULL AND REGION <> '';
            """)

            # Sites
            cur.execute("""
                INSERT INTO sites (company, sitegroup, site, region_id)
                SELECT DISTINCT
                    t.COMPANY,
                    t.SITEGROUP,
                    t.SITE,
                    r.region_id
                FROM WKPBE_Test_kurz t
                JOIN regions r ON r.region = t.REGION
                WHERE t.SITE IS NOT NULL AND t.SITE <> '';
            """)

            # Rooms
            cur.execute("""
                INSERT INTO rooms (room, physicalposition, ci_room, floor, site_id)
                SELECT DISTINCT
                    t.ROOM,
                    t.PHYSICALPOSITION,
                    t.CI_ROOM,
                    t.FLOOR,
                    s.site_id
                FROM WKPBE_Test_kurz t
                JOIN sites s ON s.site = t.SITE
                WHERE
                    (t.ROOM IS NOT NULL AND t.ROOM <> '')
                    OR (t.CI_ROOM IS NOT NULL AND t.CI_ROOM <> '');
            """)

            # einfache Lookup-Tabellen
            cur.execute("""
                INSERT INTO pl_names (pl_name)
                SELECT DISTINCT PL_NAME
                FROM WKPBE_Test_kurz
                WHERE PL_NAME IS NOT NULL AND PL_NAME <> '';
            """)

            cur.execute("""
                INSERT INTO owned_bys (owned_by)
                SELECT DISTINCT OWNED_BY
                FROM WKPBE_Test_kurz
                WHERE OWNED_BY IS NOT NULL AND OWNED_BY <> '';
            """)

            cur.execute("""
                INSERT INTO used_bys (used_by)
                SELECT DISTINCT USED_BY
                FROM WKPBE_Test_kurz
                WHERE USED_BY IS NOT NULL AND USED_BY <> '';
            """)

            cur.execute("""
                INSERT INTO supported_bys (supported_by)
                SELECT DISTINCT SUPPORTED_BY
                FROM WKPBE_Test_kurz
                WHERE SUPPORTED_BY IS NOT NULL AND SUPPORTED_BY <> '';
            """)

            cur.execute("""
                INSERT INTO suppliers (suppliername)
                SELECT DISTINCT SUPPLIERNAME
                FROM WKPBE_Test_kurz
                WHERE SUPPLIERNAME IS NOT NULL AND SUPPLIERNAME <> '';
            """)

            cur.execute("""
                INSERT INTO departments (department)
                SELECT DISTINCT DEPARTMENT
                FROM WKPBE_Test_kurz
                WHERE DEPARTMENT IS NOT NULL AND DEPARTMENT <> '';
            """)

            cur.execute("""
                INSERT INTO cost_centers (pl_cost_center)
                SELECT DISTINCT PL_COST_CENTER
                FROM WKPBE_Test_kurz
                WHERE PL_COST_CENTER IS NOT NULL AND PL_COST_CENTER <> '';
            """)

            cur.execute("""
                INSERT INTO manufacturers (manufacturername)
                SELECT DISTINCT MANUFACTURERNAME
                FROM WKPBE_Test_kurz
                WHERE MANUFACTURERNAME IS NOT NULL AND MANUFACTURERNAME <> '';
            """)

            cur.execute("""
                INSERT INTO tbltier1 (tier1)
                SELECT DISTINCT TIER1
                FROM WKPBE_Test_kurz
                WHERE TIER1 IS NOT NULL AND TIER1 <> '';
            """)

            cur.execute("""
                INSERT INTO tbltier2 (tier2)
                SELECT DISTINCT TIER2
                FROM WKPBE_Test_kurz
                WHERE TIER2 IS NOT NULL AND TIER2 <> '';
            """)

            cur.execute("""
                INSERT INTO tbltier3 (tier3)
                SELECT DISTINCT TIER3
                FROM WKPBE_Test_kurz
                WHERE TIER3 IS NOT NULL AND TIER3 <> '';
            """)

            cur.execute("""
                INSERT INTO relations (relation)
                SELECT DISTINCT RELATION
                FROM WKPBE_Test_kurz
                WHERE RELATION IS NOT NULL AND RELATION <> '';
            """)

            cur.execute("""
                INSERT INTO types (type)
                SELECT DISTINCT TYPE
                FROM WKPBE_Test_kurz
                WHERE TYPE IS NOT NULL AND TYPE <> '';
            """)

            cur.execute("""
                INSERT INTO depots (depot)
                SELECT DISTINCT DEPOT
                FROM WKPBE_Test_kurz
                WHERE DEPOT IS NOT NULL AND DEPOT <> '';
            """)

            cur.execute("""
                INSERT INTO tblpl_status (pl_status)
                SELECT DISTINCT PL_STATUS
                FROM WKPBE_Test_kurz
                WHERE PL_STATUS IS NOT NULL AND PL_STATUS <> '';
            """)

            cur.execute("""
                INSERT INTO tblci_status (ci_status)
                SELECT DISTINCT CI_STATUS
                FROM WKPBE_Test_kurz
                WHERE CI_STATUS IS NOT NULL AND CI_STATUS <> '';
            """)

            # Partnumbers vor Models
            cur.execute("""
                INSERT INTO partnumbers (partnumber)
                SELECT DISTINCT PARTNUMBER
                FROM WKPBE_Test_kurz
                WHERE PARTNUMBER IS NOT NULL AND PARTNUMBER <> '';
            """)

            # Models direkt mit partnumber_id
            cur.execute("""
                INSERT INTO models (manu_id, tier1_id, tier2_id, tier3_id, model, partnumber_id)
                SELECT DISTINCT
                    man.manu_id,
                    t1.tier1_id,
                    t2.tier2_id,
                    t3.tier3_id,
                    t.MODEL,
                    p.partnumber_id
                FROM WKPBE_Test_kurz t
                LEFT JOIN manufacturers man ON man.manufacturername = t.MANUFACTURERNAME
                LEFT JOIN tbltier1       t1  ON t1.tier1 = t.TIER1
                LEFT JOIN tbltier2       t2  ON t2.tier2 = t.TIER2
                LEFT JOIN tbltier3       t3  ON t3.tier3 = t.TIER3
                LEFT JOIN partnumbers    p   ON p.partnumber = t.PARTNUMBER
                WHERE t.MODEL IS NOT NULL AND t.MODEL <> '';
            """)

            # Devices (Model -> Partnumber kommt über models.partnumber_id)
            cur.execute("""
                INSERT INTO devices (
                  serialnumber, shortdescription, destination_classid,
                  purchase_date, received_date, installation_date,
                  available_date, return_date, disposal_date,
                  mark_as_deleted, create_date, modified_date,
                  role, childname, confbuildnumber, buildnumber,
                  additional_information, supported,
                  pl_name_id, owner_id, supporter_id, user_id,
                  model_id, costcenter_id, supplier_id,
                  room_id, relation_id, department_id,
                  type_id, depot_id, pl_status_id, ci_status_id
                )
                SELECT
                  t.SERIALNUMBER,
                  t.SHORTDESCRIPTION,
                  t.DESTINATION_CLASSID,
                  t.PURCHASE_DATE,
                  t.RECEIVED_DATE,
                  t.INSTALLATION_DATE,
                  t.AVAILABLE_DATE,
                  t.RETURN_DATE,
                  t.DISPOSAL_DATE,
                  t.MARK_AS_DELETED,
                  t.CREATE_DATE,
                  t.MODIFIED_DATE,
                  t.ROLE,
                  t.CHILDNAME,
                  t.CONFBASICNUMBER,
                  t.BUILDNUMBER,
                  t.ADDITIONAL_INFORMATION,
                  t.SUPPORTED,
                  pn.pl_name_id,
                  ob.owner_id,
                  sb.supporter_id,
                  ub.user_id,
                  m.model_id,
                  cc.cc_id,
                  sup.supplier_id,
                  r.room_id,
                  rel.relation_id,
                  dept.department_id,
                  ty.type_id,
                  dp.depot_id,
                  pls.pl_status_id,
                  cis.ci_status_id
                FROM WKPBE_Test_kurz t
                LEFT JOIN pl_names       pn   ON pn.pl_name        = t.PL_NAME
                LEFT JOIN owned_bys      ob   ON ob.owned_by       = t.OWNED_BY
                LEFT JOIN supported_bys  sb   ON sb.supported_by   = t.SUPPORTED_BY
                LEFT JOIN used_bys       ub   ON ub.used_by        = t.USED_BY
                LEFT JOIN cost_centers   cc   ON cc.pl_cost_center = t.PL_COST_CENTER
                LEFT JOIN suppliers      sup  ON sup.suppliername  = t.SUPPLIERNAME
                LEFT JOIN departments    dept ON dept.department   = t.DEPARTMENT
                LEFT JOIN relations      rel  ON rel.relation      = t.RELATION
                LEFT JOIN types          ty   ON ty.type           = t.TYPE
                LEFT JOIN depots         dp   ON dp.depot          = t.DEPOT
                LEFT JOIN tblpl_status   pls  ON pls.pl_status     = t.PL_STATUS
                LEFT JOIN tblci_status   cis  ON cis.ci_status     = t.CI_STATUS
                LEFT JOIN manufacturers  man  ON man.manufacturername = t.MANUFACTURERNAME
                LEFT JOIN tbltier1       t1   ON t1.tier1          = t.TIER1
                LEFT JOIN tbltier2       t2   ON t2.tier2          = t.TIER2
                LEFT JOIN tbltier3       t3   ON t3.tier3          = t.TIER3
                LEFT JOIN models         m    ON m.model           = t.MODEL
                                               AND m.manu_id       = man.manu_id
                                               AND m.tier1_id      = t1.tier1_id
                                               AND m.tier2_id      = t2.tier2_id
                                               AND m.tier3_id      = t3.tier3_id
                LEFT JOIN sites          s    ON s.site = t.SITE
                LEFT JOIN rooms          r    ON r.site_id = s.site_id
                               AND IFNULL(r.room, '')    = IFNULL(t.ROOM, '')
                               AND IFNULL(r.ci_room, '') = IFNULL(t.CI_ROOM, '');
            """)


def recreate_device_flat_view():
    """
    View device_flat neu anlegen (1:1 Model–Partnumber, kein zwPartnumbersModels mehr).
    """
    sql = """
    DROP VIEW IF EXISTS device_flat;
    CREATE VIEW device_flat AS
    SELECT
      pn.pl_name                         AS PL_NAME,
      rg.region                          AS REGION,
      s.company                          AS COMPANY,
      s.sitegroup                        AS SITEGROUP,
      s.site                             AS SITE,
      r.room                             AS ROOM,
      r.physicalposition                 AS PHYSICALPOSITION,
      d.shortdescription                 AS SHORTDESCRIPTION,
      dept.department                    AS DEPARTMENT,
      ob.owned_by                        AS OWNED_BY,
      ub.used_by                         AS USED_BY,
      sb.supported_by                    AS SUPPORTED_BY,
      cc.pl_cost_center                  AS PL_COST_CENTER,
      pls.pl_status                      AS PL_STATUS,
      rel.relation                       AS RELATION,
      d.destination_classid              AS DESTINATION_CLASSID,
      t1.tier1                           AS TIER1,
      t2.tier2                           AS TIER2,
      t3.tier3                           AS TIER3,
      m.model                            AS MODEL,
      manu.manufacturername              AS MANUFACTURERNAME,
      d.serialnumber                     AS SERIALNUMBER,
      r.ci_room                          AS CI_ROOM,
      r.floor                            AS FLOOR,
      pnbr.partnumber                    AS PARTNUMBER,
      sup.suppliername                   AS SUPPLIERNAME,
      cis.ci_status                      AS CI_STATUS,
      d.purchase_date                    AS PURCHASE_DATE,
      d.received_date                    AS RECEIVED_DATE,
      d.installation_date                AS INSTALLATION_DATE,
      d.available_date                   AS AVAILABLE_DATE,
      d.return_date                      AS RETURN_DATE,
      d.disposal_date                    AS DISPOSAL_DATE,
      d.mark_as_deleted                  AS MARK_AS_DELETED,
      d.create_date                      AS CREATE_DATE,
      d.modified_date                    AS MODIFIED_DATE,
      d.role                             AS `ROLE`,
      d.childname                        AS CHILDNAME,
      d.confbuildnumber                  AS CONFBASICNUMBER,
      d.buildnumber                      AS BUILDNUMBER,
      tp.type                            AS `TYPE`,
      d.additional_information           AS ADDITIONAL_INFORMATION,
      dp.depot                           AS DEPOT,
      d.supported                        AS SUPPORTED
    FROM devices d
    LEFT JOIN pl_names          pn   ON pn.pl_name_id      = d.pl_name_id
    LEFT JOIN owned_bys         ob   ON ob.owner_id        = d.owner_id
    LEFT JOIN used_bys          ub   ON ub.user_id         = d.user_id
    LEFT JOIN supported_bys     sb   ON sb.supporter_id    = d.supporter_id
    LEFT JOIN cost_centers      cc   ON cc.cc_id           = d.costcenter_id
    LEFT JOIN tblpl_status      pls  ON pls.pl_status_id   = d.pl_status_id
    LEFT JOIN tblci_status      cis  ON cis.ci_status_id   = d.ci_status_id
    LEFT JOIN relations         rel  ON rel.relation_id    = d.relation_id
    LEFT JOIN departments       dept ON dept.department_id = d.department_id
    LEFT JOIN rooms             r    ON r.room_id          = d.room_id
    LEFT JOIN sites             s    ON s.site_id          = r.site_id
    LEFT JOIN regions           rg   ON rg.region_id       = s.region_id
    LEFT JOIN types             tp   ON tp.type_id         = d.type_id
    LEFT JOIN depots            dp   ON dp.depot_id        = d.depot_id
    LEFT JOIN models            m    ON m.model_id         = d.model_id
    LEFT JOIN partnumbers       pnbr ON pnbr.partnumber_id = m.partnumber_id
    LEFT JOIN manufacturers     manu ON manu.manu_id       = m.manu_id
    LEFT JOIN tbltier1          t1   ON t1.tier1_id        = m.tier1_id
    LEFT JOIN tbltier2          t2   ON t2.tier2_id        = m.tier2_id
    LEFT JOIN tbltier3          t3   ON t3.tier3_id        = m.tier3_id
    LEFT JOIN suppliers         sup  ON sup.supplier_id    = d.supplier_id;
    """
    with _conn().cursor() as cur:
        for stmt in sql.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt + ";")
