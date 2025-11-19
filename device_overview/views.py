import csv
import io

from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import connections, transaction
from django.shortcuts import render, redirect
from django.urls import reverse
from django.views import View

from .forms import CsvUploadForm


def get_device_cursor():
    """Cursor auf die MariaDB (device_db)."""
    return connections["device_db"].cursor()


class AnalysisView(LoginRequiredMixin, View):
    login_url = "login"

    def get(self, request):
        # Platzhalter – später mit echter Filterlogik füllen
        return render(request, "analysis.html")



class IndexView(View):
    """Startseite: Header + Hinweis / Upload-Hinweis (Logik im Template)."""

    def get(self, request):
        form = CsvUploadForm()
        return render(request, "index.html", {"upload_form": form})


# ---------- Helper-Funktionen für die MariaDB ----------

def _clear_all_tables():
    """
    Alle relevanten Tabellen in device_db leeren (TRUNCATE),
    aber nicht droppen. Reihenfolge wegen FK-Constraints wichtig.
    """
    sql = """
    SET FOREIGN_KEY_CHECKS = 0;
    TRUNCATE TABLE devices;
    TRUNCATE TABLE zwPartnumbersModels;
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
    with get_device_cursor() as cur:
        for stmt in sql.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt + ";")


def _import_csv_to_staging(csv_file):
    """
    CSV (mit ;) in die Staging-Tabelle WKPBE_Test_kurz schreiben.
    Spaltennamen müssen zur CSV passen.
    """
    data = csv_file.read().decode("utf-8-sig")
    f = io.StringIO(data)

    reader = csv.DictReader(f, delimiter=";")

    rows = []
    for row in reader:
        rows.append([
            row.get("PL_NAME"),
            row.get("REGION"),
            row.get("COMPANY"),
            row.get("SITEGROUP"),
            row.get("SITE"),
            row.get("ROOM"),
            row.get("PHYSICALPOSITION"),
            row.get("SHORTDESCRIPTION"),
            row.get("DEPARTMENT"),
            row.get("OWNED_BY"),
            row.get("USED_BY"),
            row.get("SUPPORTED_BY"),
            row.get("PL_COST_CENTER"),
            row.get("PL_STATUS"),
            row.get("RELATION"),
            row.get("DESTINATION_CLASSID"),
            row.get("TIER1"),
            row.get("TIER2"),
            row.get("TIER3"),
            row.get("MODEL"),
            row.get("MANUFACTURERNAME"),
            row.get("CI_NAME"),
            row.get("SERIALNUMBER"),
            row.get("CI_ID"),
            row.get("BUDGETCODE"),
            row.get("CI_ROOM"),
            row.get("FLOOR"),
            row.get("PARTNUMBER"),
            row.get("SUPPLIERNAME"),
            row.get("CI_STATUS"),
            row.get("PURCHASE_DATE"),
            row.get("RECEIVED_DATE"),
            row.get("INSTALLATION_DATE"),
            row.get("AVAILABLE_DATE"),
            row.get("RETURN_DATE"),
            row.get("DISPOSAL_DATE"),
            row.get("MARK_AS_DELETED"),
            row.get("CREATE_DATE"),
            row.get("MODIFIED_DATE"),
            row.get("ROLE"),
            row.get("CHILDNAME"),
            row.get("CONFBASICNUMBER"),
            row.get("BUILDNUMBER"),
            row.get("TYPE"),
            row.get("ADDITIONAL_INFORMATION"),
            row.get("DEPOT"),
            row.get("SUPPORTED"),
        ])

    insert_sql = """
    INSERT INTO WKPBE_Test_kurz (
        PL_NAME, REGION, COMPANY, SITEGROUP, SITE,
        ROOM, PHYSICALPOSITION, SHORTDESCRIPTION, DEPARTMENT,
        OWNED_BY, USED_BY, SUPPORTED_BY,
        PL_COST_CENTER, PL_STATUS, RELATION, DESTINATION_CLASSID,
        TIER1, TIER2, TIER3,
        MODEL, MANUFACTURERNAME, CI_NAME, SERIALNUMBER, CI_ID,
        BUDGETCODE, CI_ROOM, FLOOR, PARTNUMBER, SUPPLIERNAME,
        CI_STATUS, PURCHASE_DATE, RECEIVED_DATE,
        INSTALLATION_DATE, AVAILABLE_DATE, RETURN_DATE,
        DISPOSAL_DATE, MARK_AS_DELETED, CREATE_DATE, MODIFIED_DATE,
        ROLE, CHILDNAME, CONFBASICNUMBER, BUILDNUMBER,
        TYPE, ADDITIONAL_INFORMATION, DEPOT, SUPPORTED
    ) VALUES (
        %s,%s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,%s,
        %s,%s,%s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s,
        %s,%s,%s,%s
    )
    """

    with get_device_cursor() as cur:
        cur.execute("TRUNCATE TABLE WKPBE_Test_kurz;")
        if rows:
            cur.executemany(insert_sql, rows)


def _populate_normalized_from_staging():
    """
    WKPBE_Test_kurz -> Lookup-Tabellen + devices.
    Hier laufen nur SQLs, keine Django-Models.
    """
    with get_device_cursor() as cur, transaction.atomic(using="device_db"):
        # Lookups
        cur.execute("INSERT INTO regions (region) SELECT DISTINCT REGION FROM WKPBE_Test_kurz WHERE REGION IS NOT NULL AND REGION <> '';")
        cur.execute("""
            INSERT INTO sites (company, sitegroup, site, region_id)
            SELECT DISTINCT
                t.COMPANY, t.SITEGROUP, t.SITE, r.region_id
            FROM WKPBE_Test_kurz t
            JOIN regions r ON r.region = t.REGION
            WHERE t.SITE IS NOT NULL AND t.SITE <> '';
        """)
        cur.execute("""
            INSERT INTO rooms (room, physicalposition, ci_room, floor, site_id)
            SELECT DISTINCT
                t.ROOM, t.PHYSICALPOSITION, t.CI_ROOM, t.FLOOR, s.site_id
            FROM WKPBE_Test_kurz t
            JOIN sites s ON s.site = t.SITE
            WHERE t.ROOM IS NOT NULL OR t.CI_ROOM IS NOT NULL;
        """)
        cur.execute("INSERT INTO pl_names (pl_name) SELECT DISTINCT PL_NAME FROM WKPBE_Test_kurz WHERE PL_NAME IS NOT NULL AND PL_NAME <> '';")
        cur.execute("INSERT INTO owned_bys (owned_by) SELECT DISTINCT OWNED_BY FROM WKPBE_Test_kurz WHERE OWNED_BY IS NOT NULL AND OWNED_BY <> '';")
        cur.execute("INSERT INTO used_bys (used_by) SELECT DISTINCT USED_BY FROM WKPBE_Test_kurz WHERE USED_BY IS NOT NULL AND USED_BY <> '';")
        cur.execute("INSERT INTO supported_bys (supported_by) SELECT DISTINCT SUPPORTED_BY FROM WKPBE_Test_kurz WHERE SUPPORTED_BY IS NOT NULL AND SUPPORTED_BY <> '';")
        cur.execute("INSERT INTO suppliers (suppliername) SELECT DISTINCT SUPPLIERNAME FROM WKPBE_Test_kurz WHERE SUPPLIERNAME IS NOT NULL AND SUPPLIERNAME <> '';")
        cur.execute("INSERT INTO departments (department) SELECT DISTINCT DEPARTMENT FROM WKPBE_Test_kurz WHERE DEPARTMENT IS NOT NULL AND DEPARTMENT <> '';")
        cur.execute("INSERT INTO cost_centers (pl_cost_center) SELECT DISTINCT PL_COST_CENTER FROM WKPBE_Test_kurz WHERE PL_COST_CENTER IS NOT NULL AND PL_COST_CENTER <> '';")
        cur.execute("INSERT INTO manufacturers (manufacturername) SELECT DISTINCT MANUFACTURERNAME FROM WKPBE_Test_kurz WHERE MANUFACTURERNAME IS NOT NULL AND MANUFACTURERNAME <> '';")
        cur.execute("INSERT INTO tbltier1 (tier1) SELECT DISTINCT TIER1 FROM WKPBE_Test_kurz WHERE TIER1 IS NOT NULL AND TIER1 <> '';")
        cur.execute("INSERT INTO tbltier2 (tier2) SELECT DISTINCT TIER2 FROM WKPBE_Test_kurz WHERE TIER2 IS NOT NULL AND TIER2 <> '';")
        cur.execute("INSERT INTO tbltier3 (tier3) SELECT DISTINCT TIER3 FROM WKPBE_Test_kurz WHERE TIER3 IS NOT NULL AND TIER3 <> '';")
        cur.execute("INSERT INTO relations (relation) SELECT DISTINCT RELATION FROM WKPBE_Test_kurz WHERE RELATION IS NOT NULL AND RELATION <> '';")
        cur.execute("INSERT INTO types (type) SELECT DISTINCT TYPE FROM WKPBE_Test_kurz WHERE TYPE IS NOT NULL AND TYPE <> '';")
        cur.execute("INSERT INTO depots (depot) SELECT DISTINCT DEPOT FROM WKPBE_Test_kurz WHERE DEPOT IS NOT NULL AND DEPOT <> '';")
        cur.execute("INSERT INTO tblpl_status (pl_status) SELECT DISTINCT PL_STATUS FROM WKPBE_Test_kurz WHERE PL_STATUS IS NOT NULL AND PL_STATUS <> '';")
        cur.execute("INSERT INTO tblci_status (ci_status) SELECT DISTINCT CI_STATUS FROM WKPBE_Test_kurz WHERE CI_STATUS IS NOT NULL AND CI_STATUS <> '';")

        # Models
        cur.execute("""
            INSERT INTO models (manu_id, tier1_id, tier2_id, tier3_id, model)
            SELECT DISTINCT
                man.manu_id,
                t1.tier1_id,
                t2.tier2_id,
                t3.tier3_id,
                t.MODEL
            FROM WKPBE_Test_kurz t
            LEFT JOIN manufacturers man ON man.manufacturername = t.MANUFACTURERNAME
            LEFT JOIN tbltier1 t1       ON t1.tier1 = t.TIER1
            LEFT JOIN tbltier2 t2       ON t2.tier2 = t.TIER2
            LEFT JOIN tbltier3 t3       ON t3.tier3 = t.TIER3
            WHERE t.MODEL IS NOT NULL AND t.MODEL <> '';
        """)

        # Partnumbers + Zwischentabelle
        cur.execute("INSERT INTO partnumbers (partnumber) SELECT DISTINCT PARTNUMBER FROM WKPBE_Test_kurz WHERE PARTNUMBER IS NOT NULL AND PARTNUMBER <> '';")
        cur.execute("""
            INSERT INTO zwPartnumbersModels (partnumber_id, model_id)
            SELECT DISTINCT
                p.partnumber_id,
                m.model_id
            FROM WKPBE_Test_kurz t
            JOIN partnumbers p ON p.partnumber = t.PARTNUMBER
            JOIN models m ON m.model = t.MODEL;
        """)

        # Devices
        cur.execute("""INSERT INTO devices (
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

            -- hier wichtig: r.room_id kann NULL bleiben
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
            LEFT JOIN sites          s    ON s.site            = t.SITE
            LEFT JOIN rooms          r    ON r.site_id         = s.site_id
                                        AND (
                                                (t.ROOM IS NOT NULL AND t.ROOM <> '' AND r.room = t.ROOM)
                                            OR (t.ROOM IS NULL OR t.ROOM = '')
                                            AND (t.CI_ROOM IS NOT NULL AND t.CI_ROOM <> '' AND r.ci_room = t.CI_ROOM)
                                        );

        """)


# ---------- Views für Upload & Datenanzeige ----------

class UploadCsvView(LoginRequiredMixin, View):
    login_url = "login"

    def post(self, request):
        form = CsvUploadForm(request.POST, request.FILES)
        if not form.is_valid():
            return render(request, "index.html", {"upload_form": form})

        csv_file = form.cleaned_data["csv_file"]
        _clear_all_tables()
        _import_csv_to_staging(csv_file)
        _populate_normalized_from_staging()

        return redirect(reverse("dataBase"))

    def get(self, request):
        return redirect("index")


class DataBaseView(LoginRequiredMixin, View):
    login_url = "login"

    def get(self, request):
        with get_device_cursor() as cur:
            cur.execute("SELECT * FROM device_flat LIMIT 1000;")
            rows = cur.fetchall()
            columns = [c[0] for c in cur.description]

        return render(request, "dataBase.html", {
            "columns": columns,
            "rows": rows,
        })

    def post(self, request):
        # Button "Clear Database" klickt hier rein
        _clear_all_tables()
        return redirect("dataBase")
