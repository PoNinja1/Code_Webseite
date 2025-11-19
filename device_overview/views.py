# device_overview/views.py

from django.shortcuts import render, redirect
from django.views import View
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import connections

from .forms import CsvUploadForm
from . import db_sql

DEVICE_ALIAS = "device_db"


class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        if self.request.user.is_authenticated:
            ctx["upload_form"] = CsvUploadForm()
        return ctx


class UploadCsvView(LoginRequiredMixin, View):
    """
    CSV Upload von der Startseite.
    """

    def post(self, request, *args, **kwargs):
        form = CsvUploadForm(request.POST, request.FILES)
        if not form.is_valid():
            # Fehler zurück auf die Startseite
            return render(request, "index.html", {"upload_form": form})

        csv_file = form.cleaned_data["csv_file"]

        # 1. Tabellen leeren
        db_sql.clear_all_tables()
        # 2. CSV -> Staging
        db_sql.import_csv_to_staging(csv_file)
        # 3. Staging -> normalisierte DB
        db_sql.populate_normalized_from_staging()
        # 4. device_flat-View sicherstellen (optional)
        db_sql.recreate_device_flat_view()

        return redirect("dataBase")


class DataBaseView(LoginRequiredMixin, TemplateView):
    """
    Zeigt device_flat; POST -> Clear Database.
    """
    template_name = "dataBase.html"

    def post(self, request, *args, **kwargs):
        db_sql.clear_all_tables()
        return redirect("dataBase")

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        conn = connections[DEVICE_ALIAS]
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM device_flat;")
            rows = cur.fetchall()
            columns = [col[0] for col in cur.description]

        ctx["columns"] = columns
        ctx["rows"] = rows
        return ctx


class AnalysisView(LoginRequiredMixin, TemplateView):
    template_name = "analysis.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)

        conn = connections[DEVICE_ALIAS]

        # Filter aus GET-Parametern
        site = self.request.GET.get("site", "").strip()
        pl_name = self.request.GET.get("pl_name", "").strip()
        region = self.request.GET.get("region", "").strip()

        # Dropdown-Liste für Sites vorbereiten
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT SITE
                FROM device_flat
                WHERE SITE IS NOT NULL AND SITE <> ''
                ORDER BY SITE;
            """)
            site_choices = [row[0] for row in cur.fetchall()]

        # Dynamisch WHERE bauen
        conditions = []
        params = []

        if site:
            conditions.append("SITE = %s")
            params.append(site)

        if pl_name:
            # einfache LIKE-Suche
            conditions.append("PL_NAME LIKE %s")
            params.append(f"%{pl_name}%")

        if region:
            conditions.append("REGION = %s")
            params.append(region)

        base_sql = "SELECT * FROM device_flat"
        if conditions:
            base_sql += " WHERE " + " AND ".join(conditions)
        base_sql += " LIMIT 1000"   # Safety-Limit

        with conn.cursor() as cur:
            cur.execute(base_sql, params)
            rows = cur.fetchall()
            columns = [col[0] for col in cur.description]

        ctx["columns"] = columns
        ctx["rows"] = rows
        ctx["site_choices"] = site_choices
        ctx["current_site"] = site
        ctx["current_pl_name"] = pl_name
        ctx["current_region"] = region

        return ctx