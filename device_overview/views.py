# device_overview/views.py

from django.shortcuts import render, redirect
from django.views import View
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import connections

from .forms import CsvUploadForm
from . import db_sql

import json

from .db_sql_analysis import (
    fetch_device_rows,
    fetch_filter_options,
    fetch_counts_by_site,  # für Counts
)


DEVICE_ALIAS = "device_db"


class IndexView(TemplateView):
    template_name = "index.html"

    def get_context_data(self, **kwargs):
        ctx = super().get_context_data(**kwargs)
        if self.request.user.is_authenticated:
            ctx["upload_form"] = CsvUploadForm()
        return ctx


class UploadCsvView(View):
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


class DataBaseView(TemplateView):
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


class AnalysisView(LoginRequiredMixin,TemplateView):
    template_name = "analysis.html"

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        query = self.request.GET

        # DACH-Default nur beim ersten Aufruf
        if query:
            dach_only = query.get("dach_only") in ("1", "on")
        else:
            dach_only = True

        ci_status = query.get("ci_status") or None
        tier3 = query.get("tier3") or None
        search = query.get("search") or None

        filters = {
            "dach_only": dach_only,
            "ci_status": ci_status,
            "tier3": tier3,
            "search": search,
        }

        # Daten aus der View device_flat (via db_sql_analysis)
        columns, rows = fetch_device_rows(filters)
        filter_options = fetch_filter_options()
        counts_by_site = fetch_counts_by_site(filters)

        context.update(
            {
                "nav_active": "analysis",
                "filters": filters,
                "columns": columns,
                "rows": rows,
                "filter_options": filter_options,
                "counts_by_site": counts_by_site,
            }
        )
        return context
