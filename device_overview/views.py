from django.shortcuts import render
from django.views import View
from django.contrib.auth.mixins import LoginRequiredMixin
from django.db import connections   # <-- WICHTIG: HIER !

class IndexView(View):
    def get(self, request):
        return render(request, "index.html")


class DataBaseView(LoginRequiredMixin, View):
    login_url = "login"

    def get(self, request):
        # HIER device_db benutzen
        with connections['device_db'].cursor() as cursor:
            cursor.execute("SELECT * FROM devices")
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        return render(request, "dataBase.html", {
            "columns": columns,
            "rows": rows,
        })


class AnalysisView(LoginRequiredMixin, View):
    login_url = "login"

    def get(self, request):
        q = request.GET.get("q", "").strip()

        if not q:
            return render(request, "analysis.html", {
                "query": "",
                "columns": [],
                "rows": [],
            })

        sql = """
            SELECT d_id, serialnumber, shortdescription
            FROM devices
            WHERE serialnumber LIKE %s
               OR shortdescription LIKE %s
            ORDER BY d_id
            LIMIT 200
        """

        like = f"%{q}%"

        # HIER device_db benutzen
        with connections['device_db'].cursor() as cursor:
            cursor.execute(sql, [like, like])
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        return render(request, "analysis.html", {
            "query": q,
            "columns": columns,
            "rows": rows,
        })
