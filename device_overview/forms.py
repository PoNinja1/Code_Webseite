from django import forms


class CsvUploadForm(forms.Form):
    csv_file = forms.FileField(
        required=True,
        label="CSV-Datei",
        widget=forms.ClearableFileInput(attrs={"accept": ".csv"})
    )
