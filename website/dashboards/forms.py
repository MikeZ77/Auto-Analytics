from django import forms
from .models import main, time


class CarForm(forms.Form):
    vehicle_type =  forms.ModelChoiceField(
        queryset = main.objects.values('body_type').distinct(),
    )
    make = forms.ModelChoiceField(
        queryset = main.objects.none()
    )
    model = forms.ModelChoiceField(
        queryset = main.objects.none()
    )
    year = forms.ModelChoiceField(
        queryset = main.objects.none()
    )

    class Meta:
        fields = ("vehicle_type", "make", "model", "year")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['make'].queryset = main.objects.none()
