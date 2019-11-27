# Generated by Django 2.2.4 on 2019-10-19 23:19

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('dashboards', '0005_auto_20190901_1242'),
    ]

    operations = [
        migrations.CreateModel(
            name='vehicle_image',
            fields=[
                ('full_vehicle', models.CharField(max_length=100, primary_key=True, serialize=False)),
                ('image_path', models.CharField(max_length=100, null=True)),
            ],
            options={
                'db_table': 'vehicle_image',
            },
        ),
    ]
