from django.db import models

# Create your models here.
class vehicle_image(models.Model):

    full_vehicle = models.CharField(max_length=100,primary_key=True)
    image_path = models.CharField(max_length=150,null=True)

    class Meta:
        db_table = 'vehicle_image'

    def __str__(self):
        return self.name

class main(models.Model):
    class Meta:
        db_table = 'main'

    def __str__(self):
        return self.name

    adID = models.CharField(max_length=11,primary_key=True)
    adType = models.CharField(max_length=10,null=True)
    condition = models.CharField(max_length=10,null=True)
    make = models.CharField(max_length=20,null=True)
    model = models.CharField(max_length=25,null=True)
    price = models.IntegerField(null=True)
    province = models.CharField(max_length=2,null=True)
    city = models.CharField(max_length=15,null=True)
    year = models.CharField(max_length=4,null=True)
    kilometers = models.CharField(max_length=13,null=True)
    exterior_color =  models.CharField(max_length=15,null=True)
    fuel_type = models.CharField(max_length=35,null=True)
    body_type = models.CharField(max_length=25,null=True)
    full_vehicle = models.CharField(max_length=100,null=True)
    #No need for FK a constraint
    #full_vehicle = models.ForeignKey(vehicle_image,on_delete=models.SET_NULL,null=True,db_column='full_vehicle')

class time(models.Model):
    class Meta:
        db_table = 'time'

    def __str__(self):
        return self.name

    adID = models.CharField(max_length=11,primary_key=True)
    time_entered = models.DateField(null=True)
    time_updated = models.DateField(null=True)
