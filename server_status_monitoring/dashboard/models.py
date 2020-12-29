from django.db import models

# Create your models here.

class event_message_detail_agg_tbl(models.Model):
    id = models.AutoField(primary_key=True)
    event_country_code = models.CharField(max_length=100)
    event_country_name = models.CharField(max_length=100)
    event_city_name = models.CharField(max_length=20)
    event_server_status_color_name = models.CharField(max_length=50)
    event_server_status_severity_level = models.CharField(max_length=50)
    total_estimated_resolution_time = models.IntegerField()
    total_message_count = models.IntegerField()
