from django.core.exceptions import ObjectDoesNotExist
from django.db import models


class Configuration(models.Model):
    TYPE_CHOICES = [
        ('bool', 'Boolean'),
        ('int', 'Integer'),
        ('float', 'Float'),
        ('string', 'String'),
    ]

    key = models.CharField(max_length=255, unique=True)
    value = models.TextField()
    type = models.CharField(max_length=10, choices=TYPE_CHOICES, default='string')

    class Meta:
        ordering = ['key']
        verbose_name = 'Configuration'
        verbose_name_plural = 'Configurations'

    def __str__(self):
        return f"{self.key}: {self.value}"

    @classmethod
    def get(cls, key, default=None):
        try:
            config = cls.objects.get(key=key)
            if config.type == 'bool':
                return config.value.lower() in ('true', '1', 'yes', 'on')
            elif config.type == 'int':
                return int(config.value)
            elif config.type == 'float':
                return float(config.value)
            return config.value
        except ObjectDoesNotExist:
            return default
