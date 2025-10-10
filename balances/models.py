from django.db import models

from utils.models import BaseEvent


class BalanceEvent(BaseEvent):
    class BalanceType(models.TextChoices):
        COLLATERAL = "Collateral"
        STABLE_DEBT = "StableDebt"
        VARIABLE_DEBT = "VariableDebt"

    asset = models.CharField(max_length=256, null=True)
    type = models.CharField(max_length=256, null=True, choices=BalanceType.choices)
