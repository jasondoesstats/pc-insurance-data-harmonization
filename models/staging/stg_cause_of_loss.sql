select
    cause_code,
    cause_description,
    line_of_business
from {{ ref('cause_of_loss') }}