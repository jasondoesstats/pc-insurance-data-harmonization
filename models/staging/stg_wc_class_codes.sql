select
    class_code,
    class_description,
    hazard_group,
    line_of_business
from {{ ref('wc_class_codes') }}