{#- Location dimension, sourced from the version-controlled seed. -#}

select
    location_id,
    location_name,
    region,
    county,
    latitude,
    longitude,
    is_active,
    case
        when region in ('Inland Empire', 'San Gabriel Valley', 'San Fernando Valley') then 'inland'
        else 'coastal'
    end as climate_zone
from {{ ref('locations') }}
