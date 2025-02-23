SELECT {{columns}} FROM {{schema}}.{{table_name}}
{% if condition %}WHERE {{ condition }}{% endif %} LIMIT {{limit}}