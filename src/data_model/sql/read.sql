SELECT {{ columns }} FROM {{ table_name }}
{% if condition %}WHERE {{ condition }}{% endif %}