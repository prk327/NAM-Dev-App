SELECT * FROM omniq.{{ table_name }}
{% if condition %}WHERE {{ condition }}{% endif %}