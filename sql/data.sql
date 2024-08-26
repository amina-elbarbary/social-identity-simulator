with cte as (
    SELECT
        c.id,
        array_agg(_i.name) AS indicators,
        array_agg(_i.option) AS options
    FROM combination c
    LEFT JOIN combination_indicator _ci ON c.id = _ci.combination_id
    LEFT JOIN indicator _i ON _i.id = _ci.indicator_id
    WHERE c.id IN (
        SELECT ci.combination_id
        FROM main.combination_indicator ci
        JOIN indicator i ON ci.indicator_id = i.id
        WHERE (i.name, i.option) IN (
            {% for filter in filters %}
                ('{{ filter.indicator }}', '{{ filter.option }}'){% if not loop.last %},{% endif %}
            {% endfor %}
        )
        GROUP BY ci.combination_id
        HAVING COUNT(*) = {{ indicator_count }}
    )
    AND NOT EXISTS (
        SELECT 1
        FROM combination_indicator ci2
        WHERE ci2.combination_id = c.id
        AND ci2.indicator_id NOT IN (
            SELECT i2.id
            FROM indicator i2
            WHERE (i2.name, i2.option) IN (
                {% for filter in filters %}
                    ('{{ filter.indicator }}', '{{ filter.option }}'){% if not loop.last %},{% endif %}
                {% endfor %}
            )
        )
    )
    GROUP BY c.id
)
SELECT
    distances.*,
    list_reduce(cte.options, (x, y) -> concat(x, ' x ', y)) as combination
FROM distances
LEFT JOIN cte ON distances.combination_id = cte.id
WHERE cte.id IS NOT NULL
AND indicator_type = '{{selected_metric}}'
