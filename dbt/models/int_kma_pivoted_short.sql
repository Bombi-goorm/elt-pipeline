SELECT *
FROM (
    SELECT 
        nx,
        ny,
        fcstTime,
        category,
        fcstValue
    FROM {{ source('short_source', 'short_fact') }}
)
PIVOT (
    MAX(fcstValue)
    FOR category IN ('PCP', 'POP', 'PTY', 'REH', 'SKY', 'SNO', 'TMN', 'TMP', 'UUU', 'VEC', 'VVV', 'WAV', 'WSD')
)
