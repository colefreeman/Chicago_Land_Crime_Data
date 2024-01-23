SELECT *
FROM {{ source('chicago_crimes', 'CRIMES') }}